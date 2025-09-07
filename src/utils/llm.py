"""Helper functions for LLM"""

import json
from pydantic import BaseModel
from src.llm.models import get_model, get_model_info
from src.utils.progress import progress
from src.graph.state import AgentState


def call_llm(
    prompt: any,
    pydantic_model: type[BaseModel],
    agent_name: str | None = None,
    state: AgentState | None = None,
    max_retries: int = 3,
    default_factory=None,
) -> BaseModel:
    """
    Makes an LLM call with retry logic, handling both JSON supported and non-JSON supported models.

    Args:
        prompt: The prompt to send to the LLM
        pydantic_model: The Pydantic model class to structure the output
        agent_name: Optional name of the agent for progress updates and model config extraction
        state: Optional state object to extract agent-specific model configuration
        max_retries: Maximum number of retries (default: 3)
        default_factory: Optional factory function to create default response on failure

    Returns:
        An instance of the specified Pydantic model
    """
    
    # Extract model configuration if state is provided and agent_name is available
    if state and agent_name:
        model_name, model_provider = get_agent_model_config(state, agent_name)
    else:
        # Use system defaults when no state or agent_name is provided
        model_name = "gpt-4.1"
        model_provider = "OPENAI"

    # Extract API keys from state if available
    api_keys = None
    if state:
        request = state.get("metadata", {}).get("request")
        if request and hasattr(request, 'api_keys'):
            api_keys = request.api_keys

    model_info = get_model_info(model_name, model_provider)
    llm = get_model(model_name, model_provider, api_keys)
    
    # Check if debug mode is enabled
    debug_enabled = False
    if state and state.get("metadata", {}).get("debug", False):
        debug_enabled = True

    # For non-JSON support models, we can use structured output
    if not (model_info and not model_info.has_json_mode()):
        llm = llm.with_structured_output(
            pydantic_model,
            method="json_mode",
        )

    # Print debug information if enabled
    if debug_enabled:
        print("=" * 80)
        print("🐛 DEBUG MODE ENABLED - LLM PROMPT")
        print("=" * 80)
        print(f"Agent: {agent_name}")
        print(f"Model: {model_name} ({model_provider})")
        print(f"Has JSON mode: {not (model_info and not model_info.has_json_mode())}")
        print(f"Attempt: 1/{max_retries}")
        print("-" * 80)
        print("PROMPT:")
        print("-" * 40)
        
        # Handle different prompt formats
        if hasattr(prompt, 'messages') and prompt.messages:
            # ChatPromptTemplate with messages
            for i, message in enumerate(prompt.messages):
                role = getattr(message, 'type', 'unknown')
                content = getattr(message, 'content', str(message))
                print(f"Message {i+1} ({role}):")
                print(content)
                print("-" * 20)
        elif hasattr(prompt, 'to_string'):
            # Prompt object with to_string method
            print(prompt.to_string())
        elif hasattr(prompt, 'content'):
            # Message with content attribute
            print(prompt.content)
        else:
            # Fallback - convert to string
            print(str(prompt))
        
        print("-" * 40)
        print("=" * 80)
    
    # Call the LLM with retries
    for attempt in range(max_retries):
        try:
            # Print retry information if debug enabled and not first attempt
            if debug_enabled and attempt > 0:
                print(f"🔄 DEBUG: Retry attempt {attempt + 1}/{max_retries}")
            
            # Call the LLM
            result = llm.invoke(prompt)
            
            # Print debug response if enabled
            if debug_enabled:
                print("=" * 80)
                print("🐛 DEBUG - LLM RESPONSE")
                print("=" * 80)
                print(f"Response type: {type(result)}")
                if hasattr(result, 'content'):
                    print("Raw content:")
                    print("-" * 40)
                    print(repr(result.content))  # Show escaped version
                    print("-" * 40)
                    print("Formatted content:")
                    print(result.content)
                else:
                    print("Response:")
                    print(result)
                print("=" * 80)

            # For non-JSON support models, we need to extract and parse the JSON manually
            if model_info and not model_info.has_json_mode():
                parsed_result = extract_json_from_response(result.content)
                if parsed_result:
                    return pydantic_model(**parsed_result)
                else:
                    # JSON extraction failed - raise a descriptive error
                    raise ValueError(f"Invalid json output: {result.content[:100]}...")
            else:
                return result

        except Exception as e:
            if agent_name:
                progress.update_status(agent_name, None, f"Error - retry {attempt + 1}/{max_retries}")

            if attempt == max_retries - 1:
                error_msg = f"Error in LLM call after {max_retries} attempts: {e}"
                print("=" * 80)
                print("LLM CALL FAILED - DEBUGGING INFO")
                print("=" * 80)
                print(f"Error: {error_msg}")
                print(f"Agent: {agent_name}")
                print(f"Model: {model_name} ({model_provider})")
                print(f"Has JSON mode: {not (model_info and not model_info.has_json_mode())}")
                
                # Show the raw LLM response if it exists and we're dealing with JSON parsing
                if hasattr(e, '__traceback__') and 'result' in locals():
                    print(f"\nRAW LLM RESPONSE:")
                    print("-" * 40)
                    if hasattr(result, 'content'):
                        print(repr(result.content))  # Use repr to show escapes/special chars
                        print("-" * 40)
                        print("FORMATTED RESPONSE:")
                        print(result.content)
                    else:
                        print(f"Result type: {type(result)}")
                        print(f"Result: {result}")
                    print("-" * 40)
                
                print("=" * 80)
                
                # Use default_factory if provided, otherwise create a basic default
                if default_factory:
                    return default_factory()
                return create_default_response(pydantic_model)

    # This should never be reached due to the retry logic above
    return create_default_response(pydantic_model)


def create_default_response(model_class: type[BaseModel]) -> BaseModel:
    """Creates a safe default response based on the model's fields."""
    default_values = {}
    for field_name, field in model_class.model_fields.items():
        if field.annotation == str:
            default_values[field_name] = "Error in analysis, using default"
        elif field.annotation == float:
            default_values[field_name] = 0.0
        elif field.annotation == int:
            default_values[field_name] = 0
        elif hasattr(field.annotation, "__origin__") and field.annotation.__origin__ == dict:
            default_values[field_name] = {}
        else:
            # For other types (like Literal), try to use the first allowed value
            if hasattr(field.annotation, "__args__"):
                default_values[field_name] = field.annotation.__args__[0]
            else:
                default_values[field_name] = None

    return model_class(**default_values)


def extract_json_from_response(content: str) -> dict | None:
    """
    Extracts JSON from markdown-formatted response.
    
    Handles various JSON code block formats that different LLMs produce:
    - ```json
    - ```jsonc  
    - ```jsoncjsonc (Ollama issue)
    - Plain JSON without code blocks
    - JSON with extra whitespace/characters
    """
    import re
    
    try:
        # Method 1: Try various code block patterns (most common)
        patterns = [
            r'```json\s*\n?(.*?)```',  # Standard ```json
            r'```jsonc\s*\n?(.*?)```',  # JSON with comments
            r'```[a-z]*json[a-z]*\s*\n?(.*?)```',  # Malformed json blocks (e.g., jsoncjsonc)
            r'```\s*\n?(\{.*?\})\s*```',  # Generic code blocks with JSON content
        ]
        
        for pattern in patterns:
            match = re.search(pattern, content, re.DOTALL | re.IGNORECASE)
            if match:
                json_text = match.group(1).strip()
                if json_text:
                    return json.loads(json_text)
        
        # Method 2: Look for JSON object boundaries without code blocks
        # Find the first { and last } that form a valid JSON object
        first_brace = content.find('{')
        if first_brace != -1:
            # Find the matching closing brace by counting braces
            brace_count = 0
            for i, char in enumerate(content[first_brace:], first_brace):
                if char == '{':
                    brace_count += 1
                elif char == '}':
                    brace_count -= 1
                    if brace_count == 0:
                        json_text = content[first_brace:i+1]
                        return json.loads(json_text)
        
        # Method 3: Try to extract lines that look like JSON
        lines = content.split('\n')
        json_lines = []
        in_json = False
        
        for line in lines:
            line = line.strip()
            if line.startswith('{') or in_json:
                in_json = True
                json_lines.append(line)
                if line.endswith('}') and line.count('}') >= line.count('{'):
                    json_text = '\n'.join(json_lines)
                    try:
                        return json.loads(json_text)
                    except:
                        continue
        
    except Exception as e:
        print(f"JSON EXTRACTION DEBUG:")
        print(f"Error: {e}")
        print(f"Content length: {len(content)}")
        print(f"Content preview (first 300 chars):")
        print(repr(content[:300]))
        print(f"Content preview (formatted):")
        print(content[:300])
        if len(content) > 300:
            print(f"... (truncated, total length: {len(content)} chars)")
    
    return None


def get_agent_model_config(state, agent_name):
    """
    Get model configuration for a specific agent from the state.
    Falls back to global model configuration if agent-specific config is not available.
    Always returns valid model_name and model_provider values.
    """
    request = state.get("metadata", {}).get("request")
    
    if request and hasattr(request, 'get_agent_model_config'):
        # Get agent-specific model configuration
        model_name, model_provider = request.get_agent_model_config(agent_name)
        # Ensure we have valid values
        if model_name and model_provider:
            return model_name, model_provider.value if hasattr(model_provider, 'value') else str(model_provider)
    
    # Fall back to global configuration (system defaults)
    model_name = state.get("metadata", {}).get("model_name") or "gpt-4.1"
    model_provider = state.get("metadata", {}).get("model_provider") or "OPENAI"
    
    # Convert enum to string if necessary
    if hasattr(model_provider, 'value'):
        model_provider = model_provider.value
    
    return model_name, model_provider
