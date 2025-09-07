#!/usr/bin/env python3
"""
Debug script for Ollama integration in the AI hedge fund.
Tests connectivity, speed, and model responses.
"""
import os
import time
import requests
from typing import Dict, Any
from langchain_ollama import ChatOllama


def test_ollama_connection() -> Dict[str, Any]:
    """Test basic Ollama server connectivity"""
    print("🔍 Testing Ollama server connectivity...")
    
    # Check environment variables
    ollama_host = os.getenv("OLLAMA_HOST", "localhost")
    base_url = os.getenv("OLLAMA_BASE_URL", f"http://{ollama_host}:11434")
    
    print(f"📍 Using Ollama URL: {base_url}")
    
    try:
        # Test basic connectivity
        response = requests.get(f"{base_url}/api/version", timeout=5)
        print(f"✅ Server responded: {response.status_code}")
        print(f"📋 Version info: {response.json()}")
        
        # List available models
        models_response = requests.get(f"{base_url}/api/tags", timeout=10)
        if models_response.status_code == 200:
            models = models_response.json()
            print(f"\n📚 Available models ({len(models.get('models', []))}):")
            for model in models.get('models', []):
                print(f"  - {model['name']} ({model.get('size', 'Unknown size')})")
        
        return {
            "status": "connected",
            "url": base_url,
            "version": response.json(),
            "models": models.get('models', []) if models_response.status_code == 200 else []
        }
        
    except requests.exceptions.ConnectionError:
        print("❌ Connection failed - is Ollama running?")
        print("💡 Try: ollama serve")
        return {"status": "connection_failed", "url": base_url}
    except requests.exceptions.Timeout:
        print("⏰ Connection timeout - Ollama might be starting up")
        return {"status": "timeout", "url": base_url}
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return {"status": "error", "url": base_url, "error": str(e)}


def test_model_response(model_name: str = "llama3.1:latest") -> Dict[str, Any]:
    """Test model response speed and quality"""
    print(f"\n🤖 Testing model: {model_name}")
    
    # Check environment variables
    ollama_host = os.getenv("OLLAMA_HOST", "localhost")
    base_url = os.getenv("OLLAMA_BASE_URL", f"http://{ollama_host}:11434")
    
    try:
        # Create ChatOllama instance (same as your hedge fund uses)
        chat_model = ChatOllama(
            model=model_name,
            base_url=base_url,
        )
        
        # Test simple prompt
        test_prompt = "What is the current year? Respond with just the year number."
        
        print(f"📝 Prompt: {test_prompt}")
        print("⏱️  Measuring response time...")
        
        start_time = time.time()
        
        # Make the request (same way as hedge fund agents)
        response = chat_model.invoke(test_prompt)
        
        end_time = time.time()
        response_time = end_time - start_time
        
        print(f"✅ Response received in {response_time:.2f} seconds")
        print(f"💬 Response: {response.content}")
        
        return {
            "status": "success",
            "model": model_name,
            "response_time": response_time,
            "response": response.content,
            "prompt": test_prompt
        }
        
    except Exception as e:
        print(f"❌ Model test failed: {e}")
        return {
            "status": "failed",
            "model": model_name,
            "error": str(e)
        }


def test_financial_agent_scenario(model_name: str = "llama3.1:latest") -> Dict[str, Any]:
    """Test a scenario similar to what hedge fund agents would do"""
    print(f"\n💰 Testing financial analysis scenario with: {model_name}")
    
    ollama_host = os.getenv("OLLAMA_HOST", "localhost")
    base_url = os.getenv("OLLAMA_BASE_URL", f"http://{ollama_host}:11434")
    
    try:
        chat_model = ChatOllama(
            model=model_name,
            base_url=base_url,
        )
        
        # Financial analysis prompt (similar to hedge fund agents)
        financial_prompt = """
You are a financial analyst. Analyze Apple (AAPL) stock briefly.
Consider: recent performance, market position, risks.
Provide a 2-sentence summary and a rating (BUY/HOLD/SELL).
"""
        
        print("📊 Testing financial analysis prompt...")
        print("⏱️  Measuring response time...")
        
        start_time = time.time()
        response = chat_model.invoke(financial_prompt)
        end_time = time.time()
        response_time = end_time - start_time
        
        print(f"✅ Financial analysis completed in {response_time:.2f} seconds")
        print(f"📈 Analysis: {response.content[:200]}...")
        
        return {
            "status": "success",
            "model": model_name,
            "response_time": response_time,
            "response_length": len(response.content),
            "analysis_preview": response.content[:200]
        }
        
    except Exception as e:
        print(f"❌ Financial scenario test failed: {e}")
        return {
            "status": "failed",
            "model": model_name,
            "error": str(e)
        }


def main():
    """Run all Ollama debugging tests"""
    print("🚀 Ollama Debug Script for AI Hedge Fund")
    print("=" * 50)
    
    # Test 1: Basic connectivity
    connection_result = test_ollama_connection()
    
    if connection_result["status"] != "connected":
        print("\n⛔ Cannot proceed - Ollama server not accessible")
        print("💡 Make sure Ollama is running: ollama serve")
        return
    
    # Test 2: Check if our configured models exist
    available_models = [model['name'] for model in connection_result.get("models", [])]
    
    # Test with a common model first
    test_models = ["llama3.1:latest", "llama3.1", "qwen2.5", "gemma2"]
    working_model = None
    
    for model in test_models:
        if any(model in available for available in available_models):
            working_model = model
            break
    
    if not working_model and available_models:
        working_model = available_models[0]  # Use first available model
        
    if not working_model:
        print("\n⚠️  No models found - you may need to pull a model")
        print("💡 Try: ollama pull llama3.1")
        return
    
    print(f"\n🎯 Using model: {working_model}")
    
    # Test 3: Basic response test
    basic_result = test_model_response(working_model)
    
    if basic_result["status"] == "success":
        # Test 4: Financial scenario test
        financial_result = test_financial_agent_scenario(working_model)
        
        # Summary
        print("\n" + "=" * 50)
        print("📊 SUMMARY")
        print("=" * 50)
        print(f"✅ Ollama server: Connected ({connection_result['url']})")
        print(f"✅ Model tested: {working_model}")
        print(f"⏱️  Basic response: {basic_result['response_time']:.2f}s")
        
        if financial_result["status"] == "success":
            print(f"⏱️  Financial analysis: {financial_result['response_time']:.2f}s")
            print("✅ Ready for hedge fund integration!")
        else:
            print("⚠️  Financial analysis failed")
            
        # Performance comparison
        basic_time = basic_result.get('response_time', 0)
        if basic_time > 10:
            print("⚠️  Response time is slow (>10s) - consider using a smaller model")
        elif basic_time > 5:
            print("⏳ Response time is moderate (5-10s)")
        else:
            print("🚀 Response time is fast (<5s)")
    
    else:
        print(f"\n❌ Model testing failed: {basic_result.get('error', 'Unknown error')}")


if __name__ == "__main__":
    main()