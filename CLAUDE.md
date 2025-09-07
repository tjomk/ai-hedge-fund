# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Architecture Overview

This is an AI-powered hedge fund that uses multiple specialized agents to make trading decisions. The system is built with two main deployment options:

### Core Python Application
- **Main CLI**: Located in `src/main.py` - runs the hedge fund simulation with multiple LLM agents
- **Backtester**: Located in `src/backtester.py` - performs historical backtesting
- **Agent System**: Individual agent implementations in `src/agents/` representing famous investors (Warren Buffett, Charlie Munger, etc.) and analytical tools (sentiment, technical, fundamental analysis)
- **LangGraph Workflow**: Uses LangGraph for orchestrating agent interactions via `src/graph/state.py`

### Web Application (Full Stack)
- **Frontend**: React/Vite TypeScript application in `app/frontend/` with drag-and-drop interface for creating trading workflows
- **Backend**: FastAPI application in `app/backend/` providing REST API endpoints
- **Database**: SQLite with SQLAlchemy ORM and Alembic migrations

## Development Commands

### Python CLI Commands
```bash
# Install dependencies
poetry install

# Run hedge fund simulation
poetry run python src/main.py --ticker AAPL,MSFT,NVDA

# Run with local models via Ollama
poetry run python src/main.py --ticker AAPL,MSFT,NVDA --ollama

# Run backtester
poetry run python src/backtester.py --ticker AAPL,MSFT,NVDA

# Specify date ranges
poetry run python src/main.py --ticker AAPL,MSFT,NVDA --start-date 2024-01-01 --end-date 2024-03-01
```

### Web Application Commands
```bash
# Quick start (from app/ directory)
./run.sh

# Manual backend startup (from app/backend)
poetry run uvicorn main:app --reload

# Manual frontend startup (from app/frontend)
npm run dev
npm run build
npm run lint

# Database migrations (from app/backend)
alembic upgrade head
```

### Docker Commands (from docker/ directory)
```bash
# Build Docker image
./run.sh build

# Run with Docker
./run.sh --ticker AAPL,MSFT,NVDA main
./run.sh --ticker AAPL,MSFT,NVDA backtest

# With local models
./run.sh --ticker AAPL,MSFT,NVDA --ollama main
```

### Code Quality Commands
```bash
# Python formatting and linting
black .
isort .
flake8

# TypeScript linting (from app/frontend)
npm run lint
```

## Key Components

### Agent Architecture
- **Investment Personalities** (`src/agents/`): Each agent embodies a specific investment philosophy (e.g., `warren_buffett.py`, `cathie_wood.py`)
- **Analytical Agents**: `fundamentals.py`, `sentiment.py`, `technicals.py`, `valuation.py`
- **Decision Makers**: `portfolio_manager.py`, `risk_manager.py`
- **State Management**: `src/graph/state.py` defines the shared state between all agents

### Data and Models
- **LLM Integration** (`src/llm/models.py`): Supports OpenAI, Anthropic, Groq, DeepSeek, and Ollama models
- **Financial Data** (`src/data/`): Data models and caching for financial information
- **API Tools** (`src/tools/api.py`): Financial data retrieval utilities

### Web Interface Components
- **Frontend Services** (`app/frontend/src/services/`): API client, flow management, backtest integration
- **React Flow Integration**: Drag-and-drop workflow builder using `@xyflow/react`
- **Backend Routes** (`app/backend/routes/`): REST endpoints for flows, backtests, model management
- **Database Models** (`app/backend/database/models.py`): SQLAlchemy models for persistence

## Environment Setup

Required API keys in `.env` file:
- At least one LLM provider: `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, `GROQ_API_KEY`, or `DEEPSEEK_API_KEY`  
- Financial data (optional - provides premium features): `FINANCIAL_DATASETS_API_KEY`

## Data Provider System

The system now uses a **multi-provider architecture** with automatic fallbacks:

### Primary Providers
- **Yahoo Finance**: Primary free source for prices, financial metrics, company facts  
- **STOOQ**: Backup for price data, international markets, currencies, commodities

### Premium/Specialized Providers
- **FinancialDatasets.ai**: Advanced features like news, insider trades, detailed financials
- **SEC Edgar**: US regulatory filings and company facts

### Provider Features
```bash
# Check provider health and features
poetry run python -c "from src.tools.api import get_provider_health_status, get_supported_providers; print(get_provider_health_status()); print(get_supported_providers())"

# Clear cache or warm cache for better performance  
poetry run python -c "from src.tools.api import clear_cache, warm_cache; warm_cache(['AAPL', 'MSFT', 'GOOGL'])"
```

### Using International Tickers

The system supports international market tickers through Yahoo Finance and STOOQ:

```bash
# Examples with London Stock Exchange tickers
poetry run python src/main.py --ticker VODL.L,BATS.L,RDSB.L  # Vodafone, British American Tobacco, Shell

# Mixed US and international tickers
poetry run python src/main.py --ticker AAPL,VODL.L,MSFT,BATS.L
```

**Ticker Format Notes:**
- London Stock Exchange tickers end with `.L` (e.g., `VODL.L` for Vodafone)  
- Other international markets may have different suffixes (.TO for TSX, .PA for Paris, etc.)
- System will attempt to find data across all available providers

## Testing

```bash
# Run tests
poetry run pytest

# Specific test
poetry run pytest tests/test_api_rate_limiting.py
```

## Migration Information

**✅ Migration Complete**: The system has been successfully migrated from financialdatasets.ai to a multi-provider architecture.

### Migration Benefits
- **No API key required** for basic functionality (prices, financial metrics, company facts)
- **Automatic fallbacks** ensure high availability 
- **Enhanced caching** improves performance
- **Circuit breakers** prevent cascade failures
- **Backward compatibility** maintained for all existing code

### Migration Status
```bash
# Check migration status and generate report
poetry run python -c "from src.data.legacy_adapter import check_migration_status, generate_migration_report; print(check_migration_status()); print(generate_migration_report())"
```

## Important Notes

- The system does not execute real trades - it's for educational/research purposes only
- **No API key required** for basic operations (uses free Yahoo Finance + STOOQ)
- Set `FINANCIAL_DATASETS_API_KEY` for premium features (news, insider trades)
- The web application runs backend on port 8000, frontend on port 5173
- Database file `hedge_fund.db` is created automatically in the project root
- Python 3.11+ is recommended; 3.13+ may have compatibility issues
- Provider health and cache statistics available via API functions