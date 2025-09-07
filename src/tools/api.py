"""
Updated API module that uses the new multi-provider architecture.
This module maintains backward compatibility while leveraging the new provider manager.
"""

import datetime
import os
import pandas as pd
import logging
from typing import List, Optional

from src.data.provider_manager import DataProviderManager
from src.data.models import (
    CompanyNews,
    FinancialMetrics,
    Price,
    LineItem,
    InsiderTrade,
    CompanyFacts,
)

logger = logging.getLogger(__name__)

# Global provider manager instance
_provider_manager = None


def get_provider_manager() -> DataProviderManager:
    """Get or create the global provider manager instance."""
    global _provider_manager
    if _provider_manager is None:
        # Initialize with API keys from environment
        financial_datasets_api_key = os.environ.get("FINANCIAL_DATASETS_API_KEY")
        
        _provider_manager = DataProviderManager(
            financial_datasets_api_key=financial_datasets_api_key
        )
        logger.info("Initialized DataProviderManager")
    return _provider_manager


# Backward-compatible API functions that now use the provider manager

def get_prices(ticker: str, start_date: str, end_date: str, api_key: str = None) -> List[Price]:
    """
    Fetch price data using the new multi-provider system.
    
    This function maintains backward compatibility with the original API
    while leveraging the new provider manager with automatic fallbacks.
    
    Args:
        ticker: Stock ticker symbol
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        api_key: Legacy parameter, now handled by provider manager
        
    Returns:
        List of Price objects
    """
    try:
        manager = get_provider_manager()
        prices, provider_used = manager.get_prices(ticker, start_date, end_date)
        
        logger.debug(f"Retrieved {len(prices)} prices for {ticker} from {provider_used}")
        return prices
        
    except Exception as e:
        logger.error(f"Failed to get prices for {ticker}: {e}")
        # Return empty list for backward compatibility
        return []


def get_financial_metrics(
    ticker: str,
    end_date: str,
    period: str = "ttm",
    limit: int = 10,
    api_key: str = None,
) -> List[FinancialMetrics]:
    """
    Fetch financial metrics using the new multi-provider system.
    
    Args:
        ticker: Stock ticker symbol
        end_date: End date in YYYY-MM-DD format
        period: Period for metrics ("ttm", "annual", "quarterly")
        limit: Maximum number of periods to return
        api_key: Legacy parameter, now handled by provider manager
        
    Returns:
        List of FinancialMetrics objects
    """
    try:
        manager = get_provider_manager()
        metrics, provider_used = manager.get_financial_metrics(ticker, end_date, period, limit)
        
        logger.debug(f"Retrieved {len(metrics)} financial metrics for {ticker} from {provider_used}")
        return metrics
        
    except Exception as e:
        logger.error(f"Failed to get financial metrics for {ticker}: {e}")
        return []


def search_line_items(
    ticker: str,
    line_items: List[str],
    end_date: str,
    period: str = "ttm",
    limit: int = 10,
    api_key: str = None,
) -> List[LineItem]:
    """
    Search for line items using the new multi-provider system.
    
    Args:
        ticker: Stock ticker symbol
        line_items: List of line item names to search for
        end_date: End date in YYYY-MM-DD format
        period: Period for search ("ttm", "annual", "quarterly")
        limit: Maximum number of results to return
        api_key: Legacy parameter, now handled by provider manager
        
    Returns:
        List of LineItem objects
    """
    try:
        manager = get_provider_manager()
        items, provider_used = manager.search_line_items(ticker, line_items, end_date, period, limit)
        
        logger.debug(f"Retrieved {len(items)} line items for {ticker} from {provider_used}")
        return items
        
    except Exception as e:
        logger.error(f"Failed to search line items for {ticker}: {e}")
        return []


def get_insider_trades(
    ticker: str,
    end_date: str,
    start_date: str | None = None,
    limit: int = 1000,
    api_key: str = None,
) -> List[InsiderTrade]:
    """
    Fetch insider trades using the new multi-provider system.
    
    Args:
        ticker: Stock ticker symbol
        end_date: End date in YYYY-MM-DD format
        start_date: Start date in YYYY-MM-DD format (optional)
        limit: Maximum number of trades to return
        api_key: Legacy parameter, now handled by provider manager
        
    Returns:
        List of InsiderTrade objects
    """
    try:
        manager = get_provider_manager()
        trades, provider_used = manager.get_insider_trades(ticker, end_date, start_date, limit)
        
        logger.debug(f"Retrieved {len(trades)} insider trades for {ticker} from {provider_used}")
        return trades
        
    except Exception as e:
        logger.error(f"Failed to get insider trades for {ticker}: {e}")
        return []


def get_company_news(
    ticker: str,
    end_date: str,
    start_date: str | None = None,
    limit: int = 1000,
    api_key: str = None,
) -> List[CompanyNews]:
    """
    Fetch company news using the new multi-provider system.
    
    Args:
        ticker: Stock ticker symbol
        end_date: End date in YYYY-MM-DD format
        start_date: Start date in YYYY-MM-DD format (optional)
        limit: Maximum number of news articles to return
        api_key: Legacy parameter, now handled by provider manager
        
    Returns:
        List of CompanyNews objects
    """
    try:
        manager = get_provider_manager()
        news, provider_used = manager.get_company_news(ticker, end_date, start_date, limit)
        
        logger.debug(f"Retrieved {len(news)} news articles for {ticker} from {provider_used}")
        return news
        
    except Exception as e:
        logger.error(f"Failed to get company news for {ticker}: {e}")
        return []


def get_market_cap(
    ticker: str,
    end_date: str,
    api_key: str = None,
) -> float | None:
    """
    Fetch market capitalization using the new multi-provider system.
    
    Args:
        ticker: Stock ticker symbol
        end_date: Date for market cap in YYYY-MM-DD format
        api_key: Legacy parameter, now handled by provider manager
        
    Returns:
        Market cap as float or None if not available
    """
    try:
        manager = get_provider_manager()
        market_cap, provider_used = manager.get_market_cap(ticker, end_date)
        
        if market_cap:
            logger.debug(f"Retrieved market cap ${market_cap:,.0f} for {ticker} from {provider_used}")
        else:
            logger.debug(f"No market cap available for {ticker}")
            
        return market_cap
        
    except Exception as e:
        logger.error(f"Failed to get market cap for {ticker}: {e}")
        return None


def prices_to_df(prices: List[Price]) -> pd.DataFrame:
    """
    Convert prices to a DataFrame.
    
    This function maintains backward compatibility with the original API.
    
    Args:
        prices: List of Price objects
        
    Returns:
        DataFrame with OHLCV data indexed by date
    """
    if not prices:
        return pd.DataFrame()
        
    df = pd.DataFrame([p.model_dump() for p in prices])
    df["Date"] = pd.to_datetime(df["time"])
    df.set_index("Date", inplace=True)
    numeric_cols = ["open", "close", "high", "low", "volume"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df.sort_index(inplace=True)
    return df


def get_price_data(ticker: str, start_date: str, end_date: str, api_key: str = None) -> pd.DataFrame:
    """
    Get price data as a DataFrame.
    
    This function maintains backward compatibility with the original API
    while using the new provider system.
    
    Args:
        ticker: Stock ticker symbol
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        api_key: Legacy parameter, now handled by provider manager
        
    Returns:
        DataFrame with OHLCV data indexed by date
    """
    prices = get_prices(ticker, start_date, end_date, api_key)
    return prices_to_df(prices)


# Advanced provider management functions for monitoring and control

def get_provider_health_status() -> dict:
    """
    Get health status of all data providers.
    
    Returns:
        Dictionary containing health information for each provider
    """
    manager = get_provider_manager()
    return manager.get_provider_health_status()


def get_cache_statistics() -> dict:
    """
    Get cache statistics.
    
    Returns:
        Dictionary containing cache statistics
    """
    manager = get_provider_manager()
    return manager.get_cache_stats()


def clear_cache(provider: Optional[str] = None):
    """
    Clear cache data.
    
    Args:
        provider: Specific provider to clear, or None to clear all
    """
    manager = get_provider_manager()
    manager.clear_cache(provider)


def warm_cache(tickers: List[str], days_back: int = 30):
    """
    Warm the cache with recent data for given tickers.
    
    Args:
        tickers: List of ticker symbols to warm cache for
        days_back: Number of days of price data to cache
    """
    manager = get_provider_manager()
    manager.warm_cache(tickers, days_back)


def reset_provider_health(provider_name: str):
    """
    Reset health status for a specific provider.
    
    Args:
        provider_name: Name of the provider to reset
    """
    manager = get_provider_manager()
    manager.reset_provider_health(provider_name)


# Legacy cache functions for backward compatibility
# These now delegate to the provider manager's cache system

def get_cache():
    """
    Get cache instance for backward compatibility.
    
    Note: This returns a simplified interface that delegates to the enhanced cache.
    """
    class LegacyCacheAdapter:
        """Adapter to maintain backward compatibility with the old cache interface."""
        
        def __init__(self, provider_manager):
            self.manager = provider_manager
            
        def get_prices(self, cache_key: str) -> list:
            # Extract parameters from cache key (simplified)
            return None  # Let provider manager handle caching
            
        def set_prices(self, cache_key: str, data: list):
            # Caching is now handled by provider manager
            pass
            
        def get_financial_metrics(self, cache_key: str) -> list:
            return None
            
        def set_financial_metrics(self, cache_key: str, data: list):
            pass
            
        def get_insider_trades(self, cache_key: str) -> list:
            return None
            
        def set_insider_trades(self, cache_key: str, data: list):
            pass
            
        def get_company_news(self, cache_key: str) -> list:
            return None
            
        def set_company_news(self, cache_key: str, data: list):
            pass
    
    manager = get_provider_manager()
    return LegacyCacheAdapter(manager)


# Information functions

def get_supported_providers() -> List[str]:
    """
    Get list of supported data providers.
    
    Returns:
        List of provider names
    """
    manager = get_provider_manager()
    return list(manager.providers.keys())


def get_provider_features() -> dict:
    """
    Get supported features by each provider.
    
    Returns:
        Dictionary mapping provider names to their supported features
    """
    manager = get_provider_manager()
    return manager.get_supported_features()


# Migration utilities

def migrate_from_legacy_cache():
    """
    Utility function to help migrate from the legacy cache system.
    This function can be called during the transition period.
    """
    logger.info("Legacy cache migration is handled automatically by the provider manager")
    logger.info("No manual migration required - caching is now provider-aware")


def get_migration_status() -> dict:
    """
    Get migration status information.
    
    Returns:
        Dictionary containing migration status and statistics
    """
    manager = get_provider_manager()
    health_status = manager.get_provider_health_status()
    cache_stats = manager.get_cache_stats()
    
    return {
        "migration_complete": True,
        "provider_manager_active": True,
        "providers_available": len(manager.providers),
        "providers_healthy": sum(1 for status in health_status.values() if status['healthy']),
        "cache_entries": cache_stats.get('total_entries', 0),
        "supported_providers": list(manager.providers.keys()),
        "primary_providers": ["yahoo", "stooq"],  # Free providers
        "fallback_providers": ["financialdatasets"],  # Paid provider
    }


# For debugging and development
if __name__ == "__main__":
    # Quick test of the new API
    logging.basicConfig(level=logging.INFO)
    
    print("Testing new API with provider manager...")
    
    # Test basic functionality
    prices = get_prices("AAPL", "2025-09-01", "2025-09-05")
    print(f"Retrieved {len(prices)} prices")
    
    # Test provider health
    health = get_provider_health_status()
    print(f"Provider health: {health}")
    
    # Test migration status
    status = get_migration_status()
    print(f"Migration status: {status}")