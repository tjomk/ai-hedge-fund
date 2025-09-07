import time
import logging
from typing import List, Optional, Dict, Any, Tuple
from enum import Enum
from dataclasses import dataclass
from datetime import datetime, timedelta

from src.data.providers.base import (
    DataProvider,
    DataProviderError,
    DataProviderRateLimitError,
    DataProviderTimeoutError,
    DataProviderNotFoundError,
)
from src.data.providers import (
    YahooFinanceProvider,
    StooqProvider,
    FinancialDatasetsProvider,
    SECEdgarProvider,
)
from src.data.models import (
    Price,
    FinancialMetrics,
    CompanyNews,
    InsiderTrade,
    LineItem,
    CompanyFacts,
)
from src.data.enhanced_cache import get_enhanced_cache, CacheEntryStatus

logger = logging.getLogger(__name__)


class ProviderPriority(Enum):
    """Provider priority levels for different data types."""
    PRIMARY = 1
    SECONDARY = 2
    FALLBACK = 3


@dataclass
class ProviderHealth:
    """Track provider health and circuit breaker state."""
    is_healthy: bool = True
    failure_count: int = 0
    last_failure_time: Optional[datetime] = None
    circuit_open_until: Optional[datetime] = None
    total_requests: int = 0
    successful_requests: int = 0


class DataProviderManager:
    """
    Manages multiple data providers with intelligent fallback logic.
    
    Features:
    - Provider prioritization by data type
    - Circuit breaker pattern for failing providers
    - Health monitoring and recovery
    - Automatic provider selection and fallback
    """
    
    def __init__(self, financial_datasets_api_key: Optional[str] = None):
        """
        Initialize the provider manager with all available providers.
        
        Args:
            financial_datasets_api_key: Optional API key for FinancialDatasets provider
        """
        # Initialize providers
        self.providers = {
            'yahoo': YahooFinanceProvider(),
            'stooq': StooqProvider(),
            'financialdatasets': FinancialDatasetsProvider(financial_datasets_api_key),
            'sec_edgar': SECEdgarProvider(),
        }
        
        # Provider health tracking
        self.provider_health = {
            name: ProviderHealth() for name in self.providers.keys()
        }
        
        # Initialize enhanced cache
        self.cache = get_enhanced_cache()
        
        # Circuit breaker configuration
        self.failure_threshold = 3  # Failures before opening circuit
        self.circuit_timeout = timedelta(minutes=5)  # How long to keep circuit open
        self.success_threshold = 2  # Successes needed to close circuit
        
        # Provider priorities by data type
        self.provider_priorities = {
            'prices': [
                ('yahoo', ProviderPriority.PRIMARY),   # Yahoo as primary free provider
                ('stooq', ProviderPriority.SECONDARY), # STOOQ as backup
                ('financialdatasets', ProviderPriority.FALLBACK),
            ],
            'financial_metrics': [
                ('yahoo', ProviderPriority.PRIMARY),   # Yahoo for basic metrics
                ('financialdatasets', ProviderPriority.SECONDARY),
            ],
            'company_news': [
                ('financialdatasets', ProviderPriority.PRIMARY),
            ],
            'insider_trades': [
                ('financialdatasets', ProviderPriority.PRIMARY),
            ],
            'line_items': [
                ('sec_edgar', ProviderPriority.PRIMARY),  # SEC Edgar for US companies
                ('financialdatasets', ProviderPriority.SECONDARY),
            ],
            'company_facts': [
                ('yahoo', ProviderPriority.PRIMARY),   # Yahoo for basic facts
                ('sec_edgar', ProviderPriority.SECONDARY), # SEC Edgar for US regulatory data
                ('financialdatasets', ProviderPriority.FALLBACK),
            ],
            'market_cap': [
                ('yahoo', ProviderPriority.PRIMARY),   # Yahoo as primary
                ('financialdatasets', ProviderPriority.SECONDARY),
            ],
        }
        
        logger.info(f"Initialized DataProviderManager with {len(self.providers)} providers")
    
    def _is_circuit_open(self, provider_name: str) -> bool:
        """Check if circuit breaker is open for a provider."""
        health = self.provider_health[provider_name]
        if health.circuit_open_until and datetime.now() < health.circuit_open_until:
            return True
        return False
    
    def _open_circuit(self, provider_name: str):
        """Open circuit breaker for a provider."""
        health = self.provider_health[provider_name]
        health.circuit_open_until = datetime.now() + self.circuit_timeout
        health.is_healthy = False
        logger.warning(f"Circuit breaker opened for provider '{provider_name}' until {health.circuit_open_until}")
    
    def _close_circuit(self, provider_name: str):
        """Close circuit breaker for a provider."""
        health = self.provider_health[provider_name]
        health.circuit_open_until = None
        health.failure_count = 0
        health.is_healthy = True
        logger.info(f"Circuit breaker closed for provider '{provider_name}'")
    
    def _record_success(self, provider_name: str):
        """Record a successful request for a provider."""
        health = self.provider_health[provider_name]
        health.total_requests += 1
        health.successful_requests += 1
        
        # If circuit was open, check if we can close it
        if health.circuit_open_until:
            consecutive_successes = health.successful_requests - (health.total_requests - health.successful_requests)
            if consecutive_successes >= self.success_threshold:
                self._close_circuit(provider_name)
    
    def _record_failure(self, provider_name: str, error: Exception):
        """Record a failed request for a provider."""
        health = self.provider_health[provider_name]
        health.total_requests += 1
        health.failure_count += 1
        health.last_failure_time = datetime.now()
        
        # Open circuit breaker if failure threshold reached
        if health.failure_count >= self.failure_threshold and not health.circuit_open_until:
            self._open_circuit(provider_name)
        
        # Mark provider as unhealthy
        self.providers[provider_name].mark_unhealthy(error)
        
        logger.warning(f"Provider '{provider_name}' failed: {error}")
    
    def _get_available_providers(self, data_type: str) -> List[Tuple[str, DataProvider]]:
        """Get available providers for a data type, ordered by priority."""
        if data_type not in self.provider_priorities:
            logger.error(f"Unknown data type: {data_type}")
            return []
        
        available_providers = []
        for provider_name, priority in self.provider_priorities[data_type]:
            provider = self.providers[provider_name]
            
            # Skip if circuit breaker is open
            if self._is_circuit_open(provider_name):
                logger.debug(f"Skipping '{provider_name}' - circuit breaker open")
                continue
            
            # Skip if provider doesn't support the feature
            if not provider.supports_feature(data_type):
                logger.debug(f"Skipping '{provider_name}' - doesn't support {data_type}")
                continue
                
            available_providers.append((provider_name, provider))
        
        return available_providers
    
    def _execute_with_fallback(self, data_type: str, func_name: str, *args, **kwargs):
        """Execute a function with automatic provider fallback and caching."""
        # Create cache key arguments
        cache_method_map = {
            'get_prices': ('get_prices', 'set_prices'),
            'get_financial_metrics': ('get_financial_metrics', 'set_financial_metrics'),
            'get_company_news': ('get_company_news', 'set_company_news'),
            'get_insider_trades': ('get_insider_trades', 'set_insider_trades'),
            'search_line_items': ('search_line_items_cached', 'set_line_items'),
            'get_company_facts': ('get_company_facts', 'set_company_facts'),
            'get_market_cap': ('get_market_cap', 'set_market_cap'),
        }
        
        cache_get_method, cache_set_method = cache_method_map.get(func_name, (None, None))
        
        # First, check cache if available
        stale_data = None
        if cache_get_method:
            try:
                cache_result = getattr(self.cache, cache_get_method)(*args, **kwargs)
                if cache_result:
                    data, provider_name, cache_status = cache_result
                    logger.debug(f"Retrieved {data_type} from cache (provider: {provider_name}, status: {cache_status.value})")
                    
                    # If cache is fresh, return immediately
                    if cache_status == CacheEntryStatus.FRESH:
                        return data, provider_name
                    
                    # If cache is stale, try to refresh but fall back to cached data if needed
                    stale_data = data, provider_name
            except Exception as e:
                logger.debug(f"Cache lookup failed for {data_type}: {e}")
        
        available_providers = self._get_available_providers(data_type)
        
        if not available_providers:
            if stale_data:
                logger.info(f"No available providers for {data_type}, returning stale cached data")
                return stale_data
            raise DataProviderError(f"No available providers for {data_type}")
        
        last_error = None
        for provider_name, provider in available_providers:
            try:
                logger.debug(f"Trying provider '{provider_name}' for {data_type}")
                func = getattr(provider, func_name)
                result = func(*args, **kwargs)
                
                # Cache the result if caching is available
                if cache_set_method:
                    try:
                        cache_setter = getattr(self.cache, cache_set_method)
                        if func_name == 'search_line_items':
                            # Special handling for line items
                            cache_setter(*args[:3], args[4], args[3], result, provider_name)
                        elif func_name in ['get_company_facts', 'get_market_cap']:
                            # Single argument methods
                            cache_setter(*args, result, provider_name)
                        else:
                            # Multi-argument methods
                            cache_setter(*args, result, provider_name)
                    except Exception as e:
                        logger.warning(f"Failed to cache result for {data_type}: {e}")
                
                # Record success
                self._record_success(provider_name)
                logger.debug(f"Successfully retrieved {data_type} from '{provider_name}'")
                return result, provider_name
                
            except DataProviderRateLimitError as e:
                last_error = e
                logger.warning(f"Provider '{provider_name}' rate limited for {data_type}: {e}")
                self._record_failure(provider_name, e)
                continue
                
            except DataProviderNotFoundError as e:
                last_error = e
                logger.debug(f"Data not found in provider '{provider_name}' for {data_type}: {e}")
                # Don't record as failure - data might just not exist in this provider
                continue
                
            except Exception as e:
                last_error = e
                logger.error(f"Provider '{provider_name}' failed for {data_type}: {e}")
                self._record_failure(provider_name, e)
                continue
        
        # All providers failed - return stale data if available
        if stale_data:
            logger.warning(f"All providers failed for {data_type}, returning stale cached data. Last error: {last_error}")
            return stale_data
        
        # No data available at all
        raise DataProviderError(f"All providers failed for {data_type}. Last error: {last_error}")
    
    # Public API methods that use the fallback logic
    
    def get_prices(self, ticker: str, start_date: str, end_date: str) -> Tuple[List[Price], str]:
        """
        Get price data with automatic provider fallback.
        
        Returns:
            Tuple of (prices_list, provider_name_used)
        """
        return self._execute_with_fallback('prices', 'get_prices', ticker, start_date, end_date)
    
    def get_financial_metrics(
        self,
        ticker: str,
        end_date: str,
        period: str = "ttm",
        limit: int = 10,
    ) -> Tuple[List[FinancialMetrics], str]:
        """
        Get financial metrics with automatic provider fallback.
        
        Returns:
            Tuple of (metrics_list, provider_name_used)
        """
        return self._execute_with_fallback(
            'financial_metrics', 'get_financial_metrics', 
            ticker, end_date, period, limit
        )
    
    def get_company_news(
        self,
        ticker: str,
        end_date: str,
        start_date: Optional[str] = None,
        limit: int = 1000,
    ) -> Tuple[List[CompanyNews], str]:
        """
        Get company news with automatic provider fallback.
        
        Returns:
            Tuple of (news_list, provider_name_used)
        """
        return self._execute_with_fallback(
            'company_news', 'get_company_news',
            ticker, end_date, start_date, limit
        )
    
    def get_insider_trades(
        self,
        ticker: str,
        end_date: str,
        start_date: Optional[str] = None,
        limit: int = 1000,
    ) -> Tuple[List[InsiderTrade], str]:
        """
        Get insider trades with automatic provider fallback.
        
        Returns:
            Tuple of (trades_list, provider_name_used)
        """
        return self._execute_with_fallback(
            'insider_trades', 'get_insider_trades',
            ticker, end_date, start_date, limit
        )
    
    def search_line_items(
        self,
        ticker: str,
        line_items: List[str],
        end_date: str,
        period: str = "ttm",
        limit: int = 10,
    ) -> Tuple[List[LineItem], str]:
        """
        Search line items with automatic provider fallback.
        
        Returns:
            Tuple of (line_items_list, provider_name_used)
        """
        return self._execute_with_fallback(
            'line_items', 'search_line_items',
            ticker, line_items, end_date, period, limit
        )
    
    def get_company_facts(self, ticker: str) -> Tuple[Optional[CompanyFacts], str]:
        """
        Get company facts with automatic provider fallback.
        
        Returns:
            Tuple of (company_facts, provider_name_used)
        """
        return self._execute_with_fallback('company_facts', 'get_company_facts', ticker)
    
    def get_market_cap(self, ticker: str, end_date: str) -> Tuple[Optional[float], str]:
        """
        Get market cap with automatic provider fallback.
        
        Returns:
            Tuple of (market_cap, provider_name_used)
        """
        return self._execute_with_fallback('market_cap', 'get_market_cap', ticker, end_date)
    
    # Utility methods for monitoring and management
    
    def get_provider_health_status(self) -> Dict[str, Dict[str, Any]]:
        """Get health status of all providers."""
        status = {}
        for name, provider in self.providers.items():
            health = self.provider_health[name]
            status[name] = {
                'healthy': health.is_healthy,
                'circuit_open': self._is_circuit_open(name),
                'circuit_open_until': health.circuit_open_until.isoformat() if health.circuit_open_until else None,
                'failure_count': health.failure_count,
                'total_requests': health.total_requests,
                'successful_requests': health.successful_requests,
                'success_rate': health.successful_requests / health.total_requests if health.total_requests > 0 else 0,
                'last_failure': health.last_failure_time.isoformat() if health.last_failure_time else None,
                'last_error': str(provider.last_error) if provider.last_error else None,
            }
        return status
    
    def reset_provider_health(self, provider_name: str):
        """Reset health status for a specific provider."""
        if provider_name in self.provider_health:
            self.provider_health[provider_name] = ProviderHealth()
            self.providers[provider_name].mark_healthy()
            logger.info(f"Reset health status for provider '{provider_name}'")
    
    def get_supported_features(self) -> Dict[str, List[str]]:
        """Get supported features by each provider."""
        features = {}
        for name, provider in self.providers.items():
            supported = []
            for feature in ['prices', 'financial_metrics', 'company_news', 'insider_trades', 'line_items', 'company_facts', 'market_cap']:
                if provider.supports_feature(feature):
                    supported.append(feature)
            features[name] = supported
        return features
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        return self.cache.get_cache_stats()
    
    def clear_cache(self, provider: Optional[str] = None):
        """Clear cache data."""
        if provider:
            self.cache.clear_provider_data(provider)
            logger.info(f"Cleared cache for provider '{provider}'")
        else:
            self.cache.clear_all()
            logger.info("Cleared all cache data")
    
    def warm_cache(self, tickers: List[str], days_back: int = 30):
        """
        Warm the cache with recent data for given tickers.
        
        Args:
            tickers: List of ticker symbols to warm cache for
            days_back: Number of days of price data to cache
        """
        from datetime import datetime, timedelta
        
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
        
        logger.info(f"Warming cache for {len(tickers)} tickers")
        
        for ticker in tickers:
            try:
                # Warm price data
                self.get_prices(ticker, start_date, end_date)
                
                # Warm financial metrics
                self.get_financial_metrics(ticker, end_date, limit=1)
                
                # Warm company facts
                self.get_company_facts(ticker)
                
                logger.debug(f"Warmed cache for {ticker}")
                
            except Exception as e:
                logger.warning(f"Failed to warm cache for {ticker}: {e}")
    
    def __str__(self) -> str:
        """String representation of the provider manager."""
        healthy_count = sum(1 for health in self.provider_health.values() if health.is_healthy)
        return f"DataProviderManager({healthy_count}/{len(self.providers)} providers healthy)"
    
    def __repr__(self) -> str:
        """Detailed representation of the provider manager."""
        return f"DataProviderManager(providers={list(self.providers.keys())}, cache_entries={len(self.cache._cache)})"