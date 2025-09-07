import time
import hashlib
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from enum import Enum

from src.data.models import (
    Price,
    FinancialMetrics,
    CompanyNews,
    InsiderTrade,
    LineItem,
    CompanyFacts,
)


class CacheEntryStatus(Enum):
    """Status of cache entries."""
    FRESH = "fresh"
    STALE = "stale"
    EXPIRED = "expired"


@dataclass
class CacheEntry:
    """Represents a cached data entry with metadata."""
    data: Any
    provider: str
    timestamp: datetime
    cache_key: str
    data_type: str
    ttl_seconds: int = 3600  # 1 hour default TTL
    
    @property
    def age(self) -> timedelta:
        """Get age of the cache entry."""
        return datetime.now() - self.timestamp
    
    @property
    def status(self) -> CacheEntryStatus:
        """Get status of the cache entry."""
        age_seconds = self.age.total_seconds()
        if age_seconds < self.ttl_seconds * 0.8:  # 80% of TTL
            return CacheEntryStatus.FRESH
        elif age_seconds < self.ttl_seconds:
            return CacheEntryStatus.STALE
        else:
            return CacheEntryStatus.EXPIRED
    
    @property
    def is_valid(self) -> bool:
        """Check if cache entry is still valid (not expired)."""
        return self.status != CacheEntryStatus.EXPIRED


class ProviderAwareCache:
    """
    Enhanced cache system that tracks data by provider and implements
    intelligent cache management with TTL and provider preference.
    """
    
    def __init__(self):
        self._cache: Dict[str, CacheEntry] = {}
        
        # TTL configurations by data type (in seconds)
        self._ttl_config = {
            'prices': 300,          # 5 minutes for price data
            'financial_metrics': 3600,  # 1 hour for financial metrics
            'company_news': 1800,   # 30 minutes for news
            'insider_trades': 7200, # 2 hours for insider trades
            'line_items': 3600,     # 1 hour for line items
            'company_facts': 86400, # 24 hours for company facts
            'market_cap': 1800,     # 30 minutes for market cap
        }
        
        # Provider preferences (higher score = preferred)
        self._provider_scores = {
            'yahoo': 10,
            'stooq': 8,
            'financialdatasets': 9,
        }
    
    def _generate_cache_key(self, data_type: str, *args, **kwargs) -> str:
        """Generate a unique cache key for the request."""
        # Convert args and kwargs to a consistent string representation
        key_data = {
            'data_type': data_type,
            'args': args,
            'kwargs': sorted(kwargs.items())
        }
        key_string = json.dumps(key_data, sort_keys=True)
        return hashlib.md5(key_string.encode()).hexdigest()
    
    def _get_ttl(self, data_type: str) -> int:
        """Get TTL for a specific data type."""
        return self._ttl_config.get(data_type, 3600)  # Default 1 hour
    
    def get(
        self, 
        data_type: str, 
        *args, 
        **kwargs
    ) -> Optional[Tuple[Any, str, CacheEntryStatus]]:
        """
        Get cached data if available and valid.
        
        Returns:
            Tuple of (data, provider_name, cache_status) or None if not cached/expired
        """
        cache_key = self._generate_cache_key(data_type, *args, **kwargs)
        
        if cache_key not in self._cache:
            return None
        
        entry = self._cache[cache_key]
        
        # Remove expired entries
        if not entry.is_valid:
            del self._cache[cache_key]
            return None
        
        return entry.data, entry.provider, entry.status
    
    def set(
        self, 
        data_type: str, 
        data: Any, 
        provider: str,
        *args, 
        **kwargs
    ):
        """
        Cache data with provider metadata.
        
        Args:
            data_type: Type of data being cached
            data: The data to cache
            provider: Name of the provider that supplied the data
            *args, **kwargs: Arguments used to fetch the data (for cache key)
        """
        cache_key = self._generate_cache_key(data_type, *args, **kwargs)
        ttl = self._get_ttl(data_type)
        
        # Check if we already have data from a preferred provider
        if cache_key in self._cache:
            existing_entry = self._cache[cache_key]
            existing_score = self._provider_scores.get(existing_entry.provider, 0)
            new_score = self._provider_scores.get(provider, 0)
            
            # Only replace if new provider is better or existing data is stale
            if (existing_entry.status == CacheEntryStatus.FRESH and 
                new_score <= existing_score):
                return  # Keep existing data
        
        entry = CacheEntry(
            data=data,
            provider=provider,
            timestamp=datetime.now(),
            cache_key=cache_key,
            data_type=data_type,
            ttl_seconds=ttl
        )
        
        self._cache[cache_key] = entry
    
    def get_prices(
        self, 
        ticker: str, 
        start_date: str, 
        end_date: str
    ) -> Optional[Tuple[List[Price], str, CacheEntryStatus]]:
        """Get cached price data."""
        return self.get('prices', ticker, start_date, end_date)
    
    def set_prices(
        self, 
        ticker: str, 
        start_date: str, 
        end_date: str, 
        prices: List[Price], 
        provider: str
    ):
        """Cache price data."""
        self.set('prices', prices, provider, ticker, start_date, end_date)
    
    def get_financial_metrics(
        self,
        ticker: str,
        end_date: str,
        period: str = "ttm",
        limit: int = 10,
    ) -> Optional[Tuple[List[FinancialMetrics], str, CacheEntryStatus]]:
        """Get cached financial metrics."""
        return self.get('financial_metrics', ticker, end_date, period, limit)
    
    def set_financial_metrics(
        self,
        ticker: str,
        end_date: str,
        period: str,
        limit: int,
        metrics: List[FinancialMetrics],
        provider: str,
    ):
        """Cache financial metrics."""
        self.set('financial_metrics', metrics, provider, ticker, end_date, period, limit)
    
    def get_company_news(
        self,
        ticker: str,
        end_date: str,
        start_date: Optional[str] = None,
        limit: int = 1000,
    ) -> Optional[Tuple[List[CompanyNews], str, CacheEntryStatus]]:
        """Get cached company news."""
        return self.get('company_news', ticker, end_date, start_date, limit)
    
    def set_company_news(
        self,
        ticker: str,
        end_date: str,
        start_date: Optional[str],
        limit: int,
        news: List[CompanyNews],
        provider: str,
    ):
        """Cache company news."""
        self.set('company_news', news, provider, ticker, end_date, start_date, limit)
    
    def get_insider_trades(
        self,
        ticker: str,
        end_date: str,
        start_date: Optional[str] = None,
        limit: int = 1000,
    ) -> Optional[Tuple[List[InsiderTrade], str, CacheEntryStatus]]:
        """Get cached insider trades."""
        return self.get('insider_trades', ticker, end_date, start_date, limit)
    
    def set_insider_trades(
        self,
        ticker: str,
        end_date: str,
        start_date: Optional[str],
        limit: int,
        trades: List[InsiderTrade],
        provider: str,
    ):
        """Cache insider trades."""
        self.set('insider_trades', trades, provider, ticker, end_date, start_date, limit)
    
    def search_line_items_cached(
        self,
        ticker: str,
        line_items: List[str],
        end_date: str,
        period: str = "ttm",
        limit: int = 10,
    ) -> Optional[Tuple[List[LineItem], str, CacheEntryStatus]]:
        """Get cached line items."""
        # Convert list to tuple for hashing
        line_items_tuple = tuple(sorted(line_items))
        return self.get('line_items', ticker, line_items_tuple, end_date, period, limit)
    
    def set_line_items(
        self,
        ticker: str,
        line_items: List[str],
        end_date: str,
        period: str,
        limit: int,
        items: List[LineItem],
        provider: str,
    ):
        """Cache line items."""
        line_items_tuple = tuple(sorted(line_items))
        self.set('line_items', items, provider, ticker, line_items_tuple, end_date, period, limit)
    
    def get_company_facts(
        self, 
        ticker: str
    ) -> Optional[Tuple[Optional[CompanyFacts], str, CacheEntryStatus]]:
        """Get cached company facts."""
        return self.get('company_facts', ticker)
    
    def set_company_facts(
        self, 
        ticker: str, 
        facts: Optional[CompanyFacts], 
        provider: str
    ):
        """Cache company facts."""
        self.set('company_facts', facts, provider, ticker)
    
    def get_market_cap(
        self, 
        ticker: str, 
        end_date: str
    ) -> Optional[Tuple[Optional[float], str, CacheEntryStatus]]:
        """Get cached market cap."""
        return self.get('market_cap', ticker, end_date)
    
    def set_market_cap(
        self, 
        ticker: str, 
        end_date: str, 
        market_cap: Optional[float], 
        provider: str
    ):
        """Cache market cap."""
        self.set('market_cap', market_cap, provider, ticker, end_date)
    
    def clear_expired(self):
        """Remove expired cache entries."""
        expired_keys = [
            key for key, entry in self._cache.items() 
            if not entry.is_valid
        ]
        for key in expired_keys:
            del self._cache[key]
    
    def clear_provider_data(self, provider: str):
        """Remove all cached data from a specific provider."""
        provider_keys = [
            key for key, entry in self._cache.items()
            if entry.provider == provider
        ]
        for key in provider_keys:
            del self._cache[key]
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        total_entries = len(self._cache)
        if total_entries == 0:
            return {
                'total_entries': 0,
                'by_provider': {},
                'by_data_type': {},
                'by_status': {},
            }
        
        by_provider = {}
        by_data_type = {}
        by_status = {}
        
        for entry in self._cache.values():
            # Count by provider
            by_provider[entry.provider] = by_provider.get(entry.provider, 0) + 1
            
            # Count by data type
            by_data_type[entry.data_type] = by_data_type.get(entry.data_type, 0) + 1
            
            # Count by status
            status = entry.status.value
            by_status[status] = by_status.get(status, 0) + 1
        
        return {
            'total_entries': total_entries,
            'by_provider': by_provider,
            'by_data_type': by_data_type,
            'by_status': by_status,
        }
    
    def clear_all(self):
        """Clear all cached data."""
        self._cache.clear()


# Global cache instance
_enhanced_cache = ProviderAwareCache()


def get_enhanced_cache() -> ProviderAwareCache:
    """Get the global enhanced cache instance."""
    return _enhanced_cache