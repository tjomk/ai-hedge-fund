from abc import ABC, abstractmethod
from typing import List, Optional
import pandas as pd

from src.data.models import (
    Price,
    FinancialMetrics,
    CompanyNews,
    InsiderTrade,
    LineItem,
    CompanyFacts,
)


class DataProviderError(Exception):
    """Base exception for data provider errors."""
    pass


class DataProviderTimeoutError(DataProviderError):
    """Raised when a data provider request times out."""
    pass


class DataProviderRateLimitError(DataProviderError):
    """Raised when a data provider rate limit is exceeded."""
    pass


class DataProviderNotFoundError(DataProviderError):
    """Raised when requested data is not found."""
    pass


class DataProvider(ABC):
    """
    Abstract base class for financial data providers.
    
    This class defines the interface that all data providers must implement.
    Each provider should handle its own rate limiting, error handling, and
    data format conversion.
    """
    
    def __init__(self, name: str, api_key: Optional[str] = None):
        self.name = name
        self.api_key = api_key
        self._is_healthy = True
        self._last_error = None
        
    @property
    def is_healthy(self) -> bool:
        """Return whether the provider is currently healthy."""
        return self._is_healthy
        
    @property
    def last_error(self) -> Optional[Exception]:
        """Return the last error encountered by this provider."""
        return self._last_error
    
    def mark_unhealthy(self, error: Exception):
        """Mark the provider as unhealthy with the given error."""
        self._is_healthy = False
        self._last_error = error
        
    def mark_healthy(self):
        """Mark the provider as healthy."""
        self._is_healthy = True
        self._last_error = None
    
    @abstractmethod
    def get_prices(
        self, 
        ticker: str, 
        start_date: str, 
        end_date: str
    ) -> List[Price]:
        """
        Fetch price data for a ticker between start_date and end_date.
        
        Args:
            ticker: Stock ticker symbol (e.g., 'AAPL')
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            List of Price objects containing OHLCV data
            
        Raises:
            DataProviderError: If the request fails
        """
        pass
    
    @abstractmethod 
    def get_financial_metrics(
        self,
        ticker: str,
        end_date: str,
        period: str = "ttm",
        limit: int = 10,
    ) -> List[FinancialMetrics]:
        """
        Fetch financial metrics for a ticker.
        
        Args:
            ticker: Stock ticker symbol
            end_date: End date for metrics in YYYY-MM-DD format
            period: Period for metrics ("ttm", "annual", "quarterly")
            limit: Maximum number of periods to return
            
        Returns:
            List of FinancialMetrics objects
            
        Raises:
            DataProviderError: If the request fails
        """
        pass
    
    @abstractmethod
    def get_company_news(
        self,
        ticker: str,
        end_date: str,
        start_date: Optional[str] = None,
        limit: int = 1000,
    ) -> List[CompanyNews]:
        """
        Fetch company news for a ticker.
        
        Args:
            ticker: Stock ticker symbol
            end_date: End date for news in YYYY-MM-DD format  
            start_date: Start date for news in YYYY-MM-DD format
            limit: Maximum number of news articles to return
            
        Returns:
            List of CompanyNews objects
            
        Raises:
            DataProviderError: If the request fails
        """
        pass
    
    @abstractmethod
    def get_insider_trades(
        self,
        ticker: str,
        end_date: str,
        start_date: Optional[str] = None,
        limit: int = 1000,
    ) -> List[InsiderTrade]:
        """
        Fetch insider trades for a ticker.
        
        Args:
            ticker: Stock ticker symbol
            end_date: End date for trades in YYYY-MM-DD format
            start_date: Start date for trades in YYYY-MM-DD format  
            limit: Maximum number of trades to return
            
        Returns:
            List of InsiderTrade objects
            
        Raises:
            DataProviderError: If the request fails
        """
        pass
    
    @abstractmethod
    def search_line_items(
        self,
        ticker: str,
        line_items: List[str],
        end_date: str,
        period: str = "ttm",
        limit: int = 10,
    ) -> List[LineItem]:
        """
        Search for specific line items in financial statements.
        
        Args:
            ticker: Stock ticker symbol
            line_items: List of line item names to search for
            end_date: End date for search in YYYY-MM-DD format
            period: Period for search ("ttm", "annual", "quarterly")
            limit: Maximum number of results to return
            
        Returns:
            List of LineItem objects
            
        Raises:
            DataProviderError: If the request fails
        """
        pass
    
    @abstractmethod
    def get_company_facts(
        self,
        ticker: str,
    ) -> Optional[CompanyFacts]:
        """
        Fetch basic company facts and information.
        
        Args:
            ticker: Stock ticker symbol
            
        Returns:
            CompanyFacts object or None if not found
            
        Raises:
            DataProviderError: If the request fails
        """
        pass
    
    @abstractmethod
    def get_market_cap(
        self,
        ticker: str,
        end_date: str,
    ) -> Optional[float]:
        """
        Fetch market capitalization for a ticker.
        
        Args:
            ticker: Stock ticker symbol
            end_date: Date for market cap in YYYY-MM-DD format
            
        Returns:
            Market cap as float or None if not available
            
        Raises:
            DataProviderError: If the request fails
        """
        pass
    
    def get_price_data_as_dataframe(
        self, 
        ticker: str, 
        start_date: str, 
        end_date: str
    ) -> pd.DataFrame:
        """
        Convenience method to get price data as a pandas DataFrame.
        
        Args:
            ticker: Stock ticker symbol
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            DataFrame with OHLCV data indexed by date
            
        Raises:
            DataProviderError: If the request fails
        """
        prices = self.get_prices(ticker, start_date, end_date)
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
    
    def supports_feature(self, feature: str) -> bool:
        """
        Check if this provider supports a specific feature.
        
        Args:
            feature: Feature name (e.g., 'prices', 'financial_metrics', 'news')
            
        Returns:
            True if feature is supported, False otherwise
        """
        supported_features = {
            'prices': True,
            'financial_metrics': True, 
            'company_news': True,
            'insider_trades': True,
            'line_items': True,
            'company_facts': True,
            'market_cap': True,
        }
        return supported_features.get(feature, False)
    
    def __str__(self) -> str:
        status = "healthy" if self._is_healthy else "unhealthy"
        return f"{self.name} ({status})"
    
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name='{self.name}', healthy={self._is_healthy})"