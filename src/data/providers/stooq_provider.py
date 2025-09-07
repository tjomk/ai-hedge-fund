import time
import datetime
from typing import List, Optional
import pandas as pd
from requests.exceptions import HTTPError, ConnectionError

try:
    import pandas_datareader.data as pdr
    PANDAS_DATAREADER_AVAILABLE = True
except ImportError as e:
    pdr = None
    PANDAS_DATAREADER_AVAILABLE = False
    _import_error = str(e)

from .base import (
    DataProvider,
    DataProviderError,
    DataProviderRateLimitError,
    DataProviderTimeoutError,
    DataProviderNotFoundError,
)
from src.data.models import (
    Price,
    FinancialMetrics,
    CompanyNews,
    InsiderTrade,
    LineItem,
    CompanyFacts,
)


class StooqProvider(DataProvider):
    """
    STOOQ data provider using pandas-datareader.
    
    This provider offers free access to:
    - Historical price data for stocks, indices, currencies, commodities
    - International market coverage
    
    Note: STOOQ has limited fundamental data compared to specialized providers.
    """
    
    def __init__(self, api_key: Optional[str] = None):
        super().__init__("STOOQ", api_key)
        self.max_retries = 3
        self.retry_delay = 2.0
        
        if not PANDAS_DATAREADER_AVAILABLE:
            self.mark_unhealthy(ImportError(f"pandas-datareader not available: {_import_error}"))
        
    def _handle_stooq_errors(self, func, *args, **kwargs):
        """
        Wrapper to handle STOOQ/pandas-datareader errors and implement retry logic.
        """
        last_exception = None
        
        for attempt in range(self.max_retries):
            try:
                return func(*args, **kwargs)
            except HTTPError as e:
                last_exception = e
                if e.response.status_code == 429:
                    # Rate limited
                    delay = self.retry_delay * (2 ** attempt)  # Exponential backoff
                    print(f"STOOQ rate limited. Retrying in {delay}s...")
                    time.sleep(delay)
                    continue
                elif e.response.status_code == 404:
                    # Not found
                    raise DataProviderNotFoundError(f"STOOQ data not found: {e}")
                else:
                    # Other HTTP error
                    self.mark_unhealthy(e)
                    raise DataProviderError(f"STOOQ HTTP error: {e}")
            except ConnectionError as e:
                last_exception = e
                if attempt < self.max_retries - 1:
                    delay = self.retry_delay * (2 ** attempt)
                    print(f"STOOQ connection error. Retrying in {delay}s...")
                    time.sleep(delay)
                    continue
                else:
                    self.mark_unhealthy(e)
                    raise DataProviderError(f"STOOQ connection error: {e}")
            except Exception as e:
                last_exception = e
                if "Too Many Requests" in str(e) or "429" in str(e):
                    # Rate limited
                    delay = self.retry_delay * (2 ** attempt)
                    print(f"STOOQ rate limited. Retrying in {delay}s...")
                    time.sleep(delay)
                    continue
                elif "No data fetched" in str(e) or "not found" in str(e).lower():
                    # No data available
                    raise DataProviderNotFoundError(f"STOOQ data not found: {e}")
                else:
                    # Other error
                    break
        
        # All retries failed
        self.mark_unhealthy(last_exception)
        if "Too Many Requests" in str(last_exception) or "429" in str(last_exception):
            raise DataProviderRateLimitError(f"STOOQ rate limit exceeded: {last_exception}")
        else:
            raise DataProviderError(f"STOOQ error: {last_exception}")
    
    def _convert_ticker_for_stooq(self, ticker: str) -> str:
        """
        Convert ticker symbol to STOOQ format if needed.
        STOOQ uses different conventions for some tickers.
        """
        # Add .US suffix for US stocks if not already present
        if '.' not in ticker and len(ticker) <= 5:
            return f"{ticker}.US"
        return ticker
    
    def get_prices(self, ticker: str, start_date: str, end_date: str) -> List[Price]:
        """Fetch price data from STOOQ."""
        if not PANDAS_DATAREADER_AVAILABLE:
            raise DataProviderError(f"pandas-datareader not available: {_import_error}")
            
        def _fetch_prices():
            stooq_ticker = self._convert_ticker_for_stooq(ticker)
            
            try:
                df = pdr.DataReader(stooq_ticker, 'stooq', start_date, end_date)
            except Exception as e:
                # Try without .US suffix if it fails
                if '.US' in stooq_ticker:
                    original_ticker = ticker.replace('.US', '')
                    df = pdr.DataReader(original_ticker, 'stooq', start_date, end_date)
                else:
                    raise e
            
            if df.empty:
                return []
            
            # STOOQ data comes in reverse chronological order, so we need to reverse it
            df = df.sort_index()
            
            prices = []
            for date, row in df.iterrows():
                # Skip rows with NaN values
                if pd.isna(row['Open']) or pd.isna(row['Close']):
                    continue
                    
                price = Price(
                    open=float(row['Open']),
                    close=float(row['Close']),
                    high=float(row['High']),
                    low=float(row['Low']),
                    volume=int(row['Volume']) if not pd.isna(row['Volume']) else 0,
                    time=date.strftime('%Y-%m-%d')
                )
                prices.append(price)
            
            return prices
        
        try:
            prices = self._handle_stooq_errors(_fetch_prices)
            self.mark_healthy()
            return prices
        except Exception as e:
            raise e
    
    def get_financial_metrics(
        self,
        ticker: str,
        end_date: str,
        period: str = "ttm",
        limit: int = 10,
    ) -> List[FinancialMetrics]:
        """
        STOOQ doesn't provide financial metrics.
        This method returns an empty list and should be handled by other providers.
        """
        return []
    
    def get_company_news(
        self,
        ticker: str,
        end_date: str,
        start_date: Optional[str] = None,
        limit: int = 1000,
    ) -> List[CompanyNews]:
        """
        STOOQ doesn't provide company news.
        This method returns an empty list and should be handled by other providers.
        """
        return []
    
    def get_insider_trades(
        self,
        ticker: str,
        end_date: str,
        start_date: Optional[str] = None,
        limit: int = 1000,
    ) -> List[InsiderTrade]:
        """
        STOOQ doesn't provide insider trades.
        This method returns an empty list and should be handled by other providers.
        """
        return []
    
    def search_line_items(
        self,
        ticker: str,
        line_items: List[str],
        end_date: str,
        period: str = "ttm",
        limit: int = 10,
    ) -> List[LineItem]:
        """
        STOOQ doesn't provide line item searches.
        This method returns an empty list and should be handled by other providers.
        """
        return []
    
    def get_company_facts(self, ticker: str) -> Optional[CompanyFacts]:
        """
        STOOQ doesn't provide company facts.
        This method returns None and should be handled by other providers.
        """
        return None
    
    def get_market_cap(self, ticker: str, end_date: str) -> Optional[float]:
        """
        STOOQ doesn't provide market capitalization data.
        This method returns None and should be handled by other providers.
        """
        return None
    
    def supports_feature(self, feature: str) -> bool:
        """Override to specify which features STOOQ supports."""
        supported_features = {
            'prices': True,             # Primary strength of STOOQ
            'financial_metrics': False, # Not available
            'company_news': False,      # Not available
            'insider_trades': False,    # Not available
            'line_items': False,        # Not available
            'company_facts': False,     # Not available
            'market_cap': False,        # Not available
        }
        return supported_features.get(feature, False)
    
    def get_available_indices(self) -> List[str]:
        """
        Get a list of popular indices available on STOOQ.
        This is a convenience method specific to STOOQ.
        """
        return [
            '^SPX',    # S&P 500
            '^DJI',    # Dow Jones Industrial Average
            '^IXIC',   # NASDAQ Composite
            '^RUT',    # Russell 2000
            '^VIX',    # CBOE Volatility Index
            '^TNX',    # 10-Year Treasury Note Yield
            '^GSPC',   # S&P 500 (alternative symbol)
        ]
    
    def get_available_currencies(self) -> List[str]:
        """
        Get a list of popular currency pairs available on STOOQ.
        This is a convenience method specific to STOOQ.
        """
        return [
            'EURUSD',  # Euro to US Dollar
            'GBPUSD',  # British Pound to US Dollar
            'USDJPY',  # US Dollar to Japanese Yen
            'USDCHF',  # US Dollar to Swiss Franc
            'AUDUSD',  # Australian Dollar to US Dollar
            'USDCAD',  # US Dollar to Canadian Dollar
            'NZDUSD',  # New Zealand Dollar to US Dollar
        ]
    
    def get_available_commodities(self) -> List[str]:
        """
        Get a list of popular commodities available on STOOQ.
        This is a convenience method specific to STOOQ.
        """
        return [
            'GC.F',    # Gold Futures
            'SI.F',    # Silver Futures
            'CL.F',    # Crude Oil Futures
            'NG.F',    # Natural Gas Futures
            'HG.F',    # Copper Futures
            'ZC.F',    # Corn Futures
            'ZS.F',    # Soybean Futures
        ]