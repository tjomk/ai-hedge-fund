import os
import time
from typing import List, Optional
import requests

from .base import (
    DataProvider,
    DataProviderError,
    DataProviderRateLimitError,
    DataProviderTimeoutError,
    DataProviderNotFoundError,
)
from src.data.models import (
    Price,
    PriceResponse,
    FinancialMetrics,
    FinancialMetricsResponse,
    CompanyNews,
    CompanyNewsResponse,
    InsiderTrade,
    InsiderTradeResponse,
    LineItem,
    LineItemResponse,
    CompanyFacts,
    CompanyFactsResponse,
)


class FinancialDatasetsProvider(DataProvider):
    """
    FinancialDatasets.ai data provider (legacy).
    
    This provider offers comprehensive financial data but requires an API key
    and has rate limits. It's kept for backward compatibility and as a fallback
    for advanced features not available in free providers.
    """
    
    def __init__(self, api_key: Optional[str] = None):
        api_key = api_key or os.environ.get("FINANCIAL_DATASETS_API_KEY")
        super().__init__("FinancialDatasets.ai", api_key)
        self.base_url = "https://api.financialdatasets.ai"
        self.max_retries = 3
        
    def _make_request(self, url: str, method: str = "GET", json_data: dict = None) -> requests.Response:
        """Make a request to the FinancialDatasets API with rate limiting."""
        headers = {}
        if self.api_key:
            headers["X-API-KEY"] = self.api_key
        
        last_exception = None
        
        for attempt in range(self.max_retries + 1):
            try:
                if method.upper() == "POST":
                    response = requests.post(url, headers=headers, json=json_data)
                else:
                    response = requests.get(url, headers=headers)
                
                if response.status_code == 429 and attempt < self.max_retries:
                    # Linear backoff: 60s, 90s, 120s, 150s...
                    delay = 60 + (30 * attempt)
                    print(f"FinancialDatasets rate limited. Waiting {delay}s before retrying...")
                    time.sleep(delay)
                    continue
                elif response.status_code == 404:
                    raise DataProviderNotFoundError(f"Data not found: {response.text}")
                elif response.status_code != 200:
                    raise DataProviderError(f"API error {response.status_code}: {response.text}")
                
                self.mark_healthy()
                return response
                
            except requests.exceptions.RequestException as e:
                last_exception = e
                if attempt < self.max_retries:
                    delay = 30 * (attempt + 1)
                    print(f"FinancialDatasets request failed. Retrying in {delay}s...")
                    time.sleep(delay)
                    continue
                else:
                    self.mark_unhealthy(e)
                    raise DataProviderError(f"Request failed: {e}")
        
        # All retries failed
        self.mark_unhealthy(last_exception)
        raise DataProviderRateLimitError(f"Rate limit exceeded after {self.max_retries} retries")
    
    def get_prices(self, ticker: str, start_date: str, end_date: str) -> List[Price]:
        """Fetch price data from FinancialDatasets API."""
        url = f"{self.base_url}/prices/?ticker={ticker}&interval=day&interval_multiplier=1&start_date={start_date}&end_date={end_date}"
        
        try:
            response = self._make_request(url)
            price_response = PriceResponse(**response.json())
            return price_response.prices
        except Exception as e:
            raise e
    
    def get_financial_metrics(
        self,
        ticker: str,
        end_date: str,
        period: str = "ttm",
        limit: int = 10,
    ) -> List[FinancialMetrics]:
        """Fetch financial metrics from FinancialDatasets API."""
        url = f"{self.base_url}/financial-metrics/?ticker={ticker}&report_period_lte={end_date}&limit={limit}&period={period}"
        
        try:
            response = self._make_request(url)
            metrics_response = FinancialMetricsResponse(**response.json())
            return metrics_response.financial_metrics
        except Exception as e:
            raise e
    
    def get_company_news(
        self,
        ticker: str,
        end_date: str,
        start_date: Optional[str] = None,
        limit: int = 1000,
    ) -> List[CompanyNews]:
        """Fetch company news from FinancialDatasets API."""
        url = f"{self.base_url}/news/?ticker={ticker}&end_date={end_date}"
        if start_date:
            url += f"&start_date={start_date}"
        url += f"&limit={limit}"
        
        try:
            response = self._make_request(url)
            news_response = CompanyNewsResponse(**response.json())
            return news_response.news
        except Exception as e:
            raise e
    
    def get_insider_trades(
        self,
        ticker: str,
        end_date: str,
        start_date: Optional[str] = None,
        limit: int = 1000,
    ) -> List[InsiderTrade]:
        """Fetch insider trades from FinancialDatasets API."""
        url = f"{self.base_url}/insider-trades/?ticker={ticker}&filing_date_lte={end_date}"
        if start_date:
            url += f"&filing_date_gte={start_date}"
        url += f"&limit={limit}"
        
        try:
            response = self._make_request(url)
            trades_response = InsiderTradeResponse(**response.json())
            return trades_response.insider_trades
        except Exception as e:
            raise e
    
    def search_line_items(
        self,
        ticker: str,
        line_items: List[str],
        end_date: str,
        period: str = "ttm",
        limit: int = 10,
    ) -> List[LineItem]:
        """Search for line items in FinancialDatasets API."""
        url = f"{self.base_url}/financials/search/line-items"
        
        body = {
            "tickers": [ticker],
            "line_items": line_items,
            "end_date": end_date,
            "period": period,
            "limit": limit,
        }
        
        try:
            response = self._make_request(url, method="POST", json_data=body)
            search_response = LineItemResponse(**response.json())
            return search_response.search_results
        except Exception as e:
            raise e
    
    def get_company_facts(self, ticker: str) -> Optional[CompanyFacts]:
        """Fetch company facts from FinancialDatasets API."""
        url = f"{self.base_url}/company/facts/?ticker={ticker}"
        
        try:
            response = self._make_request(url)
            facts_response = CompanyFactsResponse(**response.json())
            return facts_response.company_facts
        except Exception as e:
            raise e
    
    def get_market_cap(self, ticker: str, end_date: str) -> Optional[float]:
        """Fetch market capitalization from FinancialDatasets API."""
        import datetime
        
        # Check if end_date is today
        if end_date == datetime.datetime.now().strftime("%Y-%m-%d"):
            # Get current market cap from company facts
            try:
                company_facts = self.get_company_facts(ticker)
                return company_facts.market_cap if company_facts else None
            except Exception:
                pass
        
        # Get historical market cap from financial metrics
        try:
            financial_metrics = self.get_financial_metrics(ticker, end_date)
            if financial_metrics:
                return financial_metrics[0].market_cap
            return None
        except Exception as e:
            raise e
    
    def supports_feature(self, feature: str) -> bool:
        """FinancialDatasets supports all features (if API key is available)."""
        if not self.api_key:
            return False
            
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