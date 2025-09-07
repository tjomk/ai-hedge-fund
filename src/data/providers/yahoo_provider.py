import time
import datetime
from typing import List, Optional
import yfinance as yf
import pandas as pd
from requests.exceptions import HTTPError

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


class YahooFinanceProvider(DataProvider):
    """
    Yahoo Finance data provider using yfinance library.
    
    This provider offers free access to:
    - Historical price data
    - Basic financial metrics
    - Company information
    
    Note: Yahoo Finance has informal rate limits and may block excessive requests.
    """
    
    def __init__(self, api_key: Optional[str] = None):
        super().__init__("Yahoo Finance", api_key)
        self.max_retries = 3
        self.retry_delay = 1.0
        
    def _handle_yfinance_errors(self, func, *args, **kwargs):
        """
        Wrapper to handle yfinance errors and implement retry logic.
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
                    print(f"Yahoo Finance rate limited. Retrying in {delay}s...")
                    time.sleep(delay)
                    continue
                else:
                    # Other HTTP error
                    self.mark_unhealthy(e)
                    raise DataProviderError(f"Yahoo Finance HTTP error: {e}")
            except Exception as e:
                last_exception = e
                if "Too Many Requests" in str(e) or "429" in str(e):
                    # Rate limited
                    delay = self.retry_delay * (2 ** attempt)
                    print(f"Yahoo Finance rate limited. Retrying in {delay}s...")
                    time.sleep(delay)
                    continue
                else:
                    # Other error
                    break
        
        # All retries failed
        self.mark_unhealthy(last_exception)
        if "Too Many Requests" in str(last_exception) or "429" in str(last_exception):
            raise DataProviderRateLimitError(f"Yahoo Finance rate limit exceeded: {last_exception}")
        else:
            raise DataProviderError(f"Yahoo Finance error: {last_exception}")
    
    def get_prices(self, ticker: str, start_date: str, end_date: str) -> List[Price]:
        """Fetch price data from Yahoo Finance."""
        def _fetch_prices():
            stock = yf.Ticker(ticker)
            df = stock.history(start=start_date, end=end_date, auto_adjust=True, prepost=True)
            
            if df.empty:
                return []
            
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
            prices = self._handle_yfinance_errors(_fetch_prices)
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
        Fetch financial metrics from Yahoo Finance.
        
        Note: Yahoo Finance provides limited financial metrics compared to
        specialized financial data providers. This method returns basic metrics.
        """
        def _fetch_metrics():
            stock = yf.Ticker(ticker)
            info = stock.info
            
            if not info:
                return []
            
            # Get quarterly and annual financials for historical data
            quarterly_financials = stock.quarterly_financials
            quarterly_balance_sheet = stock.quarterly_balance_sheet
            quarterly_cashflow = stock.quarterly_cashflow
            
            metrics = []
            
            # Current metrics from info
            current_metric = self._build_financial_metric_from_info(ticker, info, end_date, period)
            if current_metric:
                metrics.append(current_metric)
            
            # Historical quarterly metrics (if available and requested)
            if period in ["quarterly", "ttm"] and not quarterly_financials.empty:
                for i, date in enumerate(quarterly_financials.columns[:min(limit-1, len(quarterly_financials.columns))]):
                    historical_metric = self._build_historical_financial_metric(
                        ticker, info, quarterly_financials, quarterly_balance_sheet, 
                        quarterly_cashflow, date, "quarterly"
                    )
                    if historical_metric:
                        metrics.append(historical_metric)
            
            return metrics[:limit]
        
        try:
            metrics = self._handle_yfinance_errors(_fetch_metrics)
            self.mark_healthy()
            return metrics
        except Exception as e:
            raise e
    
    def _build_financial_metric_from_info(
        self, ticker: str, info: dict, end_date: str, period: str
    ) -> Optional[FinancialMetrics]:
        """Build a FinancialMetrics object from Yahoo Finance info dict."""
        try:
            return FinancialMetrics(
                ticker=ticker,
                report_period=end_date,
                period=period,
                currency=info.get('currency', 'USD'),
                market_cap=info.get('marketCap'),
                enterprise_value=info.get('enterpriseValue'),
                price_to_earnings_ratio=info.get('trailingPE'),
                price_to_book_ratio=info.get('priceToBook'),
                price_to_sales_ratio=info.get('priceToSalesTrailing12Months'),
                enterprise_value_to_ebitda_ratio=info.get('enterpriseToEbitda'),
                enterprise_value_to_revenue_ratio=info.get('enterpriseToRevenue'),
                free_cash_flow_yield=None,  # Not directly available in Yahoo Finance
                peg_ratio=info.get('pegRatio'),
                gross_margin=info.get('grossMargins'),
                operating_margin=info.get('operatingMargins'),
                net_margin=info.get('profitMargins'),
                return_on_equity=info.get('returnOnEquity'),
                return_on_assets=info.get('returnOnAssets'),
                return_on_invested_capital=None,  # Not available
                asset_turnover=None,  # Not directly available
                inventory_turnover=None,  # Not available
                receivables_turnover=None,  # Not available
                days_sales_outstanding=None,  # Not available
                operating_cycle=None,  # Not available
                working_capital_turnover=None,  # Not available
                current_ratio=info.get('currentRatio'),
                quick_ratio=info.get('quickRatio'),
                cash_ratio=None,  # Not available
                operating_cash_flow_ratio=None,  # Not available
                debt_to_equity=info.get('debtToEquity'),
                debt_to_assets=None,  # Not directly available
                interest_coverage=None,  # Not available
                revenue_growth=info.get('revenueGrowth'),
                earnings_growth=info.get('earningsGrowth'),
                book_value_growth=None,  # Not available
                earnings_per_share_growth=None,  # Not directly available
                free_cash_flow_growth=None,  # Not available
                operating_income_growth=None,  # Not available
                ebitda_growth=None,  # Not available
                payout_ratio=info.get('payoutRatio'),
                earnings_per_share=info.get('trailingEps'),
                book_value_per_share=info.get('bookValue'),
                free_cash_flow_per_share=None,  # Not directly available
            )
        except Exception:
            return None
    
    def _build_historical_financial_metric(
        self, ticker: str, info: dict, financials: pd.DataFrame,
        balance_sheet: pd.DataFrame, cashflow: pd.DataFrame, 
        date: pd.Timestamp, period: str
    ) -> Optional[FinancialMetrics]:
        """Build historical FinancialMetrics from Yahoo Finance data."""
        try:
            date_str = date.strftime('%Y-%m-%d')
            
            # Extract data from financial statements
            revenue = financials.loc['Total Revenue', date] if 'Total Revenue' in financials.index else None
            net_income = financials.loc['Net Income', date] if 'Net Income' in financials.index else None
            total_assets = balance_sheet.loc['Total Assets', date] if 'Total Assets' in balance_sheet.index else None
            total_equity = balance_sheet.loc['Total Stockholder Equity', date] if 'Total Stockholder Equity' in balance_sheet.index else None
            
            # Calculate basic ratios if data is available
            roe = (net_income / total_equity * 100) if (net_income and total_equity and total_equity != 0) else None
            roa = (net_income / total_assets * 100) if (net_income and total_assets and total_assets != 0) else None
            net_margin = (net_income / revenue * 100) if (net_income and revenue and revenue != 0) else None
            
            return FinancialMetrics(
                ticker=ticker,
                report_period=date_str,
                period=period,
                currency=info.get('currency', 'USD'),
                market_cap=None,  # Historical market cap not easily available
                enterprise_value=None,
                price_to_earnings_ratio=None,
                price_to_book_ratio=None,
                price_to_sales_ratio=None,
                enterprise_value_to_ebitda_ratio=None,
                enterprise_value_to_revenue_ratio=None,
                free_cash_flow_yield=None,
                peg_ratio=None,
                gross_margin=None,
                operating_margin=None,
                net_margin=roe,
                return_on_equity=roe,
                return_on_assets=roa,
                return_on_invested_capital=None,
                asset_turnover=None,
                inventory_turnover=None,
                receivables_turnover=None,
                days_sales_outstanding=None,
                operating_cycle=None,
                working_capital_turnover=None,
                current_ratio=None,
                quick_ratio=None,
                cash_ratio=None,
                operating_cash_flow_ratio=None,
                debt_to_equity=None,
                debt_to_assets=None,
                interest_coverage=None,
                revenue_growth=None,
                earnings_growth=None,
                book_value_growth=None,
                earnings_per_share_growth=None,
                free_cash_flow_growth=None,
                operating_income_growth=None,
                ebitda_growth=None,
                payout_ratio=None,
                earnings_per_share=None,
                book_value_per_share=None,
                free_cash_flow_per_share=None,
            )
        except Exception:
            return None
    
    def get_company_news(
        self,
        ticker: str,
        end_date: str,
        start_date: Optional[str] = None,
        limit: int = 1000,
    ) -> List[CompanyNews]:
        """
        Yahoo Finance doesn't provide news through yfinance library.
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
        Yahoo Finance doesn't provide insider trades through yfinance library.
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
        Yahoo Finance doesn't support line item searches in the same way.
        This method returns an empty list and should be handled by other providers.
        """
        return []
    
    def get_company_facts(self, ticker: str) -> Optional[CompanyFacts]:
        """Fetch company facts from Yahoo Finance."""
        def _fetch_facts():
            stock = yf.Ticker(ticker)
            info = stock.info
            
            if not info:
                return None
            
            return CompanyFacts(
                ticker=ticker,
                name=info.get('longName') or info.get('shortName', ticker),
                cik=None,  # Not available in Yahoo Finance
                industry=info.get('industry'),
                sector=info.get('sector'),
                category=None,  # Not available
                exchange=info.get('exchange'),
                is_active=True,  # Assume active if data is available
                listing_date=None,  # Not directly available
                location=f"{info.get('city', '')}, {info.get('state', '')} {info.get('country', '')}".strip(', '),
                market_cap=info.get('marketCap'),
                number_of_employees=info.get('fullTimeEmployees'),
                sec_filings_url=None,  # Not available
                sic_code=None,  # Not available
                sic_industry=info.get('industry'),
                sic_sector=info.get('sector'),
                website_url=info.get('website'),
                weighted_average_shares=info.get('sharesOutstanding'),
            )
        
        try:
            facts = self._handle_yfinance_errors(_fetch_facts)
            self.mark_healthy()
            return facts
        except Exception as e:
            raise e
    
    def get_market_cap(self, ticker: str, end_date: str) -> Optional[float]:
        """Fetch market capitalization from Yahoo Finance."""
        def _fetch_market_cap():
            stock = yf.Ticker(ticker)
            info = stock.info
            return info.get('marketCap') if info else None
        
        try:
            market_cap = self._handle_yfinance_errors(_fetch_market_cap)
            self.mark_healthy()
            return market_cap
        except Exception as e:
            raise e
    
    def supports_feature(self, feature: str) -> bool:
        """Override to specify which features Yahoo Finance supports."""
        supported_features = {
            'prices': True,
            'financial_metrics': True,  # Limited compared to specialized providers
            'company_news': False,      # Not available through yfinance
            'insider_trades': False,    # Not available through yfinance
            'line_items': False,        # Not available in the same format
            'company_facts': True,
            'market_cap': True,
        }
        return supported_features.get(feature, False)