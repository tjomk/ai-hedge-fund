import requests
import time
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta

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

logger = logging.getLogger(__name__)


class SECEdgarProvider(DataProvider):
    """
    SEC EDGAR data provider for financial statement line items.
    
    Uses the official SEC API at data.sec.gov which provides free access
    to all public company filings including detailed financial statements
    with XBRL data.
    
    Features:
    - Free API access (no key required)
    - Full financial statement line items
    - Historical data from 10-K/10-Q filings
    - Real-time updates
    """
    
    def __init__(self):
        super().__init__("SEC EDGAR")
        self.base_url = "https://data.sec.gov"
        self.max_retries = 3
        self.timeout = 30
        
        # Request headers required by SEC
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Compatible Research Bot) Contact: research@example.com",
            "Accept": "application/json",
        }
        
        # Map common line items to SEC XBRL concepts
        self.line_item_mappings = {
            # Income Statement
            "revenue": ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax", "SalesRevenueNet"],
            "total_revenue": ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax"],
            "cost_of_revenue": ["CostOfRevenue", "CostOfGoodsAndServicesSold"],
            "gross_profit": ["GrossProfit"],
            "operating_income": ["OperatingIncomeLoss", "IncomeLossFromContinuingOperationsBeforeIncomeTaxes"],
            "net_income": ["NetIncomeLoss", "ProfitLoss"],
            "earnings_per_share": ["EarningsPerShareBasic", "EarningsPerShareDiluted"],
            
            # Balance Sheet  
            "total_assets": ["Assets"],
            "current_assets": ["AssetsCurrent"],
            "cash": ["CashAndCashEquivalentsAtCarryingValue", "Cash"],
            "total_liabilities": ["Liabilities"],
            "current_liabilities": ["LiabilitiesCurrent"],
            "stockholder_equity": ["StockholdersEquity"],
            "total_debt": ["DebtCurrent", "LongTermDebt"],
            
            # Cash Flow
            "operating_cash_flow": ["NetCashProvidedByUsedInOperatingActivities"],
            "investing_cash_flow": ["NetCashProvidedByUsedInInvestingActivities"],
            "financing_cash_flow": ["NetCashProvidedByUsedInFinancingActivities"],
        }
        
        # Fallback ticker to CIK mapping for common stocks
        # This helps when the SEC API is blocking requests to company_tickers.json
        self.ticker_to_cik_fallback = {
            "AAPL": "0000320193",
            "MSFT": "0000789019", 
            "GOOGL": "0001652044",
            "AMZN": "0001018724",
            "TSLA": "0001318605",
            "META": "0001326801",
            "NVDA": "0001045810",
            "BRK-A": "0001067983",
            "JPM": "0000019617",
            "JNJ": "0000200406",
            "V": "0001403161",
            "WMT": "0000104169",
            "MA": "0001141391",
            "HD": "0000354950",
            "PG": "0000080424",
            "UNH": "0000731766",
            "BAC": "0000070858",
            "XOM": "0000034088",
            "CVX": "0000093410",
            "ABBV": "0001551152",
        }
    
    def _make_request(self, url: str) -> requests.Response:
        """Make a request to the SEC API with proper rate limiting and error handling."""
        last_exception = None
        
        for attempt in range(self.max_retries):
            try:
                # SEC recommends no more than 10 requests per second
                time.sleep(0.1)  
                
                response = requests.get(url, headers=self.headers, timeout=self.timeout)
                
                if response.status_code == 200:
                    return response
                elif response.status_code == 404:
                    raise DataProviderNotFoundError(f"Data not found: {url}")
                elif response.status_code == 429:
                    wait_time = 2 ** attempt
                    logger.warning(f"SEC API rate limit hit, waiting {wait_time} seconds")
                    time.sleep(wait_time)
                    last_exception = DataProviderRateLimitError("SEC API rate limit exceeded")
                    continue
                else:
                    raise DataProviderError(f"SEC API returned status {response.status_code}: {response.text}")
                    
            except requests.exceptions.Timeout:
                last_exception = DataProviderTimeoutError(f"Request to SEC API timed out after {self.timeout} seconds")
            except requests.exceptions.RequestException as e:
                last_exception = DataProviderError(f"Request to SEC API failed: {e}")
            
            if attempt < self.max_retries - 1:
                wait_time = 2 ** attempt
                logger.warning(f"Request failed (attempt {attempt + 1}), retrying in {wait_time} seconds: {last_exception}")
                time.sleep(wait_time)
        
        raise last_exception or DataProviderError("All requests to SEC API failed")
    
    def _get_cik_from_ticker(self, ticker: str) -> Optional[str]:
        """Convert ticker symbol to CIK using fallback mapping and SEC API."""
        ticker_upper = ticker.upper()
        
        # First, try the fallback mapping for common tickers
        if ticker_upper in self.ticker_to_cik_fallback:
            cik = self.ticker_to_cik_fallback[ticker_upper]
            logger.debug(f"Found CIK {cik} for ticker {ticker} via fallback mapping")
            return cik
        
        try:
            # Try the SEC API as a fallback
            url = f"{self.base_url}/files/company_tickers.json"
            response = self._make_request(url)
            data = response.json()
            
            # Search for the ticker in the company tickers data
            for entry in data.values():
                if entry.get("ticker", "").upper() == ticker_upper:
                    cik = str(entry.get("cik_str", "")).zfill(10)  # Pad with zeros
                    logger.debug(f"Found CIK {cik} for ticker {ticker} via SEC API")
                    return cik
                    
        except Exception as e:
            logger.debug(f"SEC API lookup failed for ticker {ticker}: {e}")
        
        logger.warning(f"Could not find CIK for ticker {ticker} in fallback mapping or SEC API")
        return None
    
    def _get_company_facts_data(self, cik: str) -> Optional[Dict[str, Any]]:
        """Get the full company facts data from SEC API."""
        try:
            url = f"{self.base_url}/api/xbrl/companyfacts/CIK{cik}.json"
            response = self._make_request(url)
            return response.json()
        except DataProviderNotFoundError:
            logger.debug(f"No company facts found for CIK {cik}")
            return None
        except Exception as e:
            logger.error(f"Failed to get company facts for CIK {cik}: {e}")
            return None
    
    def _extract_line_items_from_facts(
        self, 
        company_data: Dict[str, Any], 
        ticker: str,
        line_items: List[str],
        end_date: str,
        period: str = "ttm",
        limit: int = 10
    ) -> List[LineItem]:
        """Extract specific line items from SEC company facts data."""
        results = []
        
        # Get US-GAAP facts
        us_gaap = company_data.get("facts", {}).get("us-gaap", {})
        if not us_gaap:
            logger.warning(f"No US-GAAP data found for ticker {ticker}")
            return results
        
        # Convert end_date to datetime for comparison
        try:
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError:
            logger.error(f"Invalid end_date format: {end_date}")
            return results
        
        for requested_item in line_items:
            # Get possible SEC concepts for this line item
            possible_concepts = self.line_item_mappings.get(requested_item.lower(), [requested_item])
            
            for concept in possible_concepts:
                if concept in us_gaap:
                    concept_data = us_gaap[concept]
                    units = concept_data.get("units", {})
                    
                    # Prefer USD values, fall back to shares or other units
                    unit_priority = ["USD", "USD-per-shares", "shares", "pure"]
                    selected_unit = None
                    
                    for unit in unit_priority:
                        if unit in units:
                            selected_unit = unit
                            break
                    
                    if not selected_unit:
                        # Take the first available unit
                        selected_unit = next(iter(units.keys())) if units else None
                    
                    if selected_unit and selected_unit in units:
                        # Get the most recent filings within our date range
                        filings = units[selected_unit]
                        valid_filings = []
                        
                        for filing in filings:
                            filing_end = filing.get("end")
                            if filing_end:
                                try:
                                    filing_end_dt = datetime.strptime(filing_end, "%Y-%m-%d")
                                    if filing_end_dt <= end_dt:
                                        # Filter by period type if specified
                                        form = filing.get("form", "").upper()
                                        if period == "annual" and "10-K" not in form:
                                            continue
                                        elif period == "quarterly" and "10-Q" not in form:
                                            continue
                                        
                                        valid_filings.append(filing)
                                except ValueError:
                                    continue
                        
                        # Sort by end date descending and take the most recent ones
                        valid_filings.sort(key=lambda x: x.get("end", ""), reverse=True)
                        valid_filings = valid_filings[:limit]
                        
                        for filing in valid_filings:
                            line_item = LineItem(
                                ticker=ticker,
                                report_period=filing.get("end", ""),
                                period=filing.get("form", "").replace("-", "").lower(),
                                currency=selected_unit if selected_unit == "USD" else "USD"
                            )
                            
                            # Add the specific line item value with dynamic field
                            setattr(line_item, requested_item.lower().replace(" ", "_"), filing.get("val"))
                            setattr(line_item, "concept", concept)
                            setattr(line_item, "form", filing.get("form"))
                            setattr(line_item, "filed", filing.get("filed"))
                            setattr(line_item, "frame", filing.get("frame"))
                            
                            results.append(line_item)
                        
                        # Found data for this concept, move to next requested item
                        break
        
        return results
    
    def supports_feature(self, feature: str) -> bool:
        """SEC EDGAR primarily supports line_items and company_facts."""
        supported_features = {
            'prices': False,  # SEC doesn't provide price data
            'financial_metrics': False,  # SEC provides raw data, not calculated metrics
            'company_news': False,  # SEC doesn't provide news
            'insider_trades': True,  # SEC has insider trading data (future enhancement)
            'line_items': True,  # Primary strength - financial statement line items
            'company_facts': True,  # Basic company information
            'market_cap': False,  # SEC doesn't provide market cap calculations
        }
        return supported_features.get(feature, False)
    
    # Required abstract methods - most will raise NotImplementedError since SEC focuses on filings
    
    def get_prices(self, ticker: str, start_date: str, end_date: str) -> List[Price]:
        """SEC doesn't provide price data."""
        raise DataProviderNotFoundError("SEC EDGAR does not provide price data")
    
    def get_financial_metrics(self, ticker: str, end_date: str, period: str = "ttm", limit: int = 10) -> List[FinancialMetrics]:
        """SEC doesn't provide calculated financial metrics."""
        raise DataProviderNotFoundError("SEC EDGAR does not provide calculated financial metrics")
    
    def get_company_news(self, ticker: str, end_date: str, start_date: Optional[str] = None, limit: int = 1000) -> List[CompanyNews]:
        """SEC doesn't provide news data."""
        raise DataProviderNotFoundError("SEC EDGAR does not provide news data")
    
    def get_insider_trades(self, ticker: str, end_date: str, start_date: Optional[str] = None, limit: int = 1000) -> List[InsiderTrade]:
        """SEC has insider trading data but not implemented yet."""
        raise DataProviderNotFoundError("SEC EDGAR insider trades not implemented yet")
    
    def search_line_items(
        self,
        ticker: str,
        line_items: List[str],
        end_date: str,
        period: str = "ttm",
        limit: int = 10,
    ) -> List[LineItem]:
        """
        Search for specific line items in SEC filings.
        
        This is the primary function of the SEC EDGAR provider.
        """
        try:
            # Get CIK for the ticker
            cik = self._get_cik_from_ticker(ticker)
            if not cik:
                raise DataProviderNotFoundError(f"Could not find SEC CIK for ticker {ticker}")
            
            # Get company facts data
            company_data = self._get_company_facts_data(cik)
            if not company_data:
                raise DataProviderNotFoundError(f"No SEC filing data found for ticker {ticker}")
            
            # Extract line items
            line_item_results = self._extract_line_items_from_facts(
                company_data, ticker, line_items, end_date, period, limit
            )
            
            if not line_item_results:
                logger.warning(f"No line items found for {ticker} with items: {line_items}")
            
            return line_item_results
            
        except Exception as e:
            if isinstance(e, (DataProviderNotFoundError, DataProviderError)):
                raise
            logger.error(f"Unexpected error searching line items for {ticker}: {e}")
            raise DataProviderError(f"Failed to search line items: {e}")
    
    def get_company_facts(self, ticker: str) -> Optional[CompanyFacts]:
        """Get basic company information from SEC data."""
        try:
            # Get CIK for the ticker
            cik = self._get_cik_from_ticker(ticker)
            if not cik:
                return None
            
            # Get company facts data
            company_data = self._get_company_facts_data(cik)
            if not company_data:
                return None
            
            # Extract basic company info
            entity_name = company_data.get("entityName", "")
            
            return CompanyFacts(
                ticker=ticker,
                name=entity_name,
                cik=cik,
                sec_filings_url=f"https://www.sec.gov/edgar/browse/?CIK={cik}",
            )
            
        except Exception as e:
            logger.error(f"Failed to get company facts for {ticker}: {e}")
            return None
    
    def get_market_cap(self, ticker: str, end_date: str) -> Optional[float]:
        """SEC doesn't provide market cap calculations."""
        raise DataProviderNotFoundError("SEC EDGAR does not provide market cap data")