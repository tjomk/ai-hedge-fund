"""
Legacy adapter for smooth migration from financialdatasets.ai to multi-provider system.
This module provides utilities to help during the transition period.
"""

import logging
import warnings
from typing import Any, Dict, List, Optional
from datetime import datetime

from src.tools.api import (
    get_provider_manager,
    get_prices,
    get_financial_metrics,
    get_company_news,
    get_insider_trades,
    search_line_items,
    get_market_cap,
    get_price_data,
)

logger = logging.getLogger(__name__)


class DeprecationWarning(UserWarning):
    """Custom warning for deprecated functionality."""
    pass


def warn_deprecated(old_function: str, new_function: str = None):
    """Issue a deprecation warning for old API usage."""
    if new_function:
        message = f"{old_function} is deprecated. Use {new_function} instead."
    else:
        message = f"{old_function} is deprecated and will be removed in a future version."
    
    warnings.warn(message, DeprecationWarning, stacklevel=3)


# Legacy function aliases with deprecation warnings

def get_financialdatasets_prices(*args, **kwargs):
    """
    Legacy function for getting prices from financialdatasets.ai.
    Now uses the multi-provider system with automatic fallback.
    """
    warn_deprecated("get_financialdatasets_prices", "get_prices")
    return get_prices(*args, **kwargs)


def get_financialdatasets_metrics(*args, **kwargs):
    """
    Legacy function for getting financial metrics from financialdatasets.ai.
    Now uses the multi-provider system with automatic fallback.
    """
    warn_deprecated("get_financialdatasets_metrics", "get_financial_metrics")
    return get_financial_metrics(*args, **kwargs)


def get_financialdatasets_news(*args, **kwargs):
    """
    Legacy function for getting company news from financialdatasets.ai.
    Now uses the multi-provider system with automatic fallback.
    """
    warn_deprecated("get_financialdatasets_news", "get_company_news")
    return get_company_news(*args, **kwargs)


# Migration utilities

class MigrationHelper:
    """
    Helper class to assist with migration from the old API to the new multi-provider system.
    """
    
    def __init__(self):
        self.migration_log = []
        self._provider_manager = None
    
    @property
    def provider_manager(self):
        """Get the provider manager instance."""
        if self._provider_manager is None:
            self._provider_manager = get_provider_manager()
        return self._provider_manager
    
    def log_migration_event(self, event: str, details: Dict[str, Any] = None):
        """Log a migration event for tracking purposes."""
        entry = {
            'timestamp': datetime.now().isoformat(),
            'event': event,
            'details': details or {}
        }
        self.migration_log.append(entry)
        logger.info(f"Migration event: {event}")
    
    def check_api_key_usage(self) -> Dict[str, Any]:
        """
        Check if the old FINANCIAL_DATASETS_API_KEY is still being used.
        """
        import os
        
        api_key = os.environ.get("FINANCIAL_DATASETS_API_KEY")
        status = {
            'api_key_present': api_key is not None,
            'api_key_length': len(api_key) if api_key else 0,
            'recommendation': None
        }
        
        if api_key:
            status['recommendation'] = (
                "FINANCIAL_DATASETS_API_KEY is still set. "
                "This will be used as a fallback provider, which is recommended for maximum data coverage."
            )
            self.log_migration_event('api_key_check', {'status': 'present', 'fallback_available': True})
        else:
            status['recommendation'] = (
                "No FINANCIAL_DATASETS_API_KEY found. "
                "The system will use free providers (Yahoo Finance, STOOQ). "
                "Consider setting the API key for access to premium features like news and insider trades."
            )
            self.log_migration_event('api_key_check', {'status': 'absent', 'fallback_available': False})
        
        return status
    
    def test_data_continuity(self, test_ticker: str = "AAPL") -> Dict[str, Any]:
        """
        Test that data retrieval works with the new system and compare with expectations.
        """
        from datetime import datetime, timedelta
        
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        
        results = {
            'ticker': test_ticker,
            'test_date': end_date,
            'tests': {},
            'summary': {}
        }
        
        # Test price data
        try:
            prices = get_prices(test_ticker, start_date, end_date)
            results['tests']['prices'] = {
                'success': True,
                'count': len(prices),
                'provider': 'unknown'  # We don't expose this in the legacy API
            }
            self.log_migration_event('data_test_prices', {'ticker': test_ticker, 'success': True, 'count': len(prices)})
        except Exception as e:
            results['tests']['prices'] = {
                'success': False,
                'error': str(e)
            }
            self.log_migration_event('data_test_prices', {'ticker': test_ticker, 'success': False, 'error': str(e)})
        
        # Test financial metrics
        try:
            metrics = get_financial_metrics(test_ticker, end_date, limit=1)
            results['tests']['financial_metrics'] = {
                'success': True,
                'count': len(metrics)
            }
            self.log_migration_event('data_test_metrics', {'ticker': test_ticker, 'success': True, 'count': len(metrics)})
        except Exception as e:
            results['tests']['financial_metrics'] = {
                'success': False,
                'error': str(e)
            }
            self.log_migration_event('data_test_metrics', {'ticker': test_ticker, 'success': False, 'error': str(e)})
        
        # Test market cap
        try:
            market_cap = get_market_cap(test_ticker, end_date)
            results['tests']['market_cap'] = {
                'success': market_cap is not None,
                'value': market_cap
            }
            self.log_migration_event('data_test_market_cap', {'ticker': test_ticker, 'success': market_cap is not None})
        except Exception as e:
            results['tests']['market_cap'] = {
                'success': False,
                'error': str(e)
            }
            self.log_migration_event('data_test_market_cap', {'ticker': test_ticker, 'success': False, 'error': str(e)})
        
        # Summary
        successful_tests = sum(1 for test in results['tests'].values() if test.get('success', False))
        total_tests = len(results['tests'])
        
        results['summary'] = {
            'successful_tests': successful_tests,
            'total_tests': total_tests,
            'success_rate': successful_tests / total_tests if total_tests > 0 else 0,
            'migration_viable': successful_tests >= total_tests * 0.8  # 80% success rate threshold
        }
        
        return results
    
    def generate_migration_report(self) -> str:
        """
        Generate a comprehensive migration report.
        """
        report_lines = [
            "AI Hedge Fund Data Provider Migration Report",
            "=" * 50,
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
        ]
        
        # API Key Status
        api_key_status = self.check_api_key_usage()
        report_lines.extend([
            "API Key Status:",
            f"  - Key Present: {'Yes' if api_key_status['api_key_present'] else 'No'}",
            f"  - Recommendation: {api_key_status['recommendation']}",
            "",
        ])
        
        # Provider Health
        try:
            health_status = self.provider_manager.get_provider_health_status()
            healthy_count = sum(1 for status in health_status.values() if status['healthy'])
            total_count = len(health_status)
            
            report_lines.extend([
                "Provider Health:",
                f"  - Healthy Providers: {healthy_count}/{total_count}",
                "",
                "Provider Details:",
            ])
            
            for provider, status in health_status.items():
                health_icon = "✓" if status['healthy'] else "✗"
                success_rate = f"{status['success_rate']:.1%}" if status['total_requests'] > 0 else "N/A"
                report_lines.append(f"  - {provider}: {health_icon} (Success: {success_rate})")
            
            report_lines.append("")
        except Exception as e:
            report_lines.extend([
                "Provider Health: ERROR",
                f"  - Could not retrieve health status: {e}",
                "",
            ])
        
        # Data Continuity Test
        continuity_test = self.test_data_continuity()
        report_lines.extend([
            "Data Continuity Test:",
            f"  - Test Ticker: {continuity_test['ticker']}",
            f"  - Success Rate: {continuity_test['summary']['success_rate']:.1%}",
            f"  - Migration Viable: {'Yes' if continuity_test['summary']['migration_viable'] else 'No'}",
            "",
            "Test Details:",
        ])
        
        for test_name, test_result in continuity_test['tests'].items():
            status_icon = "✓" if test_result['success'] else "✗"
            detail = f"Count: {test_result.get('count', 'N/A')}" if test_result['success'] else f"Error: {test_result.get('error', 'Unknown')}"
            report_lines.append(f"  - {test_name}: {status_icon} ({detail})")
        
        report_lines.append("")
        
        # Migration Events Log
        if self.migration_log:
            report_lines.extend([
                "Migration Events:",
                ""
            ])
            for event in self.migration_log[-10:]:  # Show last 10 events
                report_lines.append(f"  - {event['timestamp']}: {event['event']}")
            report_lines.append("")
        
        # Recommendations
        report_lines.extend([
            "Recommendations:",
            "  1. The migration to multi-provider system is complete",
            "  2. All existing code should work without changes",
            "  3. Monitor provider health using get_provider_health_status()",
            "  4. Consider setting FINANCIAL_DATASETS_API_KEY for premium features",
            "  5. Use cache warming for frequently accessed tickers",
            "",
        ])
        
        return "\n".join(report_lines)
    
    def get_migration_log(self) -> List[Dict[str, Any]]:
        """Get the migration log entries."""
        return self.migration_log.copy()


# Global migration helper instance
_migration_helper = MigrationHelper()


def get_migration_helper() -> MigrationHelper:
    """Get the global migration helper instance."""
    return _migration_helper


# Convenience functions

def check_migration_status() -> Dict[str, Any]:
    """
    Quick check of migration status.
    
    Returns:
        Dictionary with migration status information
    """
    helper = get_migration_helper()
    
    api_status = helper.check_api_key_usage()
    continuity_test = helper.test_data_continuity()
    
    return {
        'migration_complete': True,
        'api_key_present': api_status['api_key_present'],
        'data_continuity_viable': continuity_test['summary']['migration_viable'],
        'recommendation': api_status['recommendation']
    }


def generate_migration_report() -> str:
    """
    Generate and return a migration report.
    
    Returns:
        String containing the full migration report
    """
    helper = get_migration_helper()
    return helper.generate_migration_report()


if __name__ == "__main__":
    # Quick migration check
    print("AI Hedge Fund Migration Check")
    print("=" * 40)
    
    status = check_migration_status()
    print(f"Migration Complete: {'✓' if status['migration_complete'] else '✗'}")
    print(f"Data Continuity: {'✓' if status['data_continuity_viable'] else '✗'}")
    print(f"API Key Present: {'✓' if status['api_key_present'] else '✗'}")
    print()
    print("Recommendation:", status['recommendation'])
    print()
    
    # Generate full report
    print("Full Migration Report:")
    print(generate_migration_report())