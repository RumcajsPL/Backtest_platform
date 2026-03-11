"""
eToro API client with robust error handling and retry logic.
"""
import requests
import uuid
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from broker_support.config.settings import settings


class EToroClient:
    """Production-grade API client for eToro."""
    
    def __init__(self):
        self.api_key = settings.etoro_api_key
        self.user_key = settings.etoro_user_key  # Add this to settings.py
        self.base_url = settings.etoro_base_url.rstrip('/')
        self.session = requests.Session()
        
    def _get_headers(self) -> Dict[str, str]:
        """Generate headers for eToro API requests."""
        return {
            'x-api-key': self.api_key,
            'x-user-key': self.user_key,
            'x-request-id': str(uuid.uuid4()),  # Unique ID for each request
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'User-Agent': 'BrokerSupport/0.1.0'
        }
        
    def _make_request(self, method: str, endpoint: str, **kwargs) -> Dict[str, Any]:
        """Make HTTP request with error handling."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        headers = self._get_headers()
        
        try:
            logger.debug(f"Making {method} request to {url}")
            logger.debug(f"Headers: { {k: v[:10] + '...' if k in ['x-api-key', 'x-user-key'] else v for k, v in headers.items()} }")
            
            response = self.session.request(
                method, url, 
                headers=headers, 
                timeout=settings.timeout_seconds, 
                **kwargs
            )
            
            # Log response info for debugging
            logger.debug(f"Response status: {response.status_code}")
            logger.debug(f"Response headers: {dict(response.headers)}")
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response status: {e.response.status_code}")
                logger.error(f"Response body: {e.response.text[:500]}")
            raise
    
    @retry(
        stop=stop_after_attempt(settings.max_retries),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def test_connection(self) -> bool:
        """
        Test API connectivity using watchlists endpoint (public).
        """
        logger.info("Testing eToro API connection...")
        
        try:
            # Try the watchlists endpoint as shown in docs
            result = self._make_request('GET', 'api/v1/watchlists')
            logger.success(f"Connection successful! Watchlists: {result}")
            return True
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            return False
    
    @retry(
        stop=stop_after_attempt(settings.max_retries),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def fetch_closed_trades(self, start_date: datetime, end_date: Optional[datetime] = None) -> List[Dict]:
        """
        Fetch closed trades within date range.
        
        Args:
            start_date: Start date for trade history
            end_date: End date (defaults to now)
            
        Returns:
            List of closed trades
        """
        if end_date is None:
            end_date = datetime.now()
            
        logger.info(f"Fetching closed trades from {start_date} to {end_date}")
        
        # Format dates as required by API (adjust format based on docs)
        params = {
            'from': start_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
            'to': end_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
            'status': 'CLOSED'
        }
        
        # Try different possible endpoints for trade history
        endpoints = [
            'api/v1/portfolio/history',
            'api/v1/trades/history',
            'api/v1/transactions'
        ]
        
        for endpoint in endpoints:
            try:
                result = self._make_request('GET', endpoint, params=params)
                trades = result.get('data', []) if isinstance(result, dict) else result
                if trades:
                    logger.info(f"Retrieved {len(trades)} closed trades from {endpoint}")
                    return trades
            except Exception as e:
                logger.debug(f"Endpoint {endpoint} failed: {e}")
                continue
        
        logger.warning("No trades found with any endpoint")
        return []
    
    def get_instruments(self) -> List[Dict]:
        """Fetch available trading instruments."""
        try:
            result = self._make_request('GET', 'api/v1/market/instruments')
            return result.get('data', [])
        except Exception as e:
            logger.error(f"Failed to fetch instruments: {e}")
            return []
    
    def get_portfolio(self) -> Dict[str, Any]:
        """
        Get comprehensive portfolio information including positions and orders.
        
        Returns:
            Dictionary containing portfolio data with positions, orders, mirrors, and credit
        """
        logger.info("Fetching portfolio information...")
        
        try:
            result = self._make_request('GET', 'api/v1/trading/info/demo/portfolio')
            
            # The response structure has clientPortfolio as the main container
            portfolio = result.get('clientPortfolio', {})
            
            positions = portfolio.get('positions', [])
            logger.info(f"Retrieved {len(positions)} open positions")
            
            return portfolio
        except Exception as e:
            logger.error(f"Failed to fetch portfolio: {e}")
            return {}

def fetch_closed_trades(self, from_date: datetime = None, to_date: datetime = None) -> List[Dict]:
    """
    Fetch closed trades history from demo account.
    
    Args:
        from_date: Start date for trade history (default: 7 days ago)
        to_date: End date for trade history (default: now)
    
    Returns:
        List of closed trades with details
    """
    if to_date is None:
        to_date = datetime.now()
    if from_date is None:
        from_date = to_date - timedelta(days=7)
    
    logger.info(f"Fetching closed trades from {from_date} to {to_date}")
    
    # Correct endpoints based on documentation
    endpoints = [
        'api/v1/trading/info/trade/history',                    # Real account pattern
        'api/v1/trading/info/demo/trade/history',               # Demo pattern
        'api/v1/trading/info/portfolio/history',                # Portfolio history
        'api/v1/trading/info/demo/portfolio/history',           # Demo portfolio history
        'api/v1/trading/info/positions/closed',                 # Closed positions
        'api/v1/trading/info/demo/positions/closed',            # Demo closed positions
    ]
    
    # Format dates for API - try different formats
    date_formats = [
        ('fromDate', 'toDate'),  # CamelCase
        ('from_date', 'to_date'), # Snake case
        ('from', 'to'),           # Simple
    ]
    
    for endpoint in endpoints:
        for from_key, to_key in date_formats:
            params = {
                from_key: from_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                to_key: to_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
            }
            
            try:
                logger.debug(f"Trying endpoint: {endpoint} with params: {params}")
                result = self._make_request('GET', endpoint, params=params)
                
                # Handle different response formats
                if isinstance(result, list):
                    trades = result
                elif isinstance(result, dict):
                    # Check common response wrappers
                    trades = (result.get('data') or 
                             result.get('trades') or 
                             result.get('items') or 
                             result.get('positions') or 
                             [])
                else:
                    trades = []
                
                if trades:
                    logger.success(f"Found {len(trades)} trades via {endpoint}")
                    return trades
                else:
                    logger.debug(f"Endpoint {endpoint} returned empty list")
                    
            except Exception as e:
                logger.debug(f"Endpoint {endpoint} with {from_key} failed: {e}")
                continue
    
    logger.warning("No trades found with any endpoint")
    return []