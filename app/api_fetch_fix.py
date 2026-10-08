"""
Fixed API Fetch Implementation for Product Hunter
This file provides a working API fetch mechanism with proper error handling and logging.
"""

import os
import json
import logging
from typing import Dict, List, Any, Optional
import requests
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('api_fetch')


class APIFetchError(Exception):
    """Custom exception for API fetch errors"""
    pass


class ProductHunterAPI:
    """Main API client for Product Hunter"""
    
    def __init__(self):
        # Load environment variables
        self.rapidapi_key = os.getenv('RAPIDAPI_KEY')
        self.rapidapi_host = os.getenv('RAPIDAPI_HOST', 'amazon-data-scraper-api2.p.rapidapi.com')
        self.base_url = f'https://{self.rapidapi_host}'
        
        # Headers for all requests
        self.headers = {
            'X-RapidAPI-Key': self.rapidapi_key,
            'X-RapidAPI-Host': self.rapidapi_host
        }
        
        # Validate configuration
        if not self.rapidapi_key:
            logger.error("RAPIDAPI_KEY environment variable is not set!")
            raise APIFetchError("Missing RAPIDAPI_KEY")
        
        logger.info(f"API client initialized with host: {self.rapidapi_host}")
    
    def fetch_products(self, search_query: str, max_results: int = 10) -> List[Dict[str, Any]]:
        """
        Fetch products from API with proper error handling
        
        Args:
            search_query: Product search term
            max_results: Maximum number of results to return
            
        Returns:
            List of product dictionaries
        """
        logger.info(f"Fetching products for query: '{search_query}'")
        
        try:
            # Example endpoint - adjust based on your actual API
            endpoint = f"{self.base_url}/search"
            params = {
                'searchQuery': search_query,
                'maxResults': max_results
            }
            
            logger.info(f"Making request to: {endpoint}")
            logger.debug(f"Headers: {{'X-RapidAPI-Key': '{self.rapidapi_key[:8]}...', 'X-RapidAPI-Host': '{self.rapidapi_host}'}}")
            
            response = requests.get(
                endpoint,
                headers=self.headers,
                params=params,
                timeout=30
            )
            
            # Log response status
            logger.info(f"Response status: {response.status_code}")
            
            # Check for errors
            if response.status_code != 200:
                error_msg = f"API returned status {response.status_code}: {response.text[:200]}"
                logger.error(error_msg)
                raise APIFetchError(error_msg)
            
            # Parse response
            try:
                data = response.json()
                logger.info(f"Successfully parsed JSON response")
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON: {e}")
                logger.error(f"Response text: {response.text[:500]}")
                raise APIFetchError(f"Invalid JSON response: {e}")
            
            # Extract products from response
            products = self._parse_products(data)
            logger.info(f"Extracted {len(products)} products")
            
            return products
            
        except requests.exceptions.Timeout:
            logger.error("API request timed out after 30 seconds")
            raise APIFetchError("Request timeout")
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error: {e}")
            raise APIFetchError(f"Connection failed: {e}")
        except Exception as e:
            logger.error(f"Unexpected error: {type(e).__name__}: {e}")
            raise APIFetchError(f"Fetch failed: {e}")
    
    def _parse_products(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Parse API response into product list
        Adjust this method based on your actual API response structure
        """
        products = []
        
        try:
            # Common response structures - adjust based on your API
            if 'products' in data:
                raw_products = data['products']
            elif 'results' in data:
                raw_products = data['results']
            elif 'items' in data:
                raw_products = data['items']
            elif 'data' in data:
                raw_products = data['data']
            else:
                # If response is already a list
                raw_products = data if isinstance(data, list) else []
            
            for item in raw_products:
                try:
                    product = {
                        'title': item.get('title', item.get('name', 'Unknown')),
                        'price': item.get('price', item.get('current_price', 0)),
                        'rating': item.get('rating', item.get('average_rating', 0)),
                        'reviews': item.get('reviews', item.get('review_count', 0)),
                        'url': item.get('url', item.get('link', '')),
                        'image': item.get('image', item.get('thumbnail', '')),
                        'fetched_at': datetime.utcnow().isoformat()
                    }
                    products.append(product)
                except Exception as e:
                    logger.warning(f"Failed to parse product item: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Failed to parse products: {e}")
            raise
        
        return products
    
    def test_connection(self) -> bool:
        """Test API connection with a simple request"""
        logger.info("Testing API connection...")
        try:
            # Simple test endpoint
            response = requests.get(
                f"{self.base_url}/test",
                headers=self.headers,
                timeout=10
            )
            logger.info(f"Test connection status: {response.status_code}")
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Test connection failed: {e}")
            return False


def main():
    """Test the API fetch functionality"""
    print("=" * 60)
    print("Product Hunter API Fetch Test")
    print("=" * 60)
    
    # Check environment variables
    print("\n1. Checking environment variables...")
    api_key = os.getenv('RAPIDAPI_KEY')
    api_host = os.getenv('RAPIDAPI_HOST', 'amazon-data-scraper-api2.p.rapidapi.com')
    
    print(f"   RAPIDAPI_KEY: {'✓ Set' if api_key else '✗ NOT SET'}")
    print(f"   RAPIDAPI_HOST: {api_host}")
    
    if not api_key:
        print("\nERROR: RAPIDAPI_KEY is not set!")
        print("Please set it with: export RAPIDAPI_KEY='your-key-here'")
        return
    
    # Initialize API client
    print("\n2. Initializing API client...")
    try:
        client = ProductHunterAPI()
        print("   ✓ API client initialized")
    except APIFetchError as e:
        print(f"   ✗ Failed: {e}")
        return
    
    # Test connection
    print("\n3. Testing API connection...")
    if client.test_connection():
        print("   ✓ Connection successful")
    else:
        print("   ✗ Connection failed")
        return
    
    # Fetch products
    print("\n4. Fetching products...")
    test_query = "wireless headphones"
    print(f"   Search query: '{test_query}'")
    
    try:
        products = client.fetch_products(test_query, max_results=5)
        print(f"   ✓ Fetched {len(products)} products")
        
        # Display results
        print("\n5. Sample results:")
        for i, product in enumerate(products[:3], 1):
            print(f"   {i}. {product['title']}")
            print(f"      Price: ${product['price']}")
            print(f"      Rating: {product['rating']} ({product['reviews']} reviews)")
            
    except APIFetchError as e:
        print(f"   ✗ Fetch failed: {e}")
    
    print("\n" + "=" * 60)
    print("Test complete!")
    print("=" * 60)


if __name__ == '__main__':
    main()
