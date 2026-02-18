import json
import redis
import requests
import time
import os
import logging
from pymongo import MongoClient
from bs4 import BeautifulSoup
from typing import Dict, Optional
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ArticleScraper:
    def __init__(self):
        """Initialize scraper with retry strategy"""
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Set reasonable timeout
        self.timeout = 10
        
        # User agent to avoid being blocked
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
    
    def scrape_article(self, url: str) -> Dict[str, Optional[str]]:
        """
        Scrape article content from URL
        Returns dict with title and body, or error information
        """
        result = {
            'title': None,
            'body': None,
            'error': None,
            'scraped_at': datetime.utcnow().isoformat()
        }
        
        try:
            logger.info(f"Scraping: {url}")
            response = self.session.get(
                url, 
                headers=self.headers, 
                timeout=self.timeout,
                allow_redirects=True
            )
            response.raise_for_status()
            
            # Parse HTML
            soup = BeautifulSoup(response.content, 'lxml')
            
            # Extract title - try multiple common tags
            title = None
            for selector in ['h1', 'title', '[class*="headline"]', '[class*="title"]']:
                title_elem = soup.select_one(selector)
                if title_elem and title_elem.get_text(strip=True):
                    title = title_elem.get_text(strip=True)
                    break
            
            # Extract body - remove script, style tags
            for script in soup(['script', 'style', 'nav', 'footer', 'header']):
                script.decompose()
            
            # Try to find main content
            body = None
            for selector in ['article', 'main', '[class*="content"]', 'body']:
                body_elem = soup.select_one(selector)
                if body_elem:
                    body = body_elem.get_text(separator=' ', strip=True)
                    if len(body) > 100:  # Minimum content threshold
                        break
            
            # Validate results
            if not title:
                result['error'] = "Could not extract title"
                logger.warning(f"No title found for {url}")
            
            if not body or len(body) < 100:
                result['error'] = "Could not extract sufficient body content"
                logger.warning(f"Insufficient body content for {url}")
            
            result['title'] = title
            result['body'] = body[:5000] if body else None  # Limit body size
            
            logger.info(f"Successfully scraped: {url}")
            
        except requests.exceptions.Timeout:
            result['error'] = "Request timeout"
            logger.error(f"Timeout scraping {url}")
        except requests.exceptions.RequestException as e:
            result['error'] = f"Request error: {str(e)}"
            logger.error(f"Request error for {url}: {e}")
        except Exception as e:
            result['error'] = f"Parsing error: {str(e)}"
            logger.error(f"Unexpected error scraping {url}: {e}")
        
        return result

class ArticleConsumer:
    def __init__(self, 
                 redis_host: str = 'localhost', 
                 redis_port: int = 6379,
                 mongo_host: str = 'localhost',
                 mongo_port: int = 27017,
                 mongo_db: str = 'articles_db'):
        """Initialize Redis and MongoDB connections"""
        
        # Redis connection
        self.redis_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            decode_responses=True
        )
        self.queue_name = 'article_queue'
        
        # MongoDB connection
        self.mongo_client = MongoClient(f'mongodb://{mongo_host}:{mongo_port}/')
        self.db = self.mongo_client[mongo_db]
        self.collection = self.db['articles']
        
        # Create unique index for idempotency
        self.collection.create_index('id', unique=True)
        
        # Initialize scraper
        self.scraper = ArticleScraper()
        
        logger.info("Consumer initialized successfully")
    
    def process_task(self, task_json: str) -> bool:
        """Process a single article task"""
        try:
            task = json.loads(task_json)
            article_id = task.get('id')
            url = task.get('url')
            
            logger.info(f"Processing article: {article_id}")
            
            # Scrape content
            scraped_data = self.scraper.scrape_article(url)
            
            # Prepare document for MongoDB
            document = {
                'id': article_id,
                'url': url,
                'source': task.get('source'),
                'category': task.get('category'),
                'priority': task.get('priority'),
                'title': scraped_data.get('title'),
                'body': scraped_data.get('body'),
                'error': scraped_data.get('error'),
                'scraped_at': scraped_data.get('scraped_at'),
                'processed_at': datetime.utcnow().isoformat(),
                'word_count': len(scraped_data.get('body', '').split()) if scraped_data.get('body') else 0
            }
            
            # Store in MongoDB (with idempotency via unique index)
            try:
                self.collection.insert_one(document)
                logger.info(f"Stored article {article_id} in database")
            except Exception as e:
                if 'duplicate key error' in str(e):
                    logger.warning(f"Article {article_id} already exists in database")
                else:
                    raise
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing task: {e}")
            return False
    
    def run(self):
        """Main consumer loop - listen to Redis queue and process tasks"""
        logger.info("Consumer started, waiting for tasks...")
        
        while True:
            try:
                # Blocking pop from Redis list (timeout 1 second)
                task = self.redis_client.blpop(self.queue_name, timeout=1)
                
                if task:
                    _, task_json = task
                    self.process_task(task_json)
                
            except KeyboardInterrupt:
                logger.info("Consumer shutting down...")
                break
            except Exception as e:
                logger.error(f"Error in consumer loop: {e}")
                time.sleep(5)  # Wait before retrying

def main():
    # Get configuration from environment
    redis_host = os.getenv('REDIS_HOST', 'localhost')
    redis_port = int(os.getenv('REDIS_PORT', 6379))
    mongo_host = os.getenv('MONGO_HOST', 'localhost')
    mongo_port = int(os.getenv('MONGO_PORT', 27017))
    mongo_db = os.getenv('MONGO_DB', 'articles_db')
    
    # Wait for services to be ready
    logger.info("Waiting for Redis and MongoDB to be ready...")
    time.sleep(10)
    
    # Initialize and run consumer
    consumer = ArticleConsumer(
        redis_host=redis_host,
        redis_port=redis_port,
        mongo_host=mongo_host,
        mongo_port=mongo_port,
        mongo_db=mongo_db
    )
    
    consumer.run()

if __name__ == "__main__":
    main()