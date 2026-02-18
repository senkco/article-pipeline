import json
import redis
import time
import os
import logging
from typing import List, Dict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ArticlePublisher:
    def __init__(self, redis_host: str = 'localhost', redis_port: int = 6379):
        """Initialize Redis connection"""
        self.redis_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            decode_responses=True
        )
        self.queue_name = 'article_queue'
        self.published_set = 'published_articles'  # For idempotency
        
    def load_articles(self, filepath: str) -> List[Dict]:
        """Load articles from JSON file"""
        try:
            with open(filepath, 'r') as f:
                articles = json.load(f)
            logger.info(f"Loaded {len(articles)} articles from {filepath}")
            return articles
        except FileNotFoundError:
            logger.error(f"File not found: {filepath}")
            return []
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in file: {e}")
            return []
    
    def publish_article(self, article: Dict) -> bool:
        """
        Publish a single article to Redis queue
        Uses Redis SET for idempotency - prevents duplicate publishing
        """
        article_id = article.get('id')
        
        # Check if article was already published (idempotency)
        if self.redis_client.sismember(self.published_set, article_id):
            logger.info(f"Article {article_id} already published, skipping")
            return False
        
        try:
            # Convert article to JSON string
            article_json = json.dumps(article)
            
            # Push to Redis list (queue)
            self.redis_client.rpush(self.queue_name, article_json)
            
            # Mark as published
            self.redis_client.sadd(self.published_set, article_id)
            
            logger.info(f"Published article: {article_id} (priority: {article.get('priority')})")
            return True
        except Exception as e:
            logger.error(f"Error publishing article {article_id}: {e}")
            return False
    
    def publish_all(self, articles: List[Dict]):
        """Publish all articles with priority sorting"""
        # Sort by priority (lower number = higher priority)
        sorted_articles = sorted(articles, key=lambda x: x.get('priority', 999))
        
        published_count = 0
        skipped_count = 0
        
        for article in sorted_articles:
            if self.publish_article(article):
                published_count += 1
            else:
                skipped_count += 1
        
        logger.info(f"Publishing complete: {published_count} published, {skipped_count} skipped")
        logger.info(f"Total articles in queue: {self.redis_client.llen(self.queue_name)}")

def main():
    # Get configuration from environment variables
    redis_host = os.getenv('REDIS_HOST', 'localhost')
    redis_port = int(os.getenv('REDIS_PORT', 6379))
    
    # Wait for Redis to be ready
    logger.info("Waiting for Redis to be ready...")
    time.sleep(5)
    
    # Initialize publisher
    publisher = ArticlePublisher(redis_host, redis_port)
    
    # Load and publish articles
    articles = publisher.load_articles('/app/data/articles.json')
    
    if articles:
        publisher.publish_all(articles)
    else:
        logger.error("No articles to publish")

if __name__ == "__main__":
    main()