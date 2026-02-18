# article-pipeline
A distributed publisher-consumer pipeline for scraping and storing article content using Redis, MongoDB, and Docker.

## Overview

This system:
1. Publishes article metadata from a JSON file to a Redis queue
2. Consumes tasks from Redis, scrapes article content, and stores it in MongoDB
3. Runs entirely via Docker Compose

## Prerequisites

- Docker Desktop (with Docker Compose)
- At least 4GB RAM allocated to Docker

## Quick Start

1. Clone this repository
2. Navigate to the project directory
3. Run the entire pipeline with a single command:
```bash
docker-compose up --build
```

The system will:
- Start Redis and MongoDB
- Publish articles from `data/articles.json` to the queue
- Start consumer workers to process the articles
- Store scraped content in MongoDB

## Architecture
```
articles.json → Publisher → Redis Queue → Consumer → MongoDB
```

### Services

- **redis**: Message broker (Redis 7)
- **db**: MongoDB 6 database
- **publisher**: Reads JSON file and pushes tasks to Redis
- **consumer**: Scrapes articles and stores in MongoDB (2 replicas)

## Additional Feature: Word Count Analysis

**Innovation**: Each scraped article automatically gets a `word_count` field calculated from the body text. This simple metric provides immediate insights into article length and can be used for:
- Filtering articles by length
- Sorting by content depth
- Analytics on content volume by source/category

This feature demonstrates data enrichment during the pipeline without external dependencies.

## Database Schema

MongoDB collection: `articles`
```javascript
{
  id: String (unique),
  url: String,
  source: String,
  category: String,
  priority: Number,
  title: String,
  body: String,
  error: String (if scraping failed),
  scraped_at: ISODate,
  processed_at: ISODate,
  word_count: Number  // Additional feature
}
```

## Design Choices

### Why MongoDB?
- **Schema flexibility**: Article structures vary by source
- **Easy to scale**: NoSQL handles varying content sizes well
- **Native JSON**: Perfect for our document-based data

### Error Handling
- **Retry strategy**: Automatic retries with exponential backoff for HTTP requests
- **Timeout handling**: 10-second timeout prevents hanging on slow sites
- **Graceful degradation**: Articles with scraping errors are still stored with error messages
- **Empty content detection**: Validates minimum content length before storing

### Idempotency
- **Publisher**: Uses Redis SET to track published articles
- **Consumer**: MongoDB unique index on `id` field prevents duplicates
- **Result**: Running the publisher multiple times won't create duplicates

## Viewing Results

Connect to MongoDB to view scraped articles:
```bash
docker exec -it mongodb mongosh articles_db

# View all articles
db.articles.find().pretty()

# View articles with errors
db.articles.find({error: {$ne: null}})

# View by word count
db.articles.find().sort({word_count: -1})
```

## Stopping the Pipeline
```bash
docker-compose down
```

To remove all data:
```bash
docker-compose down -v
```

## Project Structure
```
article-pipeline/
├── docker-compose.yml       # Orchestration
├── README.md
├── data/
│   └── articles.json       # Input data
├── publisher/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── publisher.py        # Queue publisher
└── consumer/
    ├── Dockerfile
    ├── requirements.txt
    └── consumer.py         # Scraper worker
```

## Troubleshooting

**Issue**: Consumer can't connect to Redis/MongoDB
- **Solution**: Wait 10-15 seconds for health checks to pass

**Issue**: Scraping fails for certain URLs
- **Solution**: Check the `error` field in MongoDB for details

**Issue**: No articles being processed
- **Solution**: Check publisher logs: `docker logs publisher`