import asyncio
import aiohttp
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime
import logging
from typing import List, Dict
import re
from urllib.parse import urljoin

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class TechCrunchScraper:
    def __init__(self, base_url: str = "https://techcrunch.com/category/startups/"):
        self.base_url = base_url
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
        }
        self.db_path = 'techcrunch_startups.db'
        self.setup_database()

    def setup_database(self):
        """Initialize SQLite database and create articles table."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS articles (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        title TEXT NOT NULL,
                        url TEXT UNIQUE NOT NULL,
                        date_published TEXT,
                        excerpt TEXT,
                        scraped_at TEXT
                    )
                ''')
                conn.commit()
                logger.info("Database initialized successfully")
        except sqlite3.Error as e:
            logger.error(f"Database setup error: {e}")
            raise

    async def fetch_page(self, session: aiohttp.ClientSession, url: str) -> str:
        """Fetch a single page with retry logic."""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                async with session.get(url, headers=self.headers) as response:
                    if response.status == 200:
                        return await response.text()
                    elif response.status == 429:
                        retry_after = int(response.headers.get('Retry-After', 5))
                        logger.warning(f"Rate limited. Retrying after {retry_after} seconds")
                        await asyncio.sleep(retry_after)
                    else:
                        logger.error(f"Failed to fetch {url}: Status {response.status}")
                        return ""
            except aiohttp.ClientError as e:
                logger.error(f"Error fetching {url}: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff
                continue
        return ""

    def parse_articles(self, html: str) -> List[Dict]:
        """Parse article data from HTML."""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            articles = []
            
            # Find article containers (adjust selectors based on TechCrunch's structure)
            article_blocks = soup.select('article.post-block')
            
            for block in article_blocks:
                try:
                    # Extract title and URL
                    title_elem = block.select_one('h2.post-block__title a')
                    title = title_elem.get_text(strip=True) if title_elem else ""
                    url = urljoin(self.base_url, title_elem['href']) if title_elem and title_elem.get('href') else ""
                    
                    # Extract date
                    date_elem = block.select_one('time')
                    date_published = date_elem['datetime'] if date_elem else ""
                    
                    # Extract excerpt
                    excerpt_elem = block.select_one('div.post-block__content')
                    excerpt = excerpt_elem.get_text(strip=True) if excerpt_elem else ""
                    
                    if title and url:
                        articles.append({
                            'title': title,
                            'url': url,
                            'date_published': date_published,
                            'excerpt': excerpt
                        })
                except Exception as e:
                    logger.error(f"Error parsing article block: {e}")
                    continue
            
            logger.info(f"Parsed {len(articles)} articles")
            return articles
        except Exception as e:
            logger.error(f"Error parsing HTML: {e}")
            return []

    async def save_articles(self, articles: List[Dict]):
        """Save articles to SQLite database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                scraped_at = datetime.utcnow().isoformat()
                
                for article in articles:
                    try:
                        cursor.execute('''
                            INSERT OR IGNORE INTO articles (title, url, date_published, excerpt, scraped_at)
                            VALUES (?, ?, ?, ?, ?)
                        ''', (
                            article['title'],
                            article['url'],
                            article['date_published'],
                            article['excerpt'],
                            scraped_at
                        ))
                    except sqlite3.Error as e:
                        logger.error(f"Error saving article {article.get('title', 'unknown')}: {e}")
                        continue
                
                conn.commit()
                logger.info(f"Saved {len(articles)} articles to database")
        except sqlite3.Error as e:
            logger.error(f"Database error: {e}")

    async def scrape(self, max_pages: int = 2):
        """Main scraping function."""
        async with aiohttp.ClientSession() as session:
            for page in range(1, max_pages + 1):
                url = f"{self.base_url}page/{page}/" if page > 1 else self.base_url
                logger.info(f"Scraping page: {url}")
                
                html = await self.fetch_page(session, url)
                if not html:
                    logger.warning(f"Skipping page {url} due to fetch failure")
                    continue
                
                articles = self.parse_articles(html)
                if articles:
                    await self.save_articles(articles)
                
                # Respectful delay to avoid overwhelming the server
                await asyncio.sleep(2)

    def get_stored_articles(self) -> List[Dict]:
        """Retrieve all stored articles from database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT title, url, date_published, excerpt, scraped_at FROM articles')
                rows = cursor.fetchall()
                return [{
                    'title': row[0],
                    'url': row[1],
                    'date_published': row[2],
                    'excerpt': row[3],
                    'scraped_at': row[4]
                } for row in rows]
        except sqlite3.Error as e:
            logger.error(f"Error retrieving articles: {e}")
            return []

async def main():
    scraper = TechCrunchScraper()
    await scraper.scrape(max_pages=2)
    
    # Print stored articles
    articles = scraper.get_stored_articles()
    for article in articles:
        print(f"\nTitle: {article['title']}")
        print(f"URL: {article['url']}")
        print(f"Date: {article['date_published']}")
        print(f"Excerpt: {article['excerpt'][:100]}...")
        print(f"Scraped at: {article['scraped_at']}")

if __name__ == "__main__":
    asyncio.run(main())