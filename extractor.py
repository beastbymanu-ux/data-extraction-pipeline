"""
Data Extractor — Multi-source web scraper.

Extracts structured data from public sources with:
- Polite delays between requests
- User-Agent rotation
- Retry logic with exponential backoff
- Structured JSON output
"""
import requests
import random
import time
import logging
import hashlib
from datetime import datetime
from bs4 import BeautifulSoup
from config import SCRAPER_USER_AGENTS, REQUEST_DELAY_MIN, REQUEST_DELAY_MAX, MAX_RETRIES

logger = logging.getLogger(__name__)


class DataExtractor:
    """
    Base extractor with anti-detection and retry logic.
    Subclass this for specific data sources.
    """

    def __init__(self, source_name: str):
        self.source_name = source_name
        self.session = requests.Session()
        self.total_extracted = 0
        self.total_errors = 0

    def _get_headers(self) -> dict:
        return {
            "User-Agent": random.choice(SCRAPER_USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
        }

    def _polite_delay(self):
        delay = random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX)
        time.sleep(delay)

    def _fetch_page(self, url: str) -> str | None:
        """Fetch a page with retry logic."""
        for attempt in range(MAX_RETRIES):
            try:
                self._polite_delay()
                response = self.session.get(
                    url,
                    headers=self._get_headers(),
                    timeout=15,
                )
                response.raise_for_status()
                return response.text

            except requests.RequestException as e:
                wait = 2 ** attempt + random.uniform(0, 1)
                logger.warning(
                    f"[{self.source_name}] Attempt {attempt + 1}/{MAX_RETRIES} "
                    f"failed for {url}: {e}. Retrying in {wait:.1f}s"
                )
                time.sleep(wait)

        logger.error(f"[{self.source_name}] All retries failed for {url}")
        self.total_errors += 1
        return None

    def _make_id(self, text: str) -> str:
        return hashlib.md5(text.encode()).hexdigest()

    def extract(self) -> list:
        """Override in subclass. Returns list of raw data dicts."""
        raise NotImplementedError


class HackerNewsExtractor(DataExtractor):
    """
    Extracts job postings from Hacker News "Who is Hiring" threads.
    Public data, no authentication required.
    """

    def __init__(self):
        super().__init__("hackernews_jobs")
        self.base_url = "https://hacker-news.firebaseio.com/v0"

    def _get_whoishiring_stories(self) -> list:
        """Find recent 'Who is Hiring' story IDs via HN API."""
        # Search for the HN user "whoishiring"
        url = f"{self.base_url}/user/whoishiring.json"
        try:
            response = self.session.get(url, timeout=10)
            data = response.json()
            # Return the most recent submitted stories
            return data.get("submitted", [])[:5]
        except Exception as e:
            logger.error(f"Failed to get whoishiring stories: {e}")
            return []

    def _get_story_comments(self, story_id: int) -> list:
        """Get all top-level comments (job postings) from a story."""
        url = f"{self.base_url}/item/{story_id}.json"
        try:
            response = self.session.get(url, timeout=10)
            story = response.json()
            if not story:
                return []
            return story.get("kids", [])[:100]  # Top 100 comments
        except Exception as e:
            logger.error(f"Failed to get story {story_id}: {e}")
            return []

    def _parse_comment(self, comment_id: int) -> dict | None:
        """Parse a single HN comment into a job posting."""
        url = f"{self.base_url}/item/{comment_id}.json"
        try:
            self._polite_delay()
            response = self.session.get(url, timeout=10)
            comment = response.json()

            if not comment or comment.get("deleted") or comment.get("dead"):
                return None

            text = comment.get("text", "")
            if not text or len(text) < 50:
                return None

            # Parse the HTML text
            soup = BeautifulSoup(text, "html.parser")
            clean_text = soup.get_text(separator=" ", strip=True)

            # Extract company name (usually first line)
            first_line = clean_text.split("|")[0].strip() if "|" in clean_text else clean_text[:100]

            # Extract location
            location = "Remote"
            location_keywords = ["remote", "onsite", "hybrid", "office"]
            text_lower = clean_text.lower()
            for kw in location_keywords:
                if kw in text_lower:
                    location = kw.capitalize()
                    break

            return {
                "id": self._make_id(str(comment_id)),
                "source": self.source_name,
                "source_url": f"https://news.ycombinator.com/item?id={comment_id}",
                "company": first_line[:200],
                "description": clean_text[:2000],
                "location": location,
                "posted_at": datetime.fromtimestamp(comment.get("time", 0)).isoformat(),
                "raw_html": text,
                "extracted_at": datetime.utcnow().isoformat(),
            }

        except Exception as e:
            logger.error(f"Failed to parse comment {comment_id}: {e}")
            return None

    def extract(self) -> list:
        """Extract job postings from recent Who is Hiring threads."""
        logger.info(f"[{self.source_name}] Starting extraction...")
        results = []

        story_ids = self._get_whoishiring_stories()
        if not story_ids:
            logger.warning("No Who is Hiring stories found")
            return results

        # Process first story (most recent)
        story_id = story_ids[0]
        logger.info(f"[{self.source_name}] Processing story {story_id}")

        comment_ids = self._get_story_comments(story_id)
        logger.info(f"[{self.source_name}] Found {len(comment_ids)} comments to process")

        for comment_id in comment_ids:
            posting = self._parse_comment(comment_id)
            if posting:
                results.append(posting)
                self.total_extracted += 1

        logger.info(
            f"[{self.source_name}] Extraction complete: "
            f"{self.total_extracted} records, {self.total_errors} errors"
        )
        return results


class GitHubJobsExtractor(DataExtractor):
    """
    Extracts trending repositories data from GitHub API.
    Public data, no authentication required for basic endpoints.
    """

    def __init__(self):
        super().__init__("github_trending")
        self.api_url = "https://api.github.com"

    def extract(self) -> list:
        """Extract trending Python/AI repositories."""
        logger.info(f"[{self.source_name}] Starting extraction...")
        results = []

        queries = [
            "language:python stars:>100 topic:llm",
            "language:python stars:>100 topic:ai-agent",
            "language:python stars:>50 topic:rag",
        ]

        for query in queries:
            self._polite_delay()
            url = f"{self.api_url}/search/repositories"
            params = {
                "q": query,
                "sort": "updated",
                "order": "desc",
                "per_page": 30,
            }

            try:
                response = self.session.get(
                    url,
                    params=params,
                    headers=self._get_headers(),
                    timeout=15,
                )
                response.raise_for_status()
                data = response.json()

                for repo in data.get("items", []):
                    record = {
                        "id": self._make_id(repo["full_name"]),
                        "source": self.source_name,
                        "source_url": repo["html_url"],
                        "title": repo["full_name"],
                        "company": repo["owner"]["login"],
                        "description": (repo.get("description") or "")[:1000],
                        "category": "AI/ML Repository",
                        "tags": repo.get("topics", []),
                        "stars": repo["stargazers_count"],
                        "language": repo.get("language"),
                        "updated_at": repo.get("updated_at"),
                        "extracted_at": datetime.utcnow().isoformat(),
                    }
                    results.append(record)
                    self.total_extracted += 1

            except Exception as e:
                logger.error(f"[{self.source_name}] Failed query '{query}': {e}")
                self.total_errors += 1

        logger.info(
            f"[{self.source_name}] Extraction complete: "
            f"{self.total_extracted} records, {self.total_errors} errors"
        )
        return results
