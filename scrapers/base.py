"""Base scraper class with shared logic for retries, logging, and saving."""

from __future__ import annotations

import time
import random
from abc import ABC, abstractmethod
from datetime import date, datetime
from typing import Optional

import httpx
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from config import REQUEST_TIMEOUT, MIN_DELAY, MAX_DELAY, MAX_RETRIES, LOG_DIR
from database.db import get_connection

# Configure logging
LOG_DIR.mkdir(parents=True, exist_ok=True)
logger.add(
    str(LOG_DIR / "scraper.log"),
    rotation="10 MB",
    retention="30 days",
    level="INFO",
    encoding="utf-8",
)

ua = UserAgent()


class BaseScraper(ABC):
    """Base class all platform scrapers inherit from."""

    platform_name: str = "unknown"

    def __init__(self):
        self.client = httpx.Client(
            timeout=REQUEST_TIMEOUT,
            follow_redirects=True,
            http2=True,
        )
        self.snapshot_date = date.today().isoformat()

    def get_headers(self) -> dict:
        return {
            "User-Agent": ua.random,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
        }

    def random_delay(self):
        delay = random.uniform(MIN_DELAY, MAX_DELAY)
        logger.debug(f"[{self.platform_name}] Sleeping {delay:.1f}s")
        time.sleep(delay)

    @retry(
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential(multiplier=1, min=2, max=30),
    )
    def fetch(self, url: str, params: Optional[dict] = None) -> httpx.Response:
        logger.info(f"[{self.platform_name}] Fetching: {url}")
        response = self.client.get(url, headers=self.get_headers(), params=params)
        response.raise_for_status()
        return response

    def parse_html(self, html: str) -> BeautifulSoup:
        return BeautifulSoup(html, "lxml")

    def save_bestsellers(self, items: list[dict]):
        if not items:
            logger.warning(f"[{self.platform_name}] No bestseller items to save")
            return
        with get_connection() as conn:
            conn.executemany(
                """INSERT INTO bestseller_rankings
                   (platform, rank, product_name, brand, price, original_price,
                    discount_pct, category, product_url, image_url, snapshot_date)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                [
                    (
                        self.platform_name,
                        item.get("rank"),
                        item.get("product_name", ""),
                        item.get("brand", ""),
                        item.get("price"),
                        item.get("original_price"),
                        item.get("discount_pct"),
                        item.get("category", ""),
                        item.get("product_url", ""),
                        item.get("image_url", ""),
                        self.snapshot_date,
                    )
                    for item in items
                ],
            )
        logger.info(f"[{self.platform_name}] Saved {len(items)} bestseller items")

    def save_keywords(self, keywords: list[dict]):
        if not keywords:
            logger.warning(f"[{self.platform_name}] No keywords to save")
            return
        with get_connection() as conn:
            conn.executemany(
                """INSERT INTO keyword_rankings
                   (platform, keyword, rank, category, snapshot_date)
                   VALUES (?, ?, ?, ?, ?)""",
                [
                    (
                        self.platform_name,
                        kw.get("keyword", ""),
                        kw.get("rank"),
                        kw.get("category", ""),
                        self.snapshot_date,
                    )
                    for kw in keywords
                ],
            )
        logger.info(f"[{self.platform_name}] Saved {len(keywords)} keywords")

    def log_scrape(self, status: str, items_collected: int = 0,
                   error_message: str = "", duration: float = 0.0):
        with get_connection() as conn:
            conn.execute(
                """INSERT INTO scrape_log
                   (platform, status, items_collected, error_message, duration_seconds)
                   VALUES (?, ?, ?, ?, ?)""",
                (self.platform_name, status, items_collected, error_message, duration),
            )

    def run(self):
        """Execute the full scrape pipeline with logging."""
        start = time.time()
        try:
            logger.info(f"[{self.platform_name}] Starting scrape...")
            items = self.scrape()
            duration = time.time() - start
            total = items if isinstance(items, int) else 0
            self.log_scrape("success", total, duration=duration)
            logger.info(
                f"[{self.platform_name}] Completed in {duration:.1f}s"
            )
            return items
        except Exception as e:
            duration = time.time() - start
            logger.error(f"[{self.platform_name}] Failed: {e}")
            self.log_scrape("failed", error_message=str(e), duration=duration)
            raise

    @abstractmethod
    def scrape(self) -> int:
        """Override in subclass. Return total items collected."""
        ...

    def close(self):
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
