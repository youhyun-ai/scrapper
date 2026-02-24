"""Instagram hashtag metrics scraper using instaloader."""

from __future__ import annotations

import random
import time
from typing import Optional

import instaloader
from loguru import logger

from config import INSTAGRAM_HASHTAGS
from database.db import get_connection
from scrapers.base import BaseScraper


class InstagramScraper(BaseScraper):
    """Scrapes Instagram hashtag post counts via instaloader."""

    platform_name: str = "instagram"

    def __init__(self):
        super().__init__()
        self.loader = instaloader.Instaloader(
            download_pictures=False,
            download_videos=False,
            download_video_thumbnails=False,
            download_geotags=False,
            download_comments=False,
            save_metadata=False,
            compress_json=False,
            quiet=True,
        )

    def _fetch_hashtag_count(self, hashtag: str) -> Optional[int]:
        """Fetch the media count for a single hashtag.

        Returns the post count or None if the lookup fails.
        """
        try:
            ht = instaloader.Hashtag.from_name(self.loader.context, hashtag)
            count = ht.mediacount
            logger.info(
                f"[{self.platform_name}] #{hashtag} -> {count:,} posts"
            )
            return count
        except instaloader.exceptions.QueryReturnedNotFoundException:
            logger.warning(
                f"[{self.platform_name}] Hashtag #{hashtag} not found"
            )
            return None
        except instaloader.exceptions.ConnectionException as e:
            logger.warning(
                f"[{self.platform_name}] Rate-limited or connection error "
                f"for #{hashtag}: {e}"
            )
            return None
        except Exception as e:
            logger.error(
                f"[{self.platform_name}] Unexpected error for #{hashtag}: {e}"
            )
            return None

    def save_metrics(self, metrics: list[dict]):
        """Insert scraped hashtag metrics into the instagram_metrics table."""
        if not metrics:
            logger.warning(f"[{self.platform_name}] No metrics to save")
            return
        with get_connection() as conn:
            conn.executemany(
                """INSERT INTO instagram_metrics
                   (hashtag, post_count, snapshot_date)
                   VALUES (?, ?, ?)""",
                [
                    (
                        m["hashtag"],
                        m["post_count"],
                        self.snapshot_date,
                    )
                    for m in metrics
                ],
            )
        logger.info(
            f"[{self.platform_name}] Saved {len(metrics)} hashtag metrics"
        )

    def scrape(self) -> int:
        """Scrape post counts for all configured hashtags.

        Returns the total number of hashtags successfully collected.
        """
        metrics: list[dict] = []
        hashtags = INSTAGRAM_HASHTAGS

        logger.info(
            f"[{self.platform_name}] Scraping {len(hashtags)} hashtags"
        )

        for i, hashtag in enumerate(hashtags):
            # Add a random delay between requests (skip before the first one)
            if i > 0:
                delay = random.uniform(5, 10)
                logger.debug(
                    f"[{self.platform_name}] Sleeping {delay:.1f}s "
                    f"before #{hashtag}"
                )
                time.sleep(delay)

            count = self._fetch_hashtag_count(hashtag)
            if count is not None:
                metrics.append({"hashtag": hashtag, "post_count": count})

        self.save_metrics(metrics)
        logger.info(
            f"[{self.platform_name}] Collected {len(metrics)}/{len(hashtags)} "
            f"hashtags successfully"
        )
        return len(metrics)


if __name__ == "__main__":
    from database.db import init_db

    init_db()
    with InstagramScraper() as scraper:
        total = scraper.run()
        print(f"\nDone. Collected metrics for {total} hashtags.")
