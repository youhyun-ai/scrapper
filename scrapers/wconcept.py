"""W Concept bestseller rankings scraper using the display API."""

from __future__ import annotations

from typing import Optional

from loguru import logger

from config import WCONCEPT
from scrapers.base import BaseScraper


# The display API key is a static key embedded in the W Concept frontend JS bundle.
# It is not a user-specific secret; it is shipped to every visitor's browser.
_DISPLAY_API_KEY = "VWmkUPgs6g2fviPZ5JQFQ3pERP4tIXv/J2jppLqSRBk="
_API_URL = "https://gw-front.wconcept.co.kr/display/api/best/v1/product"


class WConceptScraper(BaseScraper):
    """Scrapes W Concept women's fashion bestsellers via the display API."""

    platform_name = "wconcept"

    def __init__(self):
        super().__init__()
        self.base_url = WCONCEPT["base_url"]

    def _api_post(self, url: str, payload: dict) -> dict:
        """Make an API POST request with the required display headers."""
        headers = {
            **self.get_headers(),
            "Content-Type": "application/json; charset=utf-8",
            "Accept": "application/json",
            "Referer": "https://display.wconcept.co.kr/",
            "Origin": "https://display.wconcept.co.kr",
            "display-api-key": _DISPLAY_API_KEY,
            "devicetype": "PC",
            "membergrade": "8",
        }
        response = self.client.post(url, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        return response.json()

    # ------------------------------------------------------------------
    # Bestseller scraping
    # ------------------------------------------------------------------

    # Date types to fetch — each returns a different ranking with partial overlap,
    # giving us broader product coverage after deduplication.
    _DATE_TYPES = ["daily", "weekly", "realtime"]

    # Subcategory queries to supplement the main ALL fetch.
    # Each entry is (depth1Code, depth1Name, dateType, pageSize).
    # These categories have low overlap with the ALL listing, so they
    # contribute many new unique products.  Codes discovered via the
    # /display/api/best/v1/category endpoint (POST with domain body).
    _SUBCATEGORY_QUERIES: list[tuple[str, str, str, int]] = [
        ("10102", "가방", "daily", 200),
        ("10103", "신발", "daily", 100),
    ]

    def _fetch_bestseller_page(
        self,
        date_type: str = "daily",
        page_size: int = 200,
        depth1_code: str = "ALL",
    ) -> list[dict]:
        """Fetch one page of bestseller items for a given date type and category."""
        payload = {
            "custNo": "0",
            "domain": "WOMEN",
            "genderType": "all",
            "dateType": date_type,
            "ageGroup": "all",
            "depth1Code": depth1_code,
            "depth2Code": "ALL",
            "pageSize": page_size,
            "pageNo": 1,
        }

        data = self._api_post(_API_URL, payload)

        if data.get("result") != "SUCCESS":
            logger.error(
                f"[{self.platform_name}] API returned non-success for "
                f"dateType={date_type}, depth1Code={depth1_code}: "
                f"{data.get('message')}"
            )
            return []

        return data.get("data", {}).get("content", [])

    def _ingest_content(
        self,
        content: list[dict],
        all_items: list[dict],
        seen_urls: set[str],
        rank_counter: list[int],
    ) -> None:
        """Parse *content* entries and append new (unseen) items to *all_items*."""
        for entry in content:
            rank_counter[0] += 1
            try:
                item = self._parse_product(entry, rank_counter[0])
                if item:
                    url = item.get("product_url", "")
                    if url and url not in seen_urls:
                        seen_urls.add(url)
                        all_items.append(item)
            except Exception as e:
                logger.debug(
                    f"[{self.platform_name}] Skipping product at rank "
                    f"{rank_counter[0]}: {e}"
                )

    def scrape_bestsellers(self) -> list[dict]:
        """Fetch women's bestseller product rankings from the W Concept API.

        Fetches rankings across multiple date types (daily, weekly, realtime)
        with depth1Code=ALL, then supplements with subcategory-specific queries
        (bags, shoes) that surface products not present in the ALL listing.
        Deduplicates by product URL to yield ~500-600 unique items.
        """
        logger.info(f"[{self.platform_name}] Fetching bestsellers from API...")

        all_items: list[dict] = []
        seen_urls: set[str] = set()
        # Use a mutable container so _ingest_content can update it.
        rank_counter = [0]

        # --- Phase 1: ALL category across multiple date types ----------------
        for date_type in self._DATE_TYPES:
            logger.info(
                f"[{self.platform_name}] Fetching dateType={date_type}, "
                f"depth1Code=ALL..."
            )
            try:
                content = self._fetch_bestseller_page(date_type=date_type)
            except Exception as e:
                logger.warning(
                    f"[{self.platform_name}] Failed dateType={date_type}: {e}"
                )
                continue

            if not content:
                logger.info(
                    f"[{self.platform_name}] No items for dateType={date_type}"
                )
                continue

            logger.info(
                f"[{self.platform_name}] API returned {len(content)} products "
                f"for dateType={date_type}"
            )
            self._ingest_content(content, all_items, seen_urls, rank_counter)
            self.random_delay()

        # --- Phase 2: subcategory queries for extra coverage -----------------
        for depth1_code, depth1_name, date_type, page_size in self._SUBCATEGORY_QUERIES:
            logger.info(
                f"[{self.platform_name}] Fetching subcategory "
                f"{depth1_name}({depth1_code}), dateType={date_type}, "
                f"pageSize={page_size}..."
            )
            try:
                content = self._fetch_bestseller_page(
                    date_type=date_type,
                    page_size=page_size,
                    depth1_code=depth1_code,
                )
            except Exception as e:
                logger.warning(
                    f"[{self.platform_name}] Failed subcategory "
                    f"{depth1_name}({depth1_code}): {e}"
                )
                continue

            if not content:
                logger.info(
                    f"[{self.platform_name}] No items for subcategory "
                    f"{depth1_name}({depth1_code})"
                )
                continue

            before = len(all_items)
            self._ingest_content(content, all_items, seen_urls, rank_counter)
            added = len(all_items) - before
            logger.info(
                f"[{self.platform_name}] Subcategory {depth1_name}({depth1_code}) "
                f"returned {len(content)} products, {added} new after dedup"
            )
            self.random_delay()

        # Re-number ranks sequentially after dedup
        for i, item in enumerate(all_items, start=1):
            item["rank"] = i

        query_count = len(self._DATE_TYPES) + len(self._SUBCATEGORY_QUERIES)
        logger.info(
            f"[{self.platform_name}] Extracted {len(all_items)} bestseller items "
            f"(after dedup across {query_count} API queries)"
        )
        return all_items

    def _parse_product(self, entry: dict, rank: int) -> Optional[dict]:
        """Parse a single product entry from the API response."""
        item_name = entry.get("itemName", "")
        if not item_name:
            return None

        brand = entry.get("brandNameEn", "") or entry.get("brandNameKr", "")
        final_price = entry.get("finalPrice")
        customer_price = entry.get("customerPrice")
        discount_rate = entry.get("finalDiscountRate")

        # Build product URL (API returns mobile URL; convert to desktop)
        item_cd = entry.get("itemCd", "")
        product_url = f"{self.base_url}/Product/{item_cd}" if item_cd else ""

        image_url = entry.get("productImageUrl", "")

        # Build category from depth names
        category_parts = []
        for key in ("categoryDepthName1", "categoryDepthName2", "categoryDepthName3"):
            val = entry.get(key, "")
            if val:
                category_parts.append(val)
        category = " > ".join(category_parts)

        return {
            "rank": rank,
            "product_name": item_name.strip(),
            "brand": brand.strip(),
            "price": int(final_price) if final_price is not None else None,
            "original_price": int(customer_price) if customer_price is not None else None,
            "discount_pct": int(discount_rate) if discount_rate else None,
            "category": category,
            "product_url": product_url,
            "image_url": image_url,
        }

    # ------------------------------------------------------------------
    # Main scrape method
    # ------------------------------------------------------------------

    def scrape(self) -> int:
        """Run the full W Concept scrape: bestsellers."""
        total = 0

        bestsellers = self.scrape_bestsellers()
        if bestsellers:
            self.save_bestsellers(bestsellers)
            total += len(bestsellers)
        else:
            logger.warning(f"[{self.platform_name}] No bestseller data collected")

        return total


if __name__ == "__main__":
    from database.db import init_db

    init_db()

    with WConceptScraper() as scraper:
        result = scraper.run()
        print(f"W Concept scrape complete: {result} items collected")
