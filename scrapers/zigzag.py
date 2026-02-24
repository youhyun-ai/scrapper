"""Zigzag bestseller ranking scraper using the GraphQL API.

Best-effort scraper: Zigzag is primarily a mobile app. The web version at
zigzag.kr is a Next.js app that loads product data via a GraphQL endpoint
(api.zigzag.kr). The home page serves as the ranking/bestseller feed --
products are ordered by Zigzag's internal ranking algorithm which combines
sales volume, reviews, and personalisation signals.

We call the GetPageInfoForWeb GraphQL query directly with httpx (no
Playwright needed) and paginate through the results.
"""

from __future__ import annotations

import json
import math
from typing import Optional

from loguru import logger

from config import ZIGZAG, CATEGORY_MAP
from scrapers.base import BaseScraper


# ---------------------------------------------------------------------------
# GraphQL query (trimmed to the fields we actually need)
# ---------------------------------------------------------------------------

_GQL_QUERY = """\
fragment PageInfoPart on UxPageInfo {
  page_name has_next end_cursor type
  ui_item_list { ...UxComponentPart }
}
fragment UxComponentPart on UxComponent {
  __typename
  ... on UxGoodsCardItem { ...UxGoodsCardItemPart }
}
fragment UxGoodsCardItemPart on UxGoodsCardItem {
  position type image_url webp_image_url product_url shop_name title
  price discount_rate free_shipping final_price max_price
  catalog_product_id shop_id sales_status is_zonly is_brand
  display_review_count review_score ranking log
  managed_category_list { id category_id value key depth }
}
query GetPageInfoForWeb(
  $page_id: String
  $after: String
  $external_page_id: String
) {
  page_info(
    page_id: $page_id
    after: $after
    external_page_id: $external_page_id
  ) { ...PageInfoPart }
}"""

_GRAPHQL_URL = "https://api.zigzag.kr/api/2/graphql/GetPageInfoForWeb"

# How many pages to fetch (36 items each). 3 pages = ~108 items.
_MAX_PAGES = 3


class ZigzagScraper(BaseScraper):
    """Scrapes Zigzag home-feed bestsellers via GraphQL API."""

    platform_name = "zigzag"

    def __init__(self) -> None:
        super().__init__()
        self.base_url: str = ZIGZAG["base_url"]

    # ------------------------------------------------------------------
    # API helpers
    # ------------------------------------------------------------------

    def _gql_headers(self) -> dict:
        """Headers for the GraphQL endpoint."""
        base = self.get_headers()
        base.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Origin": "https://zigzag.kr",
            "Referer": "https://zigzag.kr/",
        })
        return base

    def _fetch_page(self, cursor: Optional[str] = None) -> dict:
        """Fetch one page of the home feed via GraphQL."""
        payload = {
            "query": _GQL_QUERY,
            "variables": {
                "page_id": "web_home",
                "after": cursor,
                "external_page_id": None,
            },
        }
        response = self.client.post(
            _GRAPHQL_URL,
            headers=self._gql_headers(),
            json=payload,
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _pick_category(managed_categories: list[dict]) -> str:
        """Return the best human-readable category from Zigzag's hierarchy.

        Prefers depth-3 (e.g. "티셔츠") and normalises via CATEGORY_MAP
        when possible; falls back to the Korean value.
        """
        if not managed_categories:
            return ""
        # Sort by depth descending so we pick the most specific category
        sorted_cats = sorted(
            managed_categories, key=lambda c: c.get("depth", 0), reverse=True
        )
        for cat in sorted_cats:
            value = cat.get("value", "")
            key = cat.get("key", "")
            # Skip very broad top-level categories
            if cat.get("depth", 0) <= 1:
                continue
            # Try to map Korean name to normalised English
            normalised = CATEGORY_MAP.get(value, "")
            if normalised:
                return normalised
            return value
        # Fallback: return deepest category value
        return sorted_cats[0].get("value", "") if sorted_cats else ""

    def _parse_item(self, raw: dict, rank: int) -> Optional[dict]:
        """Parse a single UxGoodsCardItem into our standard dict."""
        title = raw.get("title", "")
        if not title:
            return None

        final_price = raw.get("final_price")
        max_price = raw.get("max_price")
        discount_rate = raw.get("discount_rate")

        # Compute original_price: max_price is the pre-discount price
        original_price = max_price if max_price and max_price != final_price else None

        # If discount_rate is missing but we have both prices, calculate it
        if not discount_rate and original_price and final_price and original_price > 0:
            discount_rate = round((1 - final_price / original_price) * 100)

        product_url = raw.get("product_url", "")
        image_url = raw.get("webp_image_url") or raw.get("image_url", "")

        category = self._pick_category(raw.get("managed_category_list", []))

        return {
            "rank": rank,
            "product_name": title.strip(),
            "brand": (raw.get("shop_name") or "").strip(),
            "price": final_price,
            "original_price": original_price,
            "discount_pct": discount_rate if discount_rate else None,
            "category": category,
            "product_url": product_url,
            "image_url": image_url,
        }

    # ------------------------------------------------------------------
    # Main scrape
    # ------------------------------------------------------------------

    def scrape_bestsellers(self) -> list[dict]:
        """Fetch bestseller products from Zigzag home feed (paginated)."""
        logger.info(f"[{self.platform_name}] Fetching bestsellers from GraphQL API...")

        items: list[dict] = []
        cursor: Optional[str] = None
        rank = 0

        for page_num in range(_MAX_PAGES):
            logger.info(
                f"[{self.platform_name}] Fetching page {page_num + 1}/{_MAX_PAGES}..."
            )

            try:
                data = self._fetch_page(cursor)
            except Exception as exc:
                logger.error(
                    f"[{self.platform_name}] Failed to fetch page {page_num + 1}: {exc}"
                )
                break

            page_info = data.get("data", {}).get("page_info", {})
            ui_items = page_info.get("ui_item_list", [])

            for raw in ui_items:
                if raw.get("__typename") != "UxGoodsCardItem":
                    continue
                rank += 1
                try:
                    item = self._parse_item(raw, rank)
                    if item:
                        items.append(item)
                except Exception as exc:
                    logger.debug(
                        f"[{self.platform_name}] Skipping item at rank {rank}: {exc}"
                    )

            cursor = page_info.get("end_cursor")
            has_next = page_info.get("has_next", False)

            if not has_next or not cursor:
                logger.info(f"[{self.platform_name}] No more pages available.")
                break

            self.random_delay()

        logger.info(
            f"[{self.platform_name}] Extracted {len(items)} bestseller items"
        )
        return items

    def scrape(self) -> int:
        """Run the full Zigzag scrape pipeline."""
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

    with ZigzagScraper() as scraper:
        result = scraper.run()
        print(f"Zigzag scrape complete: {result} items collected")
