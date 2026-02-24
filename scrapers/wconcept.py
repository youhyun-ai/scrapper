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

    def scrape_bestsellers(self) -> list[dict]:
        """Fetch women's bestseller product rankings from the W Concept API."""
        logger.info(f"[{self.platform_name}] Fetching bestsellers from API...")

        payload = {
            "custNo": "0",
            "domain": "WOMEN",
            "genderType": "all",
            "dateType": "daily",
            "ageGroup": "all",
            "depth1Code": "ALL",
            "depth2Code": "ALL",
            "pageSize": 200,
            "pageNo": 1,
        }

        data = self._api_post(_API_URL, payload)

        if data.get("result") != "SUCCESS":
            logger.error(
                f"[{self.platform_name}] API returned non-success: {data.get('message')}"
            )
            return []

        content = data.get("data", {}).get("content", [])
        logger.info(
            f"[{self.platform_name}] API returned {len(content)} products"
        )

        items: list[dict] = []
        for rank, entry in enumerate(content, start=1):
            try:
                item = self._parse_product(entry, rank)
                if item:
                    items.append(item)
            except Exception as e:
                logger.debug(
                    f"[{self.platform_name}] Skipping product at rank {rank}: {e}"
                )

        logger.info(f"[{self.platform_name}] Extracted {len(items)} bestseller items")
        return items

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
