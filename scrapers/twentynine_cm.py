"""29CM bestseller rankings scraper using the recommend API."""

from __future__ import annotations

from typing import Optional

from loguru import logger

from config import TWENTYNINE_CM
from scrapers.base import BaseScraper


# 29CM API configuration
RECOMMEND_API_BASE = "https://recommend-api.29cm.co.kr"
BEST_ITEMS_ENDPOINT = "/api/v4/best/items"
BEST_CATEGORIES_ENDPOINT = "/api/v4/best/categories"

IMAGE_BASE_URL = "https://img.29cm.co.kr"
PRODUCT_URL_TEMPLATE = "https://www.29cm.co.kr/product/{item_no}"

# Women's fashion large category code
WOMEN_CATEGORY_CODE = "268100100"


class TwentynineCmScraper(BaseScraper):
    """Scrapes 29CM for women's fashion bestsellers via the recommend API."""

    platform_name = "twentynine_cm"

    def __init__(self):
        super().__init__()
        self.base_url = TWENTYNINE_CM["base_url"]
        self.api_headers = {
            "Accept": "application/json",
            "Origin": "https://shop.29cm.co.kr",
            "Referer": "https://shop.29cm.co.kr/best-items",
        }

    def _api_get(self, url: str, params: Optional[dict] = None) -> dict:
        """Make an API request with proper headers."""
        headers = {**self.get_headers(), **self.api_headers}
        response = self.client.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        return response.json()

    # ------------------------------------------------------------------
    # Category fetching
    # ------------------------------------------------------------------

    def fetch_categories(self) -> list[dict]:
        """Fetch women's fashion subcategories from 29CM."""
        logger.info(f"[{self.platform_name}] Fetching categories...")
        url = f"{RECOMMEND_API_BASE}{BEST_CATEGORIES_ENDPOINT}"
        data = self._api_get(url, params={"categoryList": WOMEN_CATEGORY_CODE})

        if data.get("result") != "SUCCESS":
            logger.warning(f"[{self.platform_name}] Category fetch failed: {data}")
            return []

        categories = data.get("data", [])
        logger.info(
            f"[{self.platform_name}] Found {len(categories)} subcategories"
        )
        return categories

    # ------------------------------------------------------------------
    # Bestseller scraping
    # ------------------------------------------------------------------

    def scrape_bestsellers(self) -> list[dict]:
        """Fetch women's bestseller product rankings from the 29CM recommend API."""
        logger.info(f"[{self.platform_name}] Fetching bestsellers from API...")

        url = f"{RECOMMEND_API_BASE}{BEST_ITEMS_ENDPOINT}"
        params = {
            "categoryList": WOMEN_CATEGORY_CODE,
            "periodSort": "NOW",
            "limit": "100",
            "offset": "0",
        }

        data = self._api_get(url, params)

        if data.get("result") != "SUCCESS":
            logger.warning(f"[{self.platform_name}] API returned: {data.get('result')}")
            return []

        content = data.get("data", {}).get("content", [])
        items = []

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

        # Brand info
        brand_kor = entry.get("frontBrandNameKor", "")
        brand_eng = entry.get("frontBrandNameEng", "")
        brand = brand_kor if brand_kor else brand_eng

        # Price info from saleInfoV2 (more detailed) or fallback to top-level
        sale_info = entry.get("saleInfoV2", {})
        if sale_info:
            original_price = sale_info.get("consumerPrice")
            price = sale_info.get("totalSellPrice") or sale_info.get("sellPrice")
            discount_pct = sale_info.get("totalSaleRate") or sale_info.get("saleRate")
        else:
            original_price = entry.get("consumerPrice")
            price = entry.get("lastSalePrice")
            discount_pct = entry.get("lastSalePercent")

        # Product URL
        item_no = entry.get("itemNo")
        product_url = PRODUCT_URL_TEMPLATE.format(item_no=item_no) if item_no else ""

        # Image URL
        image_url = entry.get("imageUrl", "")
        if image_url and not image_url.startswith("http"):
            image_url = f"{IMAGE_BASE_URL}{image_url}"

        # Category from frontCategoryInfo
        category = ""
        front_cats = entry.get("frontCategoryInfo", [])
        if front_cats:
            cat = front_cats[0]
            cat2_name = cat.get("category2Name", "")
            cat3_name = cat.get("category3Name", "")
            category = f"{cat2_name} > {cat3_name}" if cat3_name else cat2_name

        return {
            "rank": rank,
            "product_name": item_name.strip(),
            "brand": brand.strip(),
            "price": price,
            "original_price": original_price,
            "discount_pct": discount_pct if discount_pct else None,
            "category": category,
            "product_url": product_url,
            "image_url": image_url,
        }

    # ------------------------------------------------------------------
    # Main scrape method
    # ------------------------------------------------------------------

    def scrape(self) -> int:
        """Run the full 29CM scrape: bestsellers."""
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

    with TwentynineCmScraper() as scraper:
        result = scraper.run()
        print(f"29CM scrape complete: {result} items collected")
