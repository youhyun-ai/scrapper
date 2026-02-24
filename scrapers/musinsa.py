"""Musinsa bestseller rankings and trending keyword scraper using the API."""

from __future__ import annotations

from typing import Optional

from loguru import logger

from config import MUSINSA
from scrapers.base import BaseScraper


class MusinsaScraper(BaseScraper):
    """Scrapes Musinsa for women's fashion bestsellers and trending keywords."""

    platform_name = "musinsa"

    def __init__(self):
        super().__init__()
        self.base_url = MUSINSA["base_url"]
        self.api_headers = {
            "Accept": "application/json",
            "Origin": "https://www.musinsa.com",
            "Referer": "https://www.musinsa.com/",
        }

    def _api_get(self, url: str, params: Optional[dict] = None) -> dict:
        """Make an API request with proper headers."""
        headers = {**self.get_headers(), **self.api_headers}
        response = self.client.get(
            url, headers=headers, params=params, timeout=30
        )
        response.raise_for_status()
        return response.json()

    # Major women's category codes for Musinsa rankings
    _WOMEN_CATEGORIES = {
        "000": "전체",         # all categories (already fetched)
        "002": "아우터",       # outer
        "001": "상의",         # tops
        "003": "바지",         # pants/bottoms
        "020": "원피스",       # dresses
        "022": "스커트",       # skirts
        "004": "가방",         # bags
        "005": "신발",         # shoes
    }

    # Ranking theme sections – each section returns ~100 products per category.
    # Fetching multiple sections with the same categories yields different
    # (partially overlapping) product sets, letting us collect 800-1000 items.
    _RANKING_SECTIONS = {
        "199": "전체",         # overall ranking
        "200": "NEW",          # new/trending ranking
        "201": "급상승",       # fast-rising ranking
    }

    # ------------------------------------------------------------------
    # Bestseller scraping via API
    # ------------------------------------------------------------------

    def _build_section_url(self, section_id: str) -> str:
        """Build the API URL for a given ranking section."""
        api_base = MUSINSA["api_base"]
        return f"{api_base}/sections/{section_id}"

    def _fetch_category_bestsellers(
        self,
        category_code: str,
        category_name: str,
        section_id: str = "200",
        section_label: str = "",
    ) -> list:
        """Fetch bestseller rankings for a single category and section.

        Parameters
        ----------
        category_code : str
            Musinsa category code (e.g. ``"001"`` for tops).
        category_name : str
            Human-readable category name.
        section_id : str
            Ranking theme section id (default ``"200"`` = NEW).
        section_label : str
            Human-readable label for the section (for logging).
        """
        label = f"{section_label}/{category_name}" if section_label else category_name
        logger.info(
            f"[{self.platform_name}] Fetching bestsellers for "
            f"{label} (section={section_id}, cat={category_code})..."
        )

        url = self._build_section_url(section_id)
        params = {
            "storeCode": "musinsa",
            "categoryCode": category_code,
            "gf": "F",  # women
        }

        data = self._api_get(url, params)
        modules = data.get("data", {}).get("modules", [])

        items = []
        rank = 0

        for module in modules:
            if module.get("type") != "MULTICOLUMN":
                continue

            for entry in module.get("items", []):
                if entry.get("type") != "PRODUCT_COLUMN":
                    continue

                rank += 1
                try:
                    item = self._parse_product(entry, rank)
                    if item:
                        # Tag category if not already set and this isn't "all"
                        if category_code != "000" and not item.get("category"):
                            item["category"] = category_name
                        items.append(item)
                except Exception as e:
                    logger.debug(
                        f"[{self.platform_name}] Skipping product at rank {rank}: {e}"
                    )

        logger.info(
            f"[{self.platform_name}] Extracted {len(items)} items for "
            f"{category_name} ({category_code})"
        )
        return items

    def scrape_bestsellers(self) -> list:
        """Fetch women's bestseller product rankings from the Musinsa API.

        Iterates over multiple ranking-theme *sections* (e.g. overall, NEW,
        fast-rising) and within each section fetches all major women's
        categories.  Products are deduplicated by URL so that the final list
        contains ~800-1000 unique items.
        """
        logger.info(f"[{self.platform_name}] Fetching bestsellers from API...")

        all_items: list[dict] = []
        seen_urls: set[str] = set()
        request_count = 0

        for sec_id, sec_label in self._RANKING_SECTIONS.items():
            for cat_code, cat_name in self._WOMEN_CATEGORIES.items():
                try:
                    items = self._fetch_category_bestsellers(
                        category_code=cat_code,
                        category_name=cat_name,
                        section_id=sec_id,
                        section_label=sec_label,
                    )
                    for item in items:
                        url = item.get("product_url", "")
                        if url and url in seen_urls:
                            continue
                        if url:
                            seen_urls.add(url)
                        all_items.append(item)
                except Exception as e:
                    logger.warning(
                        f"[{self.platform_name}] Failed to fetch "
                        f"{sec_label}/{cat_name} (section={sec_id}, "
                        f"cat={cat_code}): {e}"
                    )

                request_count += 1
                self.random_delay()

        # Re-number ranks sequentially after dedup
        for i, item in enumerate(all_items, start=1):
            item["rank"] = i

        total_requests = len(self._RANKING_SECTIONS) * len(self._WOMEN_CATEGORIES)
        logger.info(
            f"[{self.platform_name}] Extracted {len(all_items)} bestseller items "
            f"(after dedup across {total_requests} section/category combos)"
        )
        return all_items

    def _parse_product(self, entry: dict, rank: int) -> Optional[dict]:
        """Parse a single PRODUCT_COLUMN entry from the API response."""
        info = entry.get("info", {})
        if not info:
            return None

        product_name = info.get("productName", "")
        if not product_name:
            return None

        brand = info.get("brandName", "")
        final_price = info.get("finalPrice")
        discount_ratio = info.get("discountRatio", 0)

        # Calculate original price from discount ratio
        original_price = None
        if discount_ratio and final_price:
            original_price = round(final_price / (1 - discount_ratio / 100))

        # Product URL from onClick
        product_url = ""
        on_click = entry.get("onClick", {})
        product_url = on_click.get("url", "")
        if not product_url:
            product_id = entry.get("id", "")
            if product_id:
                product_url = f"{self.base_url}/app/goods/{product_id}"

        # Image URL
        image_url = entry.get("imageUrl", "")
        if image_url and image_url.startswith("//"):
            image_url = f"https:{image_url}"

        return {
            "rank": rank,
            "product_name": product_name.strip(),
            "brand": brand.strip(),
            "price": final_price,
            "original_price": original_price,
            "discount_pct": discount_ratio if discount_ratio else None,
            "category": "",
            "product_url": product_url,
            "image_url": image_url,
        }

    # ------------------------------------------------------------------
    # Keyword ranking scraping via API
    # ------------------------------------------------------------------

    def scrape_keywords(self) -> list:
        """Fetch trending search keyword rankings from the Musinsa API."""
        logger.info(f"[{self.platform_name}] Fetching keyword rankings from API...")

        url = MUSINSA["keyword_url"]
        params = {
            "storeCode": "musinsa",
            "subPan": "keyword",
            "gf": "F",  # women
        }

        data = self._api_get(url, params)
        modules = data.get("data", {}).get("modules", [])

        keywords = []
        for module in modules:
            if module.get("type") != "RANKING_SEARCH":
                continue

            try:
                rank = module.get("rank", "")
                title = module.get("title", {}).get("text", "")
                fluctuation = module.get("fluctuation", {})
                fluct_type = fluctuation.get("type", "NONE")
                fluct_amount = fluctuation.get("amount", "0")

                if title:
                    keywords.append({
                        "keyword": title.strip(),
                        "rank": int(rank) if str(rank).isdigit() else len(keywords) + 1,
                        "category": f"{fluct_type}:{fluct_amount}",
                    })
            except Exception as e:
                logger.debug(f"[{self.platform_name}] Skipping keyword: {e}")

        logger.info(f"[{self.platform_name}] Extracted {len(keywords)} keywords")
        return keywords

    # ------------------------------------------------------------------
    # Main scrape method
    # ------------------------------------------------------------------

    def scrape(self) -> int:
        """Run the full Musinsa scrape: bestsellers + keywords."""
        total = 0

        # Bestsellers
        bestsellers = self.scrape_bestsellers()
        if bestsellers:
            self.save_bestsellers(bestsellers)
            total += len(bestsellers)
        else:
            logger.warning(f"[{self.platform_name}] No bestseller data collected")

        self.random_delay()

        # Keywords
        keywords = self.scrape_keywords()
        if keywords:
            self.save_keywords(keywords)
            total += len(keywords)
        else:
            logger.warning(f"[{self.platform_name}] No keyword data collected")

        return total


if __name__ == "__main__":
    from database.db import init_db

    init_db()

    with MusinsaScraper() as scraper:
        result = scraper.run()
        print(f"Musinsa scrape complete: {result} items collected")
