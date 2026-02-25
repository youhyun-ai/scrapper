"""Ably (에이블리) bestseller ranking and keyword scraper.

Ably's API requires a JWT anonymous token obtained via Cloudflare-protected
pages.  We use Playwright to load the home page once, capture the auth
headers/cookies, and then call the REST API directly with httpx for
efficient paginated scraping.

Key endpoints:
- ``/api/v2/screens/TODAY/`` — paginated product feed (~400+ items)
- ``/api/v2/search/popular_queries/`` — trending keywords per age range
"""

from __future__ import annotations

import re
from typing import Optional

from loguru import logger

from config import ABLY
from scrapers.base import BaseScraper

# Maximum pages to fetch from the screens/TODAY feed.
# Each page has ~10 goods; 50 pages ≈ 500 items.
_MAX_FEED_PAGES = 50

# Age-range values for the popular-keywords endpoint.
# 0=전체, 1=10대, 2=20대초반, 3=20대중반, 4=20대후반, 5=30대이상
_KEYWORD_AGE_RANGES = [0, 1, 2, 3, 4, 5]


class AblyScraper(BaseScraper):
    """Scrapes Ably bestsellers and trending keywords via API."""

    platform_name = "ably"

    def __init__(self) -> None:
        super().__init__()
        self.base_url: str = ABLY["base_url"]
        self.ranking_url: str = ABLY["ranking_url"]
        self.search_url: str = ABLY["search_url"]
        self._api_headers: dict = {}

    # ------------------------------------------------------------------
    # Auth: obtain headers via Playwright
    # ------------------------------------------------------------------

    def _obtain_api_headers(self) -> dict:
        """Load the home page in a headless browser to obtain API auth headers.

        Ably sets an ``x-anonymous-token`` cookie/header after the initial
        page load which is required for all subsequent API calls.  We
        capture the request headers from the first successful
        ``screens/TODAY`` API call.
        """
        if self._api_headers:
            return self._api_headers

        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            logger.error(
                f"[{self.platform_name}] playwright is not installed. "
                "Run: pip install playwright && playwright install chromium"
            )
            return {}

        captured: dict = {}

        pw = sync_playwright().start()
        try:
            browser = pw.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
                    "AppleWebKit/605.1.15 (KHTML, like Gecko) "
                    "Version/17.0 Mobile/15E148 Safari/604.1"
                ),
                viewport={"width": 390, "height": 844},
                locale="ko-KR",
            )
            page = context.new_page()

            def _on_response(response):
                url = response.url
                if (
                    "api.a-bly.com/api/v2/screens/TODAY" in url
                    and response.status == 200
                    and not captured
                ):
                    captured["headers"] = dict(response.request.headers)

            page.on("response", _on_response)

            logger.info(f"[{self.platform_name}] Loading home page for auth...")
            try:
                page.goto(self.ranking_url, wait_until="domcontentloaded", timeout=60000)
                page.wait_for_timeout(5000)
            except Exception as e:
                logger.warning(f"[{self.platform_name}] Home page load error: {e}")

            # Capture cookies
            cookies = context.cookies()
            cookie_str = "; ".join(f'{c["name"]}={c["value"]}' for c in cookies)

            browser.close()
        finally:
            pw.stop()

        if not captured:
            logger.error(f"[{self.platform_name}] Failed to capture API headers")
            return {}

        headers = dict(captured["headers"])
        headers["Accept"] = "application/json"
        if cookie_str:
            headers["Cookie"] = cookie_str

        self._api_headers = headers
        logger.info(f"[{self.platform_name}] Obtained API auth headers")
        return headers

    # ------------------------------------------------------------------
    # Bestseller scraping via direct API
    # ------------------------------------------------------------------

    def scrape_bestsellers(self) -> list[dict]:
        """Fetch bestseller products by paginating the screens/TODAY API.

        Each page returns ~10 product cards.  We paginate up to
        ``_MAX_FEED_PAGES`` pages, deduplicating by ``sno`` (goods ID).
        """
        headers = self._obtain_api_headers()
        if not headers:
            return []

        items: list[dict] = []
        seen: set[str] = set()
        next_token: Optional[str] = None
        rank = 0

        logger.info(
            f"[{self.platform_name}] Fetching bestsellers from screens/TODAY API "
            f"(up to {_MAX_FEED_PAGES} pages)..."
        )

        for page_num in range(_MAX_FEED_PAGES):
            url = "https://api.a-bly.com/api/v2/screens/TODAY/"
            params: dict = {}
            if next_token:
                params["next_token"] = next_token

            try:
                response = self.client.get(url, headers=headers, params=params, timeout=30)
                if response.status_code != 200:
                    logger.warning(
                        f"[{self.platform_name}] Page {page_num}: HTTP {response.status_code}"
                    )
                    break
                data = response.json()
            except Exception as e:
                logger.error(f"[{self.platform_name}] Page {page_num} fetch error: {e}")
                break

            products = self._extract_goods_from_screens(data)
            page_new = 0

            for prod in products:
                sno = str(prod.get("sno") or "")
                name = prod.get("name") or prod.get("goods_name") or ""
                if not name or not sno:
                    continue

                if sno in seen:
                    continue
                seen.add(sno)

                rank += 1
                page_new += 1

                fpr = prod.get("first_page_rendering", {}) or {}
                raw_price = prod.get("price")
                if isinstance(raw_price, dict):
                    sale_price = raw_price.get("sale_price") or raw_price.get("price")
                    original_price = raw_price.get("origin_price") or raw_price.get("original_price")
                else:
                    sale_price = raw_price or fpr.get("price")
                    original_price = fpr.get("original_price")

                discount = prod.get("discount_rate") or fpr.get("discount_rate")
                if not discount and original_price and sale_price and original_price > sale_price:
                    discount = round((1 - sale_price / original_price) * 100)

                brand = prod.get("market_name") or fpr.get("market_name") or ""
                image_url = (
                    prod.get("image")
                    or prod.get("image_url")
                    or fpr.get("cover_image")
                    or ""
                )
                category = prod.get("category_name") or ""

                items.append({
                    "rank": rank,
                    "product_name": name.strip(),
                    "brand": brand.strip() if brand else "",
                    "price": int(sale_price) if sale_price else None,
                    "original_price": int(original_price) if original_price else None,
                    "discount_pct": int(discount) if discount else None,
                    "category": category,
                    "product_url": f"{self.base_url}/goods/{sno}",
                    "image_url": image_url,
                })

            next_token = data.get("next_token")

            if page_num % 10 == 0 or not next_token:
                logger.info(
                    f"[{self.platform_name}] Page {page_num}: "
                    f"+{page_new} new (total {len(items)})"
                )

            if not next_token:
                logger.info(f"[{self.platform_name}] No more pages (end of feed)")
                break

        logger.info(
            f"[{self.platform_name}] Extracted {len(items)} bestseller items "
            f"from {page_num + 1} API pages"
        )
        return items

    @staticmethod
    def _extract_goods_from_screens(data) -> list[dict]:
        """Extract flat list of goods dicts from Ably screens API response.

        Walks through ``components`` → ``entity`` → ``item_list`` →
        ``item_entity`` → ``item`` to collect product dicts.
        """
        if not isinstance(data, dict):
            return []

        components = data.get("components", [])
        if not isinstance(components, list):
            return []

        goods: list[dict] = []
        for comp in components:
            comp_type = comp.get("type", {})
            if not isinstance(comp_type, dict):
                continue
            item_list_type = comp_type.get("item_list", "")
            if not any(kw in item_list_type for kw in ("CARD_LIST", "GOODS_LIST")):
                continue

            entity = comp.get("entity", {})
            if not isinstance(entity, dict):
                continue
            item_list = entity.get("item_list", [])
            if not isinstance(item_list, list):
                continue

            for entry in item_list:
                if not isinstance(entry, dict):
                    continue
                ie = entry.get("item_entity", {})
                item = ie.get("item") if isinstance(ie, dict) else None
                if not item:
                    item = entry.get("item")
                if isinstance(item, dict) and item.get("sno"):
                    goods.append(item)

        return goods

    # ------------------------------------------------------------------
    # Keyword scraping via direct API
    # ------------------------------------------------------------------

    def scrape_keywords(self) -> list[dict]:
        """Fetch trending search keywords across all age ranges.

        Ably's ``/api/v2/search/popular_queries/`` endpoint returns the
        top-10 keywords for each age demographic.  We fetch all 6 ranges
        and deduplicate to collect ~40-50 unique keywords.
        """
        headers = self._obtain_api_headers()
        if not headers:
            return []

        keywords: list[dict] = []
        seen: set[str] = set()
        rank = 0

        age_labels = {
            0: "전체", 1: "10대", 2: "20대초반",
            3: "20대중반", 4: "20대후반", 5: "30대이상",
        }

        logger.info(
            f"[{self.platform_name}] Fetching keywords across "
            f"{len(_KEYWORD_AGE_RANGES)} age ranges..."
        )

        for age_range in _KEYWORD_AGE_RANGES:
            label = age_labels.get(age_range, str(age_range))
            try:
                response = self.client.get(
                    "https://api.a-bly.com/api/v2/search/popular_queries/",
                    headers=headers,
                    params={"age_range": age_range},
                    timeout=30,
                )
                if response.status_code != 200:
                    logger.warning(
                        f"[{self.platform_name}] Keywords age={label}: "
                        f"HTTP {response.status_code}"
                    )
                    continue
                data = response.json()
            except Exception as e:
                logger.warning(
                    f"[{self.platform_name}] Keywords age={label} error: {e}"
                )
                continue

            queries = data.get("queries", [])
            new_count = 0

            for kw in queries:
                if isinstance(kw, str):
                    text = kw.strip()
                elif isinstance(kw, dict):
                    text = (
                        kw.get("keyword")
                        or kw.get("name")
                        or kw.get("query")
                        or kw.get("text")
                        or kw.get("value")
                        or ""
                    ).strip()
                else:
                    continue

                if not text or text in seen:
                    continue
                seen.add(text)
                rank += 1
                new_count += 1
                keywords.append({
                    "keyword": text,
                    "rank": rank,
                    "category": label,
                })

            logger.debug(
                f"[{self.platform_name}] Keywords age={label}: "
                f"{len(queries)} total, {new_count} new"
            )

        logger.info(
            f"[{self.platform_name}] Extracted {len(keywords)} unique keywords "
            f"across {len(_KEYWORD_AGE_RANGES)} age ranges"
        )
        return keywords

    # ------------------------------------------------------------------
    # Main scrape
    # ------------------------------------------------------------------

    def scrape(self) -> int:
        """Run the full Ably scrape pipeline: bestsellers + keywords."""
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

    with AblyScraper() as scraper:
        result = scraper.run()
        print(f"Ably scrape complete: {result} items collected")
