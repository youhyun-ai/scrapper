"""Ably (에이블리) bestseller ranking and keyword scraper using Playwright.

Ably's API requires JWT authentication and the website is behind Cloudflare's
JS challenge, so we use Playwright to render the ranking page in a real browser,
then intercept API responses or parse the rendered DOM.
"""

from __future__ import annotations

import json
import re
from typing import Optional

from loguru import logger

from config import ABLY
from scrapers.base import BaseScraper


class AblyScraper(BaseScraper):
    """Scrapes Ably ranking/bestsellers and trending keywords via Playwright."""

    platform_name = "ably"

    def __init__(self) -> None:
        super().__init__()
        self.base_url: str = ABLY["base_url"]
        self.ranking_url: str = ABLY["ranking_url"]
        self.search_url: str = ABLY["search_url"]

    # ------------------------------------------------------------------
    # Playwright helpers
    # ------------------------------------------------------------------

    def _launch_browser(self):
        """Create and return a Playwright browser + context.

        Returns (playwright_instance, browser, context) or raises ImportError.
        """
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            logger.error(
                f"[{self.platform_name}] playwright is not installed. "
                "Run: pip install playwright && playwright install chromium"
            )
            raise

        pw = sync_playwright().start()
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
        return pw, browser, context

    def _scroll_page(self, page, scroll_count: int = 10, delay_ms: int = 1500):
        """Scroll the page to load lazy-loaded content."""
        for _ in range(scroll_count):
            page.evaluate("window.scrollBy(0, window.innerHeight)")
            page.wait_for_timeout(delay_ms)

    # ------------------------------------------------------------------
    # Bestseller scraping
    # ------------------------------------------------------------------

    def _collect_via_playwright(self) -> list[dict]:
        """Launch a headless browser, navigate to the ranking page, and
        collect product data from intercepted API responses or the DOM."""
        try:
            pw, browser, context = self._launch_browser()
        except ImportError:
            return []

        items: list[dict] = []
        api_items: list[dict] = []

        def _handle_response(response):
            """Intercept API responses that contain product/ranking data."""
            url = response.url
            if "api.a-bly.com" not in url:
                return
            if not any(kw in url for kw in ("goods", "ranking", "product", "best", "home")):
                return
            try:
                if response.status == 200 and "json" in (response.headers.get("content-type", "")):
                    data = response.json()
                    api_items.append({"url": url, "data": data})
                    logger.debug(f"[{self.platform_name}] Captured API: {url}")
            except Exception:
                pass

        try:
            page = context.new_page()
            page.on("response", _handle_response)

            logger.info(f"[{self.platform_name}] Navigating to {self.ranking_url}")
            try:
                page.goto(self.ranking_url, wait_until="networkidle", timeout=60000)
            except Exception as e:
                logger.warning(f"[{self.platform_name}] Initial load timeout/error: {e}")

            # Scroll down to load more products
            self._scroll_page(page)

            # Try category tabs to collect more items
            self._try_category_tabs(page, api_items)

            # Try to extract from intercepted API responses first
            items = self._parse_api_responses(api_items)
            if items:
                logger.info(
                    f"[{self.platform_name}] Extracted {len(items)} items from API responses"
                )
            else:
                # Fallback: parse DOM
                logger.info(f"[{self.platform_name}] No API items, falling back to DOM parsing")
                items = self._parse_dom(page)
        finally:
            browser.close()
            pw.stop()

        return items

    def _try_category_tabs(self, page, api_items: list[dict]):
        """Click category tabs on the ranking page to load more products."""
        tab_selectors = [
            "[role='tab']",
            "[class*='tab']",
            "[class*='category'] a",
            "[class*='category'] button",
            "nav a",
            "nav button",
        ]

        tabs = []
        for sel in tab_selectors:
            elements = page.query_selector_all(sel)
            if len(elements) >= 3:
                tabs = elements
                logger.info(
                    f"[{self.platform_name}] Found {len(elements)} tabs with '{sel}'"
                )
                break

        if not tabs:
            logger.debug(f"[{self.platform_name}] No category tabs found")
            return

        # Skip first tab (usually "전체" which we already loaded)
        for i, tab in enumerate(tabs[1:], start=1):
            try:
                tab_text = tab.inner_text().strip()
                logger.info(f"[{self.platform_name}] Clicking tab {i}: '{tab_text}'")
                tab.click()
                page.wait_for_timeout(2000)
                self._scroll_page(page, scroll_count=5, delay_ms=1000)
            except Exception as e:
                logger.debug(f"[{self.platform_name}] Tab click failed: {e}")

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------

    def _parse_api_responses(self, api_items: list[dict]) -> list[dict]:
        """Try to extract product data from captured API responses."""
        items: list[dict] = []
        seen: set[str] = set()
        rank = 0

        for captured in api_items:
            data = captured["data"]
            # Try common response shapes
            products = self._extract_product_list(data)

            for prod in products:
                if not isinstance(prod, dict):
                    continue
                name = (
                    prod.get("goods_name")
                    or prod.get("name")
                    or prod.get("product_name")
                    or prod.get("title")
                    or ""
                )
                if not name:
                    continue

                product_id = str(
                    prod.get("goods_sno")
                    or prod.get("goods_no")
                    or prod.get("id")
                    or prod.get("product_id")
                    or name
                )
                if product_id in seen:
                    continue
                seen.add(product_id)

                rank += 1
                price = (
                    prod.get("sale_price")
                    or prod.get("price")
                    or prod.get("final_price")
                )
                original_price = (
                    prod.get("origin_price")
                    or prod.get("original_price")
                    or prod.get("normal_price")
                )
                discount = (
                    prod.get("discount_rate")
                    or prod.get("discount_pct")
                    or prod.get("discount_percent")
                )
                if not discount and original_price and price and original_price > price:
                    discount = round((1 - price / original_price) * 100)

                brand = (
                    prod.get("market_name")
                    or prod.get("brand")
                    or prod.get("brand_name")
                    or prod.get("shop_name")
                    or ""
                )
                image_url = (
                    prod.get("image_url")
                    or prod.get("thumbnail")
                    or prod.get("img_url")
                    or prod.get("first_img")
                    or ""
                )
                product_url = (
                    prod.get("product_url")
                    or prod.get("url")
                    or ""
                )
                if not product_url and product_id.isdigit():
                    product_url = f"{self.base_url}/goods/{product_id}"

                # Try to extract category
                category = (
                    prod.get("category_name")
                    or prod.get("category")
                    or prod.get("cat_name")
                    or ""
                )

                items.append({
                    "rank": rank,
                    "product_name": name.strip(),
                    "brand": brand.strip() if brand else "",
                    "price": int(price) if price else None,
                    "original_price": int(original_price) if original_price else None,
                    "discount_pct": int(discount) if discount else None,
                    "category": category,
                    "product_url": product_url,
                    "image_url": image_url,
                })

        return items

    @staticmethod
    def _extract_product_list(data) -> list:
        """Extract a list of product dicts from various API response shapes."""
        if isinstance(data, list):
            return data
        if not isinstance(data, dict):
            return []
        # Try nested paths
        for key in ("data", "result", "goods", "items", "products", "content"):
            val = data.get(key)
            if isinstance(val, list) and len(val) > 0:
                return val
            if isinstance(val, dict):
                for subkey in ("list", "items", "goods", "products", "content"):
                    subval = val.get(subkey)
                    if isinstance(subval, list) and len(subval) > 0:
                        return subval
        return []

    def _parse_dom(self, page) -> list[dict]:
        """Parse product data from the rendered DOM as a fallback."""
        items: list[dict] = []

        # Try common selectors for product cards
        selectors = [
            "[class*='product']",
            "[class*='goods']",
            "[class*='item']",
            "[class*='rank']",
            "a[href*='/goods/']",
        ]

        product_elements = []
        for sel in selectors:
            elements = page.query_selector_all(sel)
            if len(elements) > 5:
                product_elements = elements
                logger.info(
                    f"[{self.platform_name}] Found {len(elements)} elements "
                    f"with selector '{sel}'"
                )
                break

        if not product_elements:
            logger.warning(f"[{self.platform_name}] Could not find product elements in DOM")
            # Save page HTML for debugging
            try:
                html = page.content()
                debug_path = f"/tmp/ably_debug_{self.snapshot_date}.html"
                with open(debug_path, "w", encoding="utf-8") as f:
                    f.write(html)
                logger.info(f"[{self.platform_name}] Saved debug HTML to {debug_path}")
            except Exception:
                pass
            return []

        for rank, el in enumerate(product_elements, start=1):
            try:
                name = el.query_selector(
                    "[class*='name'], [class*='title'], [class*='product-name']"
                )
                price = el.query_selector(
                    "[class*='price'], [class*='sale']"
                )
                brand = el.query_selector(
                    "[class*='brand'], [class*='market'], [class*='shop']"
                )
                img = el.query_selector("img")
                link = el.query_selector("a[href]") or el

                name_text = name.inner_text().strip() if name else ""
                if not name_text:
                    continue

                price_text = price.inner_text().strip() if price else ""
                price_val = None
                price_match = re.search(r"[\d,]+", price_text.replace(",", ""))
                if price_match:
                    price_val = int(price_match.group().replace(",", ""))

                brand_text = brand.inner_text().strip() if brand else ""
                img_src = img.get_attribute("src") if img else ""
                href = link.get_attribute("href") if link else ""
                if href and not href.startswith("http"):
                    href = f"{self.base_url}{href}"

                items.append({
                    "rank": rank,
                    "product_name": name_text,
                    "brand": brand_text,
                    "price": price_val,
                    "original_price": None,
                    "discount_pct": None,
                    "category": "",
                    "product_url": href,
                    "image_url": img_src,
                })
            except Exception as e:
                logger.debug(f"[{self.platform_name}] DOM parse error at rank {rank}: {e}")

        return items

    # ------------------------------------------------------------------
    # Keyword scraping
    # ------------------------------------------------------------------

    def _collect_keywords_via_playwright(self) -> list[dict]:
        """Navigate to the search page and collect trending/popular keywords.

        Ably's search page typically shows:
        - 인기 검색어 (popular search terms / trending keywords)
        - 추천 검색어 (recommended search terms)

        We intercept API responses and also parse the DOM as fallback.
        """
        try:
            pw, browser, context = self._launch_browser()
        except ImportError:
            return []

        keywords: list[dict] = []
        api_items: list[dict] = []

        def _handle_response(response):
            url = response.url
            if "api.a-bly.com" not in url:
                return
            if not any(kw in url for kw in (
                "search", "keyword", "suggest", "popular", "trending",
                "recommend", "hot", "rank",
            )):
                return
            try:
                if response.status == 200 and "json" in (response.headers.get("content-type", "")):
                    data = response.json()
                    api_items.append({"url": url, "data": data})
                    logger.debug(f"[{self.platform_name}] Captured keyword API: {url}")
            except Exception:
                pass

        try:
            page = context.new_page()
            page.on("response", _handle_response)

            logger.info(f"[{self.platform_name}] Navigating to {self.search_url}")
            try:
                page.goto(self.search_url, wait_until="networkidle", timeout=60000)
            except Exception as e:
                logger.warning(f"[{self.platform_name}] Search page load error: {e}")

            # Wait a bit for dynamic content
            page.wait_for_timeout(3000)

            # Try clicking on search input to trigger keyword suggestions
            search_selectors = [
                "input[type='search']",
                "input[type='text']",
                "[class*='search'] input",
                "[placeholder*='검색']",
                "[class*='SearchInput']",
            ]
            for sel in search_selectors:
                try:
                    el = page.query_selector(sel)
                    if el:
                        el.click()
                        page.wait_for_timeout(2000)
                        logger.debug(f"[{self.platform_name}] Clicked search input: {sel}")
                        break
                except Exception:
                    pass

            # Parse keywords from API responses
            keywords = self._parse_keyword_api(api_items)
            if keywords:
                logger.info(
                    f"[{self.platform_name}] Extracted {len(keywords)} keywords from API"
                )
            else:
                # Fallback: parse from DOM
                logger.info(
                    f"[{self.platform_name}] No API keywords, trying DOM parsing"
                )
                keywords = self._parse_keyword_dom(page)
        finally:
            browser.close()
            pw.stop()

        return keywords

    def _parse_keyword_api(self, api_items: list[dict]) -> list[dict]:
        """Extract keyword data from intercepted API responses."""
        keywords: list[dict] = []
        seen: set[str] = set()
        rank = 0

        for captured in api_items:
            data = captured["data"]
            keyword_list = self._extract_keyword_list(data)

            for kw in keyword_list:
                if isinstance(kw, str):
                    text = kw.strip()
                    category = ""
                elif isinstance(kw, dict):
                    text = (
                        kw.get("keyword")
                        or kw.get("name")
                        or kw.get("text")
                        or kw.get("title")
                        or kw.get("query")
                        or kw.get("search_keyword")
                        or kw.get("value")
                        or ""
                    ).strip()
                    category = (
                        kw.get("category")
                        or kw.get("type")
                        or kw.get("tag")
                        or ""
                    )
                else:
                    continue

                if not text or text in seen:
                    continue
                seen.add(text)
                rank += 1
                keywords.append({
                    "keyword": text,
                    "rank": rank,
                    "category": category,
                })

        return keywords

    @staticmethod
    def _extract_keyword_list(data) -> list:
        """Extract a list of keywords from various API response shapes."""
        if isinstance(data, list):
            return data
        if not isinstance(data, dict):
            return []
        # Try keyword-specific keys first
        for key in (
            "keywords", "popular_keywords", "trending_keywords",
            "hot_keywords", "recommend_keywords", "search_keywords",
            "suggestions", "popular", "trending", "ranks",
            "data", "result", "items", "list", "content",
        ):
            val = data.get(key)
            if isinstance(val, list) and len(val) > 0:
                return val
            if isinstance(val, dict):
                for subkey in (
                    "keywords", "list", "items", "popular", "trending",
                    "ranks", "content",
                ):
                    subval = val.get(subkey)
                    if isinstance(subval, list) and len(subval) > 0:
                        return subval
        return []

    def _parse_keyword_dom(self, page) -> list[dict]:
        """Extract trending keywords from the rendered DOM."""
        keywords: list[dict] = []
        seen: set[str] = set()

        # Selectors for keyword/trending sections
        keyword_selectors = [
            "[class*='keyword']",
            "[class*='popular']",
            "[class*='trending']",
            "[class*='hot']",
            "[class*='rank'] a",
            "[class*='search'] li",
            "[class*='suggest']",
            "[class*='recommend']",
        ]

        elements = []
        for sel in keyword_selectors:
            found = page.query_selector_all(sel)
            if len(found) >= 3:
                elements = found
                logger.info(
                    f"[{self.platform_name}] Found {len(found)} keyword elements "
                    f"with selector '{sel}'"
                )
                break

        if not elements:
            # Try a broader approach: look for numbered lists
            elements = page.query_selector_all("ol li, [class*='rank'] span")
            if elements:
                logger.info(
                    f"[{self.platform_name}] Found {len(elements)} elements "
                    f"via broad list selector"
                )

        rank = 0
        for el in elements:
            try:
                text = el.inner_text().strip()
                # Clean up: remove leading numbers, dots, whitespace
                text = re.sub(r"^\d+[\.\s]*", "", text).strip()
                if not text or len(text) < 2 or text in seen:
                    continue
                # Skip elements that look like prices or non-keyword content
                if re.match(r"^[\d,]+원?$", text):
                    continue
                seen.add(text)
                rank += 1
                keywords.append({
                    "keyword": text,
                    "rank": rank,
                    "category": "",
                })
            except Exception:
                pass

        if not keywords:
            logger.warning(
                f"[{self.platform_name}] Could not extract keywords from DOM"
            )
            try:
                html = page.content()
                debug_path = f"/tmp/ably_search_debug_{self.snapshot_date}.html"
                with open(debug_path, "w", encoding="utf-8") as f:
                    f.write(html)
                logger.info(
                    f"[{self.platform_name}] Saved search debug HTML to {debug_path}"
                )
            except Exception:
                pass

        return keywords

    # ------------------------------------------------------------------
    # Main scrape
    # ------------------------------------------------------------------

    def scrape(self) -> int:
        """Run the full Ably scrape pipeline: bestsellers + keywords."""
        total = 0

        # Bestsellers
        bestsellers = self._collect_via_playwright()
        if bestsellers:
            self.save_bestsellers(bestsellers)
            total += len(bestsellers)
        else:
            logger.warning(f"[{self.platform_name}] No bestseller data collected")

        self.random_delay()

        # Keywords
        keywords = self._collect_keywords_via_playwright()
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
