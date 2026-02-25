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
            if not any(kw in url for kw in (
                "goods", "ranking", "product", "best", "home",
                "category", "display", "feed", "recommend", "pick",
                "screens",
            )):
                return
            try:
                content_type = response.headers.get("content-type", "")
                if response.status == 200 and "json" in content_type:
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
                page.goto(self.ranking_url, wait_until="domcontentloaded", timeout=60000)
                # Wait for dynamic content to load after initial DOM
                page.wait_for_timeout(5000)
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
        """Extract product data from captured API responses.

        Ably's main feed uses the ``/api/v2/screens/TODAY/`` endpoint which
        returns a ``components`` list.  Each component with a ``CARD_LIST``
        item_list type contains product items nested as::

            component -> entity -> item_list[] -> item_entity -> item
        """
        items: list[dict] = []
        seen: set[str] = set()
        rank = 0

        for captured in api_items:
            data = captured["data"]
            products = self._extract_goods_from_screens(data)

            for prod in products:
                sno = str(prod.get("sno") or prod.get("goods_sno") or "")
                name = prod.get("name") or prod.get("goods_name") or ""
                if not name:
                    continue

                product_id = sno or name
                if product_id in seen:
                    continue
                seen.add(product_id)

                rank += 1

                # Price can be a plain int or nested in first_page_rendering
                fpr = prod.get("first_page_rendering", {}) or {}
                raw_price = prod.get("price")
                if isinstance(raw_price, dict):
                    sale_price = raw_price.get("sale_price") or raw_price.get("price")
                    original_price = raw_price.get("origin_price") or raw_price.get("original_price")
                else:
                    sale_price = raw_price or fpr.get("price")
                    original_price = fpr.get("original_price")

                discount = (
                    prod.get("discount_rate")
                    or fpr.get("discount_rate")
                )
                if not discount and original_price and sale_price and original_price > sale_price:
                    discount = round((1 - sale_price / original_price) * 100)

                brand = (
                    prod.get("market_name")
                    or fpr.get("market_name")
                    or ""
                )
                image_url = (
                    prod.get("image")
                    or prod.get("image_url")
                    or fpr.get("cover_image")
                    or ""
                )
                category = prod.get("category_name") or ""
                product_url = f"{self.base_url}/goods/{sno}" if sno else ""

                items.append({
                    "rank": rank,
                    "product_name": name.strip(),
                    "brand": brand.strip() if brand else "",
                    "price": int(sale_price) if sale_price else None,
                    "original_price": int(original_price) if original_price else None,
                    "discount_pct": int(discount) if discount else None,
                    "category": category,
                    "product_url": product_url,
                    "image_url": image_url,
                })

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
            # Only process component types that contain product cards
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
                # Navigate: item_entity -> item
                ie = entry.get("item_entity", {})
                item = ie.get("item") if isinstance(ie, dict) else None
                if not item:
                    item = entry.get("item")
                if isinstance(item, dict) and item.get("sno"):
                    goods.append(item)

        return goods

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
                page.goto(self.search_url, wait_until="domcontentloaded", timeout=60000)
                page.wait_for_timeout(5000)
            except Exception as e:
                logger.warning(f"[{self.platform_name}] Search page load error: {e}")

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
            logger.debug(
                f"[{self.platform_name}] Keyword API response from {captured['url']}: "
                f"keys={list(data.keys()) if isinstance(data, dict) else type(data).__name__}"
            )
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
            "popular_queries", "queries", "keywords",
            "popular_keywords", "trending_keywords",
            "hot_keywords", "recommend_keywords", "search_keywords",
            "suggestions", "popular", "trending", "ranks",
            "data", "result", "items", "list", "content",
        ):
            val = data.get(key)
            if isinstance(val, list) and len(val) > 0:
                return val
            if isinstance(val, dict):
                for subkey in (
                    "popular_queries", "queries", "keywords",
                    "list", "items", "popular", "trending",
                    "ranks", "content",
                ):
                    subval = val.get(subkey)
                    if isinstance(subval, list) and len(subval) > 0:
                        return subval
        return []

    def _parse_keyword_dom(self, page) -> list[dict]:
        """Extract trending keywords from the rendered DOM.

        Ably's search page shows "인기 검색어" as a list of ``<li>`` elements,
        each containing a rank number ``<p>`` and a keyword ``<p>``.  The CSS
        class names are styled-components hashes so we match by structure.
        """
        keywords: list[dict] = []
        seen: set[str] = set()

        # Ably-specific: keyword list items contain two <p> tags
        # (rank number + keyword text) inside <li> elements.
        # Try the most specific selectors first, then broaden.
        keyword_selectors = [
            # Ably's actual structure: <ul> with <li> children in search page
            "ul li p.typography.typography__body2",
            # Broader: any list items in the main content
            "main ul li",
            "ul li",
            # Generic fallbacks
            "[class*='keyword']",
            "[class*='popular']",
            "[class*='search'] li",
            "ol li",
        ]

        elements = []
        for sel in keyword_selectors:
            found = page.query_selector_all(sel)
            if len(found) >= 5:
                elements = found
                logger.info(
                    f"[{self.platform_name}] Found {len(found)} keyword elements "
                    f"with selector '{sel}'"
                )
                break

        rank = 0
        for el in elements:
            try:
                text = el.inner_text().strip()
                # Clean up: remove leading numbers, dots, whitespace
                text = re.sub(r"^\d+[\.\s]*", "", text).strip()
                if not text or len(text) < 2 or text in seen:
                    continue
                # Skip elements that are just numbers (rank indicators)
                if re.match(r"^\d+$", text):
                    continue
                # Skip elements that look like prices
                if re.match(r"^[\d,]+원?$", text):
                    continue
                # Skip age-range filter labels
                if re.match(r"^\d+대", text) or text in ("전체", "30대 이상"):
                    continue
                # Skip UI elements
                if text in ("앱에서 보기", "홈으로 이동", "보러가기", "완료"):
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
