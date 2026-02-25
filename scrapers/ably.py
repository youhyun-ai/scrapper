"""Ably (에이블리) bestseller ranking scraper using Playwright.

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
    """Scrapes Ably ranking/bestsellers via Playwright browser automation."""

    platform_name = "ably"

    def __init__(self) -> None:
        super().__init__()
        self.base_url: str = ABLY["base_url"]
        self.ranking_url: str = ABLY["ranking_url"]

    # ------------------------------------------------------------------
    # Playwright helpers
    # ------------------------------------------------------------------

    def _collect_via_playwright(self) -> list[dict]:
        """Launch a headless browser, navigate to the ranking page, and
        collect product data from intercepted API responses or the DOM."""
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            logger.error(
                f"[{self.platform_name}] playwright is not installed. "
                "Run: pip install playwright && playwright install chromium"
            )
            return []

        items: list[dict] = []
        api_items: list[dict] = []

        def _handle_response(response):
            """Intercept API responses that contain product/ranking data."""
            url = response.url
            if "api.a-bly.com" not in url:
                return
            # Look for goods/ranking/product-related endpoints
            if not any(kw in url for kw in ("goods", "ranking", "product", "best", "home")):
                return
            try:
                if response.status == 200 and "json" in (response.headers.get("content-type", "")):
                    data = response.json()
                    api_items.append({"url": url, "data": data})
                    logger.debug(f"[{self.platform_name}] Captured API: {url}")
            except Exception:
                pass

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
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
            page.on("response", _handle_response)

            logger.info(f"[{self.platform_name}] Navigating to {self.ranking_url}")
            try:
                page.goto(self.ranking_url, wait_until="networkidle", timeout=60000)
            except Exception as e:
                logger.warning(f"[{self.platform_name}] Initial load timeout/error: {e}")
                # Still try to work with what we have

            # Scroll down to load more products
            for scroll_i in range(10):
                page.evaluate("window.scrollBy(0, window.innerHeight)")
                page.wait_for_timeout(1500)

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

            browser.close()

        return items

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
            products = []
            if isinstance(data, dict):
                # Try nested paths
                for key in ("data", "result", "goods", "items", "products", "content"):
                    val = data.get(key)
                    if isinstance(val, list) and len(val) > 0:
                        products = val
                        break
                    if isinstance(val, dict):
                        for subkey in ("list", "items", "goods", "products", "content"):
                            subval = val.get(subkey)
                            if isinstance(subval, list) and len(subval) > 0:
                                products = subval
                                break
                        if products:
                            break
            elif isinstance(data, list):
                products = data

            for prod in products:
                if not isinstance(prod, dict):
                    continue
                # Try to extract product fields from various field names
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

                items.append({
                    "rank": rank,
                    "product_name": name.strip(),
                    "brand": brand.strip() if brand else "",
                    "price": int(price) if price else None,
                    "original_price": int(original_price) if original_price else None,
                    "discount_pct": int(discount) if discount else None,
                    "category": "",
                    "product_url": product_url,
                    "image_url": image_url,
                })

        return items

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
    # Main scrape
    # ------------------------------------------------------------------

    def scrape(self) -> int:
        """Run the full Ably scrape pipeline."""
        total = 0

        bestsellers = self._collect_via_playwright()
        if bestsellers:
            self.save_bestsellers(bestsellers)
            total += len(bestsellers)
        else:
            logger.warning(f"[{self.platform_name}] No bestseller data collected")

        return total


if __name__ == "__main__":
    from database.db import init_db

    init_db()

    with AblyScraper() as scraper:
        result = scraper.run()
        print(f"Ably scrape complete: {result} items collected")
