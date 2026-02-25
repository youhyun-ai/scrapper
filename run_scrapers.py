"""Main entry point — runs all scrapers in sequence."""

import sys
from loguru import logger
from database.db import init_db


def run_all_scrapers():
    """Run each scraper, log results, continue on failure."""
    init_db()
    results = {}

    # Import scrapers here to avoid import errors if one platform is broken
    scraper_classes = []

    try:
        from scrapers.musinsa import MusinsaScraper
        scraper_classes.append(MusinsaScraper)
    except ImportError as e:
        logger.error(f"Failed to import MusinsaScraper: {e}")

    try:
        from scrapers.twentynine_cm import TwentynineCmScraper
        scraper_classes.append(TwentynineCmScraper)
    except ImportError as e:
        logger.error(f"Failed to import TwentynineCmScraper: {e}")

    try:
        from scrapers.wconcept import WConceptScraper
        scraper_classes.append(WConceptScraper)
    except ImportError as e:
        logger.error(f"Failed to import WConceptScraper: {e}")

    try:
        from scrapers.zigzag import ZigzagScraper
        scraper_classes.append(ZigzagScraper)
    except ImportError as e:
        logger.error(f"Failed to import ZigzagScraper: {e}")

    try:
        from scrapers.ably import AblyScraper
        scraper_classes.append(AblyScraper)
    except ImportError as e:
        logger.error(f"Failed to import AblyScraper: {e}")

    # Future:
    # from scrapers.instagram import InstagramScraper

    for scraper_cls in scraper_classes:
        name = scraper_cls.platform_name
        try:
            with scraper_cls() as scraper:
                count = scraper.run()
                results[name] = {"status": "success", "items": count}
                logger.info(f"[{name}] Collected {count} items")
        except Exception as e:
            results[name] = {"status": "failed", "error": str(e)}
            logger.error(f"[{name}] Failed: {e}")

    # Summary
    logger.info("=" * 50)
    logger.info("SCRAPE SUMMARY")
    for name, result in results.items():
        if result["status"] == "success":
            logger.info(f"  {name}: OK ({result['items']} items)")
        else:
            logger.info(f"  {name}: FAILED ({result['error']})")
    logger.info("=" * 50)

    return results


if __name__ == "__main__":
    run_all_scrapers()
