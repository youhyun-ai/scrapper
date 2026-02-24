"""Scheduler — runs all scrapers on a daily schedule using the schedule library."""

import argparse
import sys
import time

import schedule
from loguru import logger

from config import SCRAPE_TIMES
from run_scrapers import run_all_scrapers


def _scheduled_run():
    """Wrapper called by the schedule library at each configured time."""
    logger.info("Scheduled scrape triggered")
    try:
        results = run_all_scrapers()
        success = sum(1 for r in results.values() if r["status"] == "success")
        failed = sum(1 for r in results.values() if r["status"] == "failed")
        logger.info(f"Scrape complete: {success} succeeded, {failed} failed")
    except Exception as e:
        logger.exception(f"Unexpected error during scheduled scrape: {e}")


def main():
    parser = argparse.ArgumentParser(description="Fashion trend scraper scheduler")
    parser.add_argument(
        "--now",
        action="store_true",
        help="Run all scrapers immediately once, then exit",
    )
    args = parser.parse_args()

    if args.now:
        logger.info("Running all scrapers immediately (--now flag)")
        _scheduled_run()
        logger.info("Immediate run finished — exiting")
        return

    # Register scheduled jobs for each configured time
    for t in SCRAPE_TIMES:
        schedule.every().day.at(t).do(_scheduled_run)
        logger.info(f"Scheduled daily scrape at {t}")

    logger.info(
        f"Scheduler started — {len(SCRAPE_TIMES)} daily job(s) registered. "
        "Press Ctrl+C to stop."
    )

    try:
        while True:
            schedule.run_pending()
            time.sleep(30)
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user (KeyboardInterrupt)")
        sys.exit(0)


if __name__ == "__main__":
    main()
