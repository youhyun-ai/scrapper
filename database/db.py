"""SQLite database connection and table management."""

import sqlite3
from contextlib import contextmanager
from config import DB_PATH, DATA_DIR


def init_db():
    """Create all tables if they don't exist."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with get_connection() as conn:
        conn.executescript(SCHEMA)


@contextmanager
def get_connection():
    """Context manager for database connections."""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


SCHEMA = """
CREATE TABLE IF NOT EXISTS keyword_rankings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    platform TEXT NOT NULL,
    keyword TEXT NOT NULL,
    rank INTEGER NOT NULL,
    category TEXT,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    snapshot_date TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_kw_platform_date
    ON keyword_rankings(platform, snapshot_date);

CREATE TABLE IF NOT EXISTS bestseller_rankings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    platform TEXT NOT NULL,
    rank INTEGER NOT NULL,
    product_name TEXT NOT NULL,
    brand TEXT,
    price INTEGER,
    original_price INTEGER,
    discount_pct INTEGER,
    category TEXT,
    product_url TEXT,
    image_url TEXT,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    snapshot_date TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_bs_platform_date
    ON bestseller_rankings(platform, snapshot_date);

CREATE TABLE IF NOT EXISTS instagram_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    hashtag TEXT NOT NULL,
    post_count INTEGER,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    snapshot_date TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ig_hashtag_date
    ON instagram_metrics(hashtag, snapshot_date);

CREATE TABLE IF NOT EXISTS scrape_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    platform TEXT NOT NULL,
    status TEXT NOT NULL,
    items_collected INTEGER DEFAULT 0,
    error_message TEXT,
    duration_seconds REAL,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""
