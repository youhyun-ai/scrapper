"""Centralized configuration for the fashion trend scraper."""

import os
from pathlib import Path

# Paths
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "trends.db"
LOG_DIR = BASE_DIR / "logs"

# Scraper settings
REQUEST_TIMEOUT = 30  # seconds
MIN_DELAY = 2  # seconds between requests
MAX_DELAY = 5
MAX_RETRIES = 3

# --- Platform URLs ---

MUSINSA = {
    "base_url": "https://www.musinsa.com",
    "api_base": "https://api.musinsa.com/api2/hm/web/v5/pans/ranking",
    "product_section_url": "https://api.musinsa.com/api2/hm/web/v5/pans/ranking/sections/200",
    "keyword_url": "https://api.musinsa.com/api2/hm/web/v5/pans/ranking",
}

TWENTYNINE_CM = {
    "bestseller_url": "https://shop.29cm.co.kr/best-items",
    "params": {
        "category_large_code": "268100100",  # women's fashion
    },
    "base_url": "https://www.29cm.co.kr",
}

WCONCEPT = {
    "bestseller_url": "https://display.wconcept.co.kr/best",
    "women_url": "https://display.wconcept.co.kr/category/women",
    "base_url": "https://www.wconcept.co.kr",
}

ZIGZAG = {
    "base_url": "https://zigzag.kr",
    "ranking_url": "https://zigzag.kr/",
}

# --- Instagram ---

INSTAGRAM_HASHTAGS = [
    "여성패션", "봄코디", "여름코디", "가을코디", "겨울코디",
    "데일리룩", "출근룩", "데이트룩", "캐주얼룩",
    "미니스커트", "크롭탑", "와이드팬츠", "원피스코디",
    "무신사", "지그재그",
    "ootd", "kfashion", "koreanfashion",
    "트렌드", "신상", "봄신상",
]

# --- Category normalization ---

CATEGORY_MAP = {
    "아우터": "outer",
    "상의": "top",
    "바지": "bottom",
    "원피스": "dress",
    "스커트": "skirt",
    "가방": "bag",
    "신발": "shoes",
    "악세서리": "accessory",
    "주얼리": "jewelry",
}

# --- Schedule ---

SCRAPE_TIMES = ["06:00", "18:00"]  # KST
