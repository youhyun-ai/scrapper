"""패션 트렌드 분석기 — Streamlit 메인 진입점."""
from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

# 프로젝트 루트를 sys.path에 추가 (배포 환경 대응)
ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from database.db import init_db

# DB가 없으면 빈 테이블 생성
init_db()

home = st.Page("pages/01_overview.py", title="개요", icon="\U0001F4CA", default=True)
keywords = st.Page("pages/02_keywords.py", title="키워드", icon="\U0001F50D")
bestsellers = st.Page("pages/03_bestsellers.py", title="베스트셀러", icon="\U0001F451")
instagram = st.Page("pages/04_instagram.py", title="인스타그램", icon="\U0001F4F7")
compare = st.Page("pages/05_compare.py", title="플랫폼 비교", icon="\U0001F504")

pg = st.navigation([home, keywords, bestsellers, instagram, compare])
pg.run()
