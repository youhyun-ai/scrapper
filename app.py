"""패션 트렌드 분석기 — Streamlit 메인 진입점."""
from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

st.set_page_config(
    page_title="패션 트렌드 분석기",
    page_icon="👗",
    layout="wide",
    initial_sidebar_state="expanded",
)

# 프로젝트 루트를 sys.path에 추가 (배포 환경 대응)
ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from database.db import init_db
from ui_theme import inject_global_css

# DB가 없으면 빈 테이블 생성
init_db()

# 글로벌 CSS 주입
inject_global_css()

# 사이드바 브랜딩
with st.sidebar:
    st.markdown("""
    <div style="padding:8px 0 16px 0;">
        <div style="font-size:1.5rem;font-weight:800;letter-spacing:-0.02em;">
            👗 Fashion Pulse
        </div>
        <div style="font-size:0.8rem;opacity:0.5;margin-top:2px;">
            실시간 패션 트렌드 분석 대시보드
        </div>
    </div>
    """, unsafe_allow_html=True)

home = st.Page("pages/01_overview.py", title="대시보드", icon="📊", default=True)
keywords = st.Page("pages/02_keywords.py", title="키워드 분석", icon="🔍")
bestsellers = st.Page("pages/03_bestsellers.py", title="베스트셀러", icon="🏆")
instagram = st.Page("pages/04_instagram.py", title="인스타그램", icon="📷")
compare = st.Page("pages/05_compare.py", title="플랫폼 비교", icon="⚖️")
analysis = st.Page("pages/06_analysis.py", title="데이터 인사이트", icon="💡")

pg = st.navigation([home, keywords, bestsellers, instagram, compare, analysis])
pg.run()
