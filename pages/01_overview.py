"""개요 대시보드 — 주요 지표, 스크래퍼 상태, 플랫폼별 현황."""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "trends.db"


def _conn() -> sqlite3.Connection:
    return sqlite3.connect(str(DB_PATH))


# ---------------------------------------------------------------------------
# 캐시 쿼리
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300)
def get_kpi_data() -> dict:
    conn = _conn()
    total_products = pd.read_sql_query(
        "SELECT COUNT(DISTINCT product_name) AS n FROM bestseller_rankings", conn
    ).iloc[0]["n"]
    total_keywords = pd.read_sql_query(
        "SELECT COUNT(DISTINCT keyword) AS n FROM keyword_rankings", conn
    ).iloc[0]["n"]
    active_platforms = pd.read_sql_query(
        "SELECT COUNT(DISTINCT platform) AS n FROM bestseller_rankings", conn
    ).iloc[0]["n"]
    conn.close()
    return {
        "total_products": int(total_products),
        "total_keywords": int(total_keywords),
        "active_platforms": int(active_platforms),
    }


@st.cache_data(ttl=300)
def get_scrape_log() -> pd.DataFrame:
    conn = _conn()
    df = pd.read_sql_query(
        """
        SELECT platform AS '플랫폼', status AS '상태',
               items_collected AS '수집 항목', error_message AS '오류 메시지',
               ROUND(duration_seconds, 2) AS '소요 시간(초)', scraped_at AS '수집 시각'
        FROM scrape_log
        WHERE id IN (
            SELECT MAX(id) FROM scrape_log GROUP BY platform
        )
        ORDER BY scraped_at DESC
        """,
        conn,
    )
    conn.close()
    return df


@st.cache_data(ttl=300)
def get_platform_breakdown() -> pd.DataFrame:
    conn = _conn()
    df = pd.read_sql_query(
        """
        SELECT platform AS '플랫폼', COUNT(*) AS '상품 수'
        FROM bestseller_rankings
        GROUP BY platform
        ORDER BY COUNT(*) DESC
        """,
        conn,
    )
    conn.close()
    return df


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

st.header("개요")

kpi = get_kpi_data()
log_df = get_scrape_log()

last_status = "N/A"
if not log_df.empty:
    statuses = log_df["상태"].unique()
    last_status = "정상" if all(s == "success" for s in statuses) else "오류 감지"

col1, col2, col3, col4 = st.columns(4)
col1.metric("추적 상품 수", kpi["total_products"])
col2.metric("추적 키워드 수", kpi["total_keywords"])
col3.metric("활성 플랫폼", kpi["active_platforms"])
col4.metric("최근 스크래핑 상태", last_status)

st.subheader("스크래퍼 상태")
if log_df.empty:
    st.info("아직 스크래핑 로그가 없습니다.")
else:
    st.dataframe(log_df, use_container_width=True, hide_index=True)

st.subheader("플랫폼별 상품 수")
breakdown = get_platform_breakdown()
if breakdown.empty:
    st.info("아직 베스트셀러 데이터가 없습니다.")
else:
    fig = px.bar(
        breakdown,
        x="플랫폼",
        y="상품 수",
        color="플랫폼",
        text_auto=True,
        title="플랫폼별 베스트셀러 상품 수",
    )
    fig.update_layout(showlegend=False)
    st.plotly_chart(fig, use_container_width=True)
