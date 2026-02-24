"""베스트셀러 대시보드."""
from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "trends.db"

PLATFORM_LABELS = {
    "전체": None,
    "무신사": "musinsa",
    "29CM": "twentynine_cm",
    "W컨셉": "wconcept",
    "지그재그": "zigzag",
}


def _conn() -> sqlite3.Connection:
    return sqlite3.connect(str(DB_PATH))


# ---------------------------------------------------------------------------
# 캐시 쿼리
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300)
def get_snapshot_dates() -> list:
    conn = _conn()
    df = pd.read_sql_query(
        "SELECT DISTINCT snapshot_date FROM bestseller_rankings ORDER BY snapshot_date DESC",
        conn,
    )
    conn.close()
    return df["snapshot_date"].tolist()


@st.cache_data(ttl=300)
def get_bestsellers(snapshot_date: str, platform) -> pd.DataFrame:
    conn = _conn()
    if platform:
        df = pd.read_sql_query(
            """
            SELECT rank AS '순위', brand AS '브랜드', product_name AS '상품명',
                   price AS '가격', discount_pct AS '할인율(%)', platform AS '플랫폼'
            FROM bestseller_rankings
            WHERE snapshot_date = ? AND platform = ?
            ORDER BY rank
            """,
            conn,
            params=(snapshot_date, platform),
        )
    else:
        df = pd.read_sql_query(
            """
            SELECT rank AS '순위', brand AS '브랜드', product_name AS '상품명',
                   price AS '가격', discount_pct AS '할인율(%)', platform AS '플랫폼'
            FROM bestseller_rankings
            WHERE snapshot_date = ?
            ORDER BY platform, rank
            """,
            conn,
            params=(snapshot_date,),
        )
    conn.close()
    return df


@st.cache_data(ttl=300)
def get_top_brands(limit: int = 10) -> pd.DataFrame:
    conn = _conn()
    df = pd.read_sql_query(
        """
        SELECT brand AS '브랜드', COUNT(*) AS '등장 횟수'
        FROM bestseller_rankings
        WHERE brand IS NOT NULL AND brand != ''
        GROUP BY brand
        ORDER BY COUNT(*) DESC
        LIMIT ?
        """,
        conn,
        params=(limit,),
    )
    conn.close()
    return df


@st.cache_data(ttl=300)
def get_avg_price_by_platform() -> pd.DataFrame:
    conn = _conn()
    df = pd.read_sql_query(
        """
        SELECT platform AS '플랫폼', ROUND(AVG(price)) AS '평균 가격'
        FROM bestseller_rankings
        WHERE price IS NOT NULL AND price > 0
        GROUP BY platform
        """,
        conn,
    )
    conn.close()
    return df


@st.cache_data(ttl=300)
def get_discount_distribution() -> pd.DataFrame:
    conn = _conn()
    df = pd.read_sql_query(
        """
        SELECT discount_pct
        FROM bestseller_rankings
        WHERE discount_pct IS NOT NULL AND discount_pct > 0
        """,
        conn,
    )
    conn.close()
    return df


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

st.header("베스트셀러")

dates = get_snapshot_dates()
if not dates:
    st.info("아직 베스트셀러 데이터가 없습니다.")
    st.stop()

col_a, col_b = st.columns(2)
with col_a:
    platform_label = st.selectbox("플랫폼", list(PLATFORM_LABELS.keys()))
with col_b:
    selected_date = st.date_input(
        "수집일",
        value=datetime.strptime(dates[0], "%Y-%m-%d").date(),
        min_value=datetime.strptime(dates[-1], "%Y-%m-%d").date(),
        max_value=datetime.strptime(dates[0], "%Y-%m-%d").date(),
    )

platform_val = PLATFORM_LABELS[platform_label]
selected_date_str = selected_date.strftime("%Y-%m-%d")

bs_df = get_bestsellers(selected_date_str, platform_val)
if bs_df.empty:
    st.warning("선택한 필터에 해당하는 데이터가 없습니다.")
    st.stop()

st.subheader("상품 순위")
st.dataframe(bs_df, use_container_width=True, hide_index=True)

# --- 상위 10개 브랜드 ---
st.subheader("상위 10개 브랜드 (전체 플랫폼, 전체 기간)")
brands = get_top_brands(10)
if not brands.empty:
    fig = px.bar(
        brands,
        x="브랜드",
        y="등장 횟수",
        text_auto=True,
        title="베스트셀러 목록 내 브랜드 등장 빈도",
    )
    fig.update_layout(xaxis_tickangle=-45)
    st.plotly_chart(fig, use_container_width=True)

# --- 플랫폼별 평균 가격 ---
st.subheader("플랫폼별 평균 가격")
avg_price = get_avg_price_by_platform()
if not avg_price.empty:
    fig = px.bar(
        avg_price,
        x="플랫폼",
        y="평균 가격",
        color="플랫폼",
        text_auto=True,
        title="플랫폼별 평균 상품 가격 (원)",
    )
    fig.update_layout(showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

# --- 할인율 분포 ---
st.subheader("할인율 분포")
disc = get_discount_distribution()
if disc.empty:
    st.info("할인 데이터가 없습니다.")
else:
    fig = px.histogram(
        disc,
        x="discount_pct",
        nbins=20,
        title="할인율 분포",
        labels={"discount_pct": "할인율 (%)"},
    )
    st.plotly_chart(fig, use_container_width=True)
