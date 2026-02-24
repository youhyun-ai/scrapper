"""플랫폼 비교 대시보드."""
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
def get_cross_platform_brands() -> pd.DataFrame:
    conn = _conn()
    df = pd.read_sql_query(
        """
        SELECT brand AS '브랜드',
               GROUP_CONCAT(DISTINCT platform) AS '등록 플랫폼',
               COUNT(DISTINCT platform) AS '플랫폼 수'
        FROM bestseller_rankings
        WHERE brand IS NOT NULL AND brand != ''
        GROUP BY brand
        HAVING COUNT(DISTINCT platform) > 1
        ORDER BY COUNT(DISTINCT platform) DESC, brand
        """,
        conn,
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
        ORDER BY AVG(price) DESC
        """,
        conn,
    )
    conn.close()
    return df


@st.cache_data(ttl=300)
def get_avg_discount_by_platform() -> pd.DataFrame:
    conn = _conn()
    df = pd.read_sql_query(
        """
        SELECT platform AS '플랫폼', ROUND(AVG(discount_pct), 1) AS '평균 할인율'
        FROM bestseller_rankings
        WHERE discount_pct IS NOT NULL AND discount_pct > 0
        GROUP BY platform
        ORDER BY AVG(discount_pct) DESC
        """,
        conn,
    )
    conn.close()
    return df


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

st.header("플랫폼 비교")

# --- 다중 플랫폼 브랜드 ---
st.subheader("다중 플랫폼 등록 브랜드")
cross_brands = get_cross_platform_brands()
if cross_brands.empty:
    st.info("아직 여러 플랫폼에 등록된 브랜드가 없습니다.")
else:
    st.dataframe(cross_brands, use_container_width=True, hide_index=True)
    csv = cross_brands.to_csv(index=False).encode("utf-8-sig")
    st.download_button(
        label="CSV 다운로드",
        data=csv,
        file_name="다중플랫폼_브랜드.csv",
        mime="text/csv",
    )

# --- 플랫폼별 평균 가격 ---
st.subheader("플랫폼별 평균 가격")
avg_price = get_avg_price_by_platform()
if avg_price.empty:
    st.info("가격 데이터가 없습니다.")
else:
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

# --- 플랫폼별 평균 할인율 ---
st.subheader("플랫폼별 평균 할인율")
avg_disc = get_avg_discount_by_platform()
if avg_disc.empty:
    st.info("할인 데이터가 없습니다.")
else:
    fig = px.bar(
        avg_disc,
        x="플랫폼",
        y="평균 할인율",
        color="플랫폼",
        text_auto=True,
        title="플랫폼별 평균 할인율 (%)",
    )
    fig.update_layout(showlegend=False)
    st.plotly_chart(fig, use_container_width=True)
