"""대시보드 — 주요 지표, 스크래퍼 상태, 플랫폼별 현황."""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ui_theme import (
    get_conn, platform_name, format_price, style_chart, hero_card,
    status_badge, section_header, PLATFORM_COLORS, CHART_COLORS,
)

# ---------------------------------------------------------------------------
# 캐시 쿼리
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300)
def get_kpi_data() -> dict:
    conn = get_conn()
    total_products = pd.read_sql_query(
        "SELECT COUNT(DISTINCT product_name) AS n FROM bestseller_rankings", conn
    ).iloc[0]["n"]
    total_keywords = pd.read_sql_query(
        "SELECT COUNT(DISTINCT keyword) AS n FROM keyword_rankings", conn
    ).iloc[0]["n"]
    active_platforms = pd.read_sql_query(
        "SELECT COUNT(DISTINCT platform) AS n FROM bestseller_rankings", conn
    ).iloc[0]["n"]
    total_brands = pd.read_sql_query(
        "SELECT COUNT(DISTINCT brand) AS n FROM bestseller_rankings WHERE brand IS NOT NULL AND brand != ''", conn
    ).iloc[0]["n"]
    conn.close()
    return {
        "total_products": int(total_products),
        "total_keywords": int(total_keywords),
        "active_platforms": int(active_platforms),
        "total_brands": int(total_brands),
    }


@st.cache_data(ttl=300)
def get_scrape_log() -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query(
        """
        SELECT platform, status, items_collected,
               error_message, ROUND(duration_seconds, 2) AS duration,
               scraped_at
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
    conn = get_conn()
    df = pd.read_sql_query(
        """
        SELECT platform, COUNT(*) AS cnt,
               COUNT(DISTINCT brand) AS brands,
               ROUND(AVG(CASE WHEN price > 0 THEN price END)) AS avg_price,
               ROUND(AVG(CASE WHEN discount_pct > 0 THEN discount_pct END), 1) AS avg_discount
        FROM bestseller_rankings
        GROUP BY platform
        ORDER BY cnt DESC
        """,
        conn,
    )
    conn.close()
    return df


@st.cache_data(ttl=300)
def get_today_highlights() -> dict:
    conn = get_conn()
    # Most discounted product today (latest snapshot)
    top_discount = pd.read_sql_query(
        """
        SELECT brand, product_name, discount_pct, platform
        FROM bestseller_rankings
        WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM bestseller_rankings)
          AND discount_pct IS NOT NULL
        ORDER BY discount_pct DESC
        LIMIT 1
        """,
        conn,
    )
    # Top brand (most appearances in latest snapshot)
    top_brand = pd.read_sql_query(
        """
        SELECT brand, COUNT(*) AS cnt
        FROM bestseller_rankings
        WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM bestseller_rankings)
          AND brand IS NOT NULL AND brand != ''
        GROUP BY brand
        ORDER BY cnt DESC
        LIMIT 1
        """,
        conn,
    )
    conn.close()
    result = {}
    if not top_discount.empty:
        r = top_discount.iloc[0]
        result["top_discount"] = {
            "brand": r["brand"],
            "name": r["product_name"],
            "pct": int(r["discount_pct"]),
            "platform": r["platform"],
        }
    if not top_brand.empty:
        r = top_brand.iloc[0]
        result["top_brand"] = {"brand": r["brand"], "count": int(r["cnt"])}
    return result


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

st.markdown("""
<div style="margin-bottom:8px;">
    <span style="font-size:2rem;font-weight:800;letter-spacing:-0.02em;">대시보드</span>
    <span style="font-size:0.85rem;opacity:0.4;margin-left:12px;">실시간 패션 마켓 현황</span>
</div>
""", unsafe_allow_html=True)

kpi = get_kpi_data()
log_df = get_scrape_log()
highlights = get_today_highlights()

# ── KPI Hero Cards ──

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.markdown(hero_card("추적 상품", f"{kpi['total_products']:,}개", "전체 플랫폼 누적"), unsafe_allow_html=True)
with col2:
    st.markdown(hero_card("추적 브랜드", f"{kpi['total_brands']:,}개", "고유 브랜드 수"), unsafe_allow_html=True)
with col3:
    st.markdown(hero_card("트렌드 키워드", f"{kpi['total_keywords']:,}개", "무신사 인기 검색어"), unsafe_allow_html=True)
with col4:
    st.markdown(hero_card("활성 플랫폼", f"{kpi['active_platforms']}개", "무신사·29CM·W컨셉·지그재그"), unsafe_allow_html=True)


# ── Today's Highlights ──

if highlights:
    st.markdown("")
    hcol1, hcol2 = st.columns(2)
    if "top_brand" in highlights:
        b = highlights["top_brand"]
        with hcol1:
            st.markdown(f"""
            <div class="insight-card">
                <p>🏅 <strong>오늘의 TOP 브랜드</strong>: <strong>{b['brand']}</strong> — 베스트셀러 {b['count']}회 등장</p>
            </div>
            """, unsafe_allow_html=True)
    if "top_discount" in highlights:
        d = highlights["top_discount"]
        with hcol2:
            st.markdown(f"""
            <div class="insight-card">
                <p>🔥 <strong>최대 할인</strong>: {d['brand']} — {d['name'][:30]}{'...' if len(d['name']) > 30 else ''} <strong>{d['pct']}% OFF</strong> ({platform_name(d['platform'])})</p>
            </div>
            """, unsafe_allow_html=True)


# ── Scraper Status ──

section_header("⚡", "스크래퍼 상태")

if log_df.empty:
    st.info("아직 스크래핑 로그가 없습니다.")
else:
    # Show last update time
    last_time = log_df["scraped_at"].max()
    st.caption(f"마지막 업데이트: {last_time}")

    cols = st.columns(len(log_df))
    for i, row in log_df.iterrows():
        with cols[i]:
            badge = status_badge(row["status"])
            plat = platform_name(row["platform"])
            items = int(row["items_collected"]) if row["items_collected"] else 0
            duration = row["duration"] if row["duration"] else 0
            st.markdown(f"""
            <div style="background:rgba(128,128,128,0.04);border:1px solid rgba(128,128,128,0.1);border-radius:12px;padding:16px;text-align:center;">
                <div style="font-weight:700;font-size:1rem;margin-bottom:8px;">{plat}</div>
                {badge}
                <div style="margin-top:10px;font-size:0.82rem;opacity:0.6;">
                    {items:,}개 수집 · {duration}초
                </div>
            </div>
            """, unsafe_allow_html=True)

    # Error detail if any failed
    failed = log_df[log_df["status"] != "success"]
    if not failed.empty:
        with st.expander("오류 상세"):
            for _, row in failed.iterrows():
                st.error(f"**{platform_name(row['platform'])}**: {row['error_message']}")


# ── Platform Overview ──

section_header("📊", "플랫폼별 현황")

breakdown = get_platform_breakdown()
if breakdown.empty:
    st.info("아직 베스트셀러 데이터가 없습니다.")
else:
    # Platform stat cards
    pcols = st.columns(len(breakdown))
    for i, row in breakdown.iterrows():
        plat = row["platform"]
        color = PLATFORM_COLORS.get(plat, "#6366f1")
        with pcols[i]:
            st.markdown(f"""
            <div style="border-left:4px solid {color};padding:12px 16px;border-radius:0 10px 10px 0;background:rgba(128,128,128,0.03);">
                <div style="font-weight:700;font-size:1.05rem;">{platform_name(plat)}</div>
                <div style="font-size:1.4rem;font-weight:800;margin:6px 0;">{int(row['cnt']):,}개</div>
                <div style="font-size:0.78rem;opacity:0.55;">
                    {int(row['brands'])}개 브랜드 · 평균 ₩{int(row['avg_price']):,}
                </div>
                <div style="font-size:0.78rem;opacity:0.55;">
                    평균 할인 {row['avg_discount']}%
                </div>
            </div>
            """, unsafe_allow_html=True)

    st.markdown("")

    # Bar chart
    breakdown["display_name"] = breakdown["platform"].apply(platform_name)
    fig = px.bar(
        breakdown,
        x="display_name",
        y="cnt",
        color="display_name",
        text_auto=True,
        color_discrete_sequence=list(PLATFORM_COLORS.values()),
    )
    fig.update_layout(
        showlegend=False,
        xaxis_title="",
        yaxis_title="상품 수",
    )
    fig.update_traces(
        textposition="outside",
        texttemplate="%{y:,}",
    )
    style_chart(fig, height=360)
    st.plotly_chart(fig, use_container_width=True)
