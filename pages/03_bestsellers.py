"""베스트셀러 대시보드 — 상품 카드 & 브랜드 분석."""
from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ui_theme import (
    get_conn, platform_name, format_price, style_chart,
    product_card_html, section_header,
    PLATFORM_COLORS, PLATFORM_DISPLAY, CHART_COLORS,
)

PLATFORM_LABELS = {
    "전체": None,
    "무신사": "musinsa",
    "29CM": "twentynine_cm",
    "W컨셉": "wconcept",
    "지그재그": "zigzag",
}

ITEMS_PER_PAGE = 20

# ---------------------------------------------------------------------------
# 캐시 쿼리
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300)
def get_snapshot_dates() -> list:
    conn = get_conn()
    df = pd.read_sql_query(
        "SELECT DISTINCT snapshot_date FROM bestseller_rankings ORDER BY snapshot_date DESC", conn,
    )
    conn.close()
    return df["snapshot_date"].tolist()


@st.cache_data(ttl=300)
def get_bestsellers_full(snapshot_date: str, platform) -> pd.DataFrame:
    conn = get_conn()
    if platform:
        df = pd.read_sql_query(
            """
            SELECT rank, brand, product_name, price, original_price, discount_pct,
                   platform, image_url, product_url
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
            SELECT rank, brand, product_name, price, original_price, discount_pct,
                   platform, image_url, product_url
            FROM bestseller_rankings
            WHERE snapshot_date = ?
            ORDER BY rank, platform
            """,
            conn,
            params=(snapshot_date,),
        )
    conn.close()
    return df


@st.cache_data(ttl=300)
def get_top_brands(snapshot_date: str, limit: int = 10) -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query(
        """
        SELECT brand, COUNT(*) AS cnt, GROUP_CONCAT(DISTINCT platform) AS platforms
        FROM bestseller_rankings
        WHERE snapshot_date = ? AND brand IS NOT NULL AND brand != ''
        GROUP BY brand
        ORDER BY cnt DESC
        LIMIT ?
        """,
        conn,
        params=(snapshot_date, limit),
    )
    conn.close()
    return df


@st.cache_data(ttl=300)
def get_avg_price_by_platform(snapshot_date: str) -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query(
        """
        SELECT platform, ROUND(AVG(price)) AS avg_price,
               ROUND(AVG(CASE WHEN discount_pct > 0 THEN discount_pct END), 1) AS avg_discount,
               COUNT(*) AS cnt
        FROM bestseller_rankings
        WHERE snapshot_date = ? AND price IS NOT NULL AND price > 0
        GROUP BY platform
        """,
        conn,
        params=(snapshot_date,),
    )
    conn.close()
    return df


@st.cache_data(ttl=300)
def get_discount_distribution(snapshot_date: str) -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query(
        """
        SELECT discount_pct, platform
        FROM bestseller_rankings
        WHERE snapshot_date = ? AND discount_pct IS NOT NULL AND discount_pct > 0
        """,
        conn,
        params=(snapshot_date,),
    )
    conn.close()
    return df


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

st.markdown("""
<div style="margin-bottom:8px;">
    <span style="font-size:2rem;font-weight:800;letter-spacing:-0.02em;">베스트셀러</span>
    <span style="font-size:0.85rem;opacity:0.4;margin-left:12px;">플랫폼별 인기 상품 탐색</span>
</div>
""", unsafe_allow_html=True)

dates = get_snapshot_dates()
if not dates:
    st.info("아직 베스트셀러 데이터가 없습니다.")
    st.stop()

# ── Filters ──

fcol1, fcol2 = st.columns([1, 1])
with fcol1:
    platform_label = st.selectbox("플랫폼", list(PLATFORM_LABELS.keys()))
with fcol2:
    selected_date = st.date_input(
        "수집일",
        value=datetime.strptime(dates[0], "%Y-%m-%d").date(),
        min_value=datetime.strptime(dates[-1], "%Y-%m-%d").date(),
        max_value=datetime.strptime(dates[0], "%Y-%m-%d").date(),
    )

platform_val = PLATFORM_LABELS[platform_label]
selected_date_str = selected_date.strftime("%Y-%m-%d")

bs_df = get_bestsellers_full(selected_date_str, platform_val)
if bs_df.empty:
    st.warning("선택한 필터에 해당하는 데이터가 없습니다.")
    st.stop()

# ── Summary metrics ──

total = len(bs_df)
avg_price = int(bs_df[bs_df["price"] > 0]["price"].mean()) if (bs_df["price"] > 0).any() else 0
avg_disc = round(bs_df[bs_df["discount_pct"] > 0]["discount_pct"].mean(), 1) if (bs_df["discount_pct"] > 0).any() else 0
n_brands = bs_df[bs_df["brand"].fillna("").str.strip() != ""]["brand"].nunique()

mcol1, mcol2, mcol3, mcol4 = st.columns(4)
mcol1.metric("총 상품", f"{total:,}개")
mcol2.metric("평균 가격", f"₩{avg_price:,}")
mcol3.metric("평균 할인율", f"{avg_disc}%")
mcol4.metric("브랜드 수", f"{n_brands}개")

# ── View toggle ──

section_header("🏆", "상품 순위")

view_mode = st.radio(
    "보기 모드",
    ["카드 뷰", "테이블 뷰"],
    horizontal=True,
    label_visibility="collapsed",
)

if view_mode == "카드 뷰":
    # Pagination
    page = st.number_input("페이지", min_value=1, max_value=max(1, (total - 1) // ITEMS_PER_PAGE + 1),
                           value=1, label_visibility="collapsed",
                           help=f"총 {total}개 상품, 페이지당 {ITEMS_PER_PAGE}개")
    start = (page - 1) * ITEMS_PER_PAGE
    end = min(start + ITEMS_PER_PAGE, total)
    page_df = bs_df.iloc[start:end]

    st.caption(f"{start+1}–{end} / {total}개 상품")

    # Render product cards in grid
    cols_per_row = 4
    for row_start in range(0, len(page_df), cols_per_row):
        cols = st.columns(cols_per_row)
        for j in range(cols_per_row):
            idx = row_start + j
            if idx >= len(page_df):
                break
            row = page_df.iloc[idx]
            with cols[j]:
                card = product_card_html(
                    rank=row["rank"],
                    brand=row["brand"],
                    name=row["product_name"],
                    price=row["price"],
                    original_price=row.get("original_price"),
                    discount_pct=row["discount_pct"],
                    image_url=row.get("image_url"),
                    platform=row["platform"],
                    product_url=row.get("product_url", ""),
                )
                st.markdown(card, unsafe_allow_html=True)
                st.markdown("")  # spacing

else:
    # Table view
    table_df = bs_df[["rank", "brand", "product_name", "price", "discount_pct", "platform"]].copy()
    table_df["platform"] = table_df["platform"].apply(platform_name)
    table_df.columns = ["순위", "브랜드", "상품명", "가격", "할인율(%)", "플랫폼"]
    st.dataframe(
        table_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "가격": st.column_config.NumberColumn("가격", format="₩%d"),
        },
    )

# CSV download
csv = bs_df[["rank", "brand", "product_name", "price", "original_price", "discount_pct", "platform"]].copy()
csv["platform"] = csv["platform"].apply(platform_name)
csv.columns = ["순위", "브랜드", "상품명", "가격", "원래 가격", "할인율(%)", "플랫폼"]
csv_data = csv.to_csv(index=False).encode("utf-8-sig")
st.download_button(
    label="📥 CSV 다운로드",
    data=csv_data,
    file_name=f"베스트셀러_{selected_date_str}.csv",
    mime="text/csv",
)

# ── Top Brands ──

st.divider()
section_header("👑", f"상위 브랜드 ({selected_date_str})")

brands = get_top_brands(selected_date_str, 15)
if not brands.empty:
    brands["platforms_display"] = brands["platforms"].apply(
        lambda x: ", ".join(platform_name(p.strip()) for p in x.split(","))
    )
    fig = px.bar(
        brands,
        x="brand",
        y="cnt",
        text_auto=True,
        color="cnt",
        color_continuous_scale=["#c7d2fe", "#6366f1"],
        hover_data={"brand": True, "cnt": True, "platforms_display": True},
        labels={"brand": "", "cnt": "등장 횟수", "platforms_display": "플랫폼"},
    )
    fig.update_traces(textposition="outside")
    fig.update_layout(xaxis_tickangle=-45, showlegend=False)
    fig.update_coloraxes(showscale=False)
    style_chart(fig, height=400)
    st.plotly_chart(fig, use_container_width=True)

# ── Price & Discount comparison ──

st.divider()

col_price, col_disc = st.columns(2)

with col_price:
    section_header("💰", "플랫폼별 평균 가격")
    avg_price_df = get_avg_price_by_platform(selected_date_str)
    if not avg_price_df.empty:
        avg_price_df["display"] = avg_price_df["platform"].apply(platform_name)
        fig = px.bar(
            avg_price_df,
            x="display",
            y="avg_price",
            color="display",
            text_auto=True,
            color_discrete_sequence=list(PLATFORM_COLORS.values()),
            labels={"display": "", "avg_price": "평균 가격 (원)"},
        )
        fig.update_traces(texttemplate="₩%{y:,.0f}", textposition="outside")
        fig.update_layout(showlegend=False)
        style_chart(fig, height=380)
        st.plotly_chart(fig, use_container_width=True)

with col_disc:
    section_header("🏷️", "할인율 분포")
    disc = get_discount_distribution(selected_date_str)
    if not disc.empty:
        disc["platform_display"] = disc["platform"].apply(platform_name)
        fig = px.histogram(
            disc,
            x="discount_pct",
            color="platform_display",
            nbins=20,
            barmode="overlay",
            opacity=0.7,
            color_discrete_sequence=list(PLATFORM_COLORS.values()),
            labels={"discount_pct": "할인율 (%)", "platform_display": "플랫폼"},
        )
        style_chart(fig, height=380)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("할인 데이터가 없습니다.")
