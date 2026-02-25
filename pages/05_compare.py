"""플랫폼 비교 대시보드."""
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
    get_conn, platform_name, style_chart, section_header,
    PLATFORM_COLORS, CHART_COLORS,
)

# ---------------------------------------------------------------------------
# 캐시 쿼리
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300)
def get_cross_platform_brands() -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query(
        """
        SELECT brand,
               GROUP_CONCAT(DISTINCT platform) AS platforms,
               COUNT(DISTINCT platform) AS platform_count,
               COUNT(*) AS total_appearances,
               ROUND(AVG(CASE WHEN price > 0 THEN price END)) AS avg_price
        FROM bestseller_rankings
        WHERE brand IS NOT NULL AND brand != ''
        GROUP BY brand
        HAVING COUNT(DISTINCT platform) > 1
        ORDER BY COUNT(DISTINCT platform) DESC, COUNT(*) DESC
        """,
        conn,
    )
    conn.close()
    return df


@st.cache_data(ttl=300)
def get_platform_stats() -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query(
        """
        SELECT platform,
               COUNT(*) AS product_count,
               COUNT(DISTINCT brand) AS brand_count,
               ROUND(AVG(CASE WHEN price > 0 THEN price END)) AS avg_price,
               ROUND(AVG(CASE WHEN discount_pct > 0 THEN discount_pct END), 1) AS avg_discount,
               ROUND(MIN(CASE WHEN price > 0 THEN price END)) AS min_price,
               ROUND(MAX(price)) AS max_price,
               ROUND(AVG(CASE WHEN price > 0 THEN price END) * 0 +
                     (SELECT AVG(price) FROM bestseller_rankings b2
                      WHERE b2.platform = bestseller_rankings.platform AND price > 0)) AS median_approx
        FROM bestseller_rankings
        GROUP BY platform
        ORDER BY product_count DESC
        """,
        conn,
    )
    conn.close()
    return df


@st.cache_data(ttl=300)
def get_price_distribution() -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query(
        """
        SELECT platform, price
        FROM bestseller_rankings
        WHERE price IS NOT NULL AND price > 0
        """,
        conn,
    )
    conn.close()
    return df


@st.cache_data(ttl=300)
def get_platform_category_overlap() -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query(
        """
        SELECT brand, platform, COUNT(*) AS cnt
        FROM bestseller_rankings
        WHERE brand IS NOT NULL AND brand != ''
        GROUP BY brand, platform
        """,
        conn,
    )
    conn.close()
    return df


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

st.markdown("""
<div style="margin-bottom:8px;">
    <span style="font-size:2rem;font-weight:800;letter-spacing:-0.02em;">플랫폼 비교</span>
    <span style="font-size:0.85rem;opacity:0.4;margin-left:12px;">플랫폼 간 포지셔닝 & 브랜드 분석</span>
</div>
""", unsafe_allow_html=True)

# ── Platform stat cards ──

section_header("📊", "플랫폼 개요")

stats = get_platform_stats()
if stats.empty:
    st.info("아직 데이터가 없습니다.")
    st.stop()

cols = st.columns(len(stats))
for i, row in stats.iterrows():
    plat = row["platform"]
    color = PLATFORM_COLORS.get(plat, "#6366f1")
    with cols[i]:
        st.markdown(f"""
        <div style="border-top:4px solid {color};padding:16px;border-radius:12px;background:rgba(128,128,128,0.03);text-align:center;">
            <div style="font-weight:800;font-size:1.1rem;margin-bottom:12px;">{platform_name(plat)}</div>
            <div style="font-size:2rem;font-weight:800;color:{color};">{int(row['product_count']):,}</div>
            <div style="font-size:0.75rem;opacity:0.5;margin-bottom:12px;">상품 수</div>
            <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;text-align:center;">
                <div>
                    <div style="font-size:1.1rem;font-weight:700;">{int(row['brand_count'])}</div>
                    <div style="font-size:0.7rem;opacity:0.5;">브랜드</div>
                </div>
                <div>
                    <div style="font-size:1.1rem;font-weight:700;">₩{int(row['avg_price']):,}</div>
                    <div style="font-size:0.7rem;opacity:0.5;">평균가</div>
                </div>
                <div>
                    <div style="font-size:1.1rem;font-weight:700;">{row['avg_discount']}%</div>
                    <div style="font-size:0.7rem;opacity:0.5;">평균 할인</div>
                </div>
                <div>
                    <div style="font-size:1.1rem;font-weight:700;">₩{int(row['min_price']):,}</div>
                    <div style="font-size:0.7rem;opacity:0.5;">최저가</div>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)

# ── Radar chart ──

st.divider()
section_header("🎯", "플랫폼 포지셔닝 비교")

if len(stats) >= 2:
    # Normalize stats for radar chart
    metrics = ["product_count", "brand_count", "avg_price", "avg_discount"]
    metric_labels = ["상품 수", "브랜드 수", "평균 가격", "평균 할인율"]

    fig = go.Figure()
    for _, row in stats.iterrows():
        plat = row["platform"]
        values = []
        for m in metrics:
            col_max = stats[m].max()
            values.append(row[m] / col_max * 100 if col_max > 0 else 0)
        values.append(values[0])  # close the radar
        fig.add_trace(go.Scatterpolar(
            r=values,
            theta=metric_labels + [metric_labels[0]],
            fill="toself",
            name=platform_name(plat),
            line=dict(color=PLATFORM_COLORS.get(plat, "#6366f1"), width=2),
            opacity=0.7,
        ))
    fig.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 110], showticklabels=False)),
        showlegend=True,
        legend=dict(orientation="h", y=-0.1),
    )
    style_chart(fig, height=450)
    st.plotly_chart(fig, use_container_width=True)

# ── Price box plot comparison ──

st.divider()
section_header("💰", "가격 분포 비교")

price_dist = get_price_distribution()
if not price_dist.empty:
    price_dist["platform_display"] = price_dist["platform"].apply(platform_name)
    fig = px.box(
        price_dist,
        x="platform_display",
        y="price",
        color="platform_display",
        color_discrete_sequence=list(PLATFORM_COLORS.values()),
        labels={"platform_display": "", "price": "가격 (원)"},
    )
    fig.update_layout(showlegend=False, yaxis_tickformat=",")
    style_chart(fig, height=420)
    st.plotly_chart(fig, use_container_width=True)

# ── Multi-platform brands ──

st.divider()
section_header("🔗", "다중 플랫폼 브랜드")
st.caption("2개 이상 플랫폼에 동시 등장하는 브랜드")

cross_brands = get_cross_platform_brands()
if cross_brands.empty:
    st.info("아직 여러 플랫폼에 등록된 브랜드가 없습니다.")
else:
    # Brand overlap heatmap
    brand_platform_df = get_platform_category_overlap()
    if not brand_platform_df.empty:
        # Create platform co-occurrence matrix
        platforms = sorted(brand_platform_df["platform"].unique())
        overlap_matrix = pd.DataFrame(0, index=[platform_name(p) for p in platforms],
                                      columns=[platform_name(p) for p in platforms])
        brand_platforms = brand_platform_df.groupby("brand")["platform"].apply(set)
        for brand_set in brand_platforms:
            for p1 in brand_set:
                for p2 in brand_set:
                    overlap_matrix.loc[platform_name(p1), platform_name(p2)] += 1

        fig = px.imshow(
            overlap_matrix,
            text_auto=True,
            color_continuous_scale=["#f0f0ff", "#6366f1"],
            labels={"color": "공유 브랜드 수"},
        )
        fig.update_layout(xaxis_title="", yaxis_title="")
        style_chart(fig, height=380)
        st.plotly_chart(fig, use_container_width=True)

    # Table
    display_brands = cross_brands.copy()
    display_brands["platforms"] = display_brands["platforms"].apply(
        lambda x: ", ".join(platform_name(p.strip()) for p in x.split(","))
    )
    display_brands["avg_price"] = display_brands["avg_price"].apply(
        lambda x: f"₩{int(x):,}" if x and x > 0 else "-"
    )
    display_brands.columns = ["브랜드", "등록 플랫폼", "플랫폼 수", "총 등장", "평균 가격"]
    st.dataframe(display_brands, use_container_width=True, hide_index=True)
    csv = display_brands.to_csv(index=False).encode("utf-8-sig")
    st.download_button(
        label="📥 CSV 다운로드",
        data=csv,
        file_name="다중플랫폼_브랜드.csv",
        mime="text/csv",
    )
