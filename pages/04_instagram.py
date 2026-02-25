"""인스타그램 해시태그 대시보드."""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ui_theme import get_conn, style_chart, hero_card, section_header, CHART_COLORS

# ---------------------------------------------------------------------------
# 캐시 쿼리
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300)
def get_latest_metrics() -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query(
        """
        SELECT hashtag, post_count, snapshot_date
        FROM instagram_metrics
        WHERE id IN (
            SELECT MAX(id) FROM instagram_metrics GROUP BY hashtag
        )
        ORDER BY post_count DESC
        """,
        conn,
    )
    conn.close()
    return df


@st.cache_data(ttl=300)
def get_hashtag_history(hashtags: tuple) -> pd.DataFrame:
    conn = get_conn()
    placeholders = ",".join("?" for _ in hashtags)
    df = pd.read_sql_query(
        f"""
        SELECT hashtag, post_count, snapshot_date
        FROM instagram_metrics
        WHERE hashtag IN ({placeholders})
        ORDER BY snapshot_date
        """,
        conn,
        params=hashtags,
    )
    conn.close()
    return df


@st.cache_data(ttl=300)
def get_all_hashtags() -> list:
    conn = get_conn()
    df = pd.read_sql_query(
        "SELECT DISTINCT hashtag FROM instagram_metrics ORDER BY hashtag", conn
    )
    conn.close()
    return df["hashtag"].tolist()


@st.cache_data(ttl=300)
def get_growth_data() -> pd.DataFrame:
    """Get growth % for each hashtag (latest vs previous snapshot)."""
    conn = get_conn()
    df = pd.read_sql_query(
        """
        WITH ranked AS (
            SELECT hashtag, post_count, snapshot_date,
                   ROW_NUMBER() OVER (PARTITION BY hashtag ORDER BY snapshot_date DESC) AS rn
            FROM instagram_metrics
        )
        SELECT
            a.hashtag,
            a.post_count AS current_count,
            b.post_count AS prev_count,
            a.snapshot_date
        FROM ranked a
        LEFT JOIN ranked b ON a.hashtag = b.hashtag AND b.rn = 2
        WHERE a.rn = 1
        ORDER BY a.post_count DESC
        """,
        conn,
    )
    conn.close()
    if not df.empty and "prev_count" in df.columns:
        df["growth_pct"] = df.apply(
            lambda r: round((r["current_count"] - r["prev_count"]) / r["prev_count"] * 100, 1)
            if r["prev_count"] and r["prev_count"] > 0 else None,
            axis=1,
        )
    return df


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

st.markdown("""
<div style="margin-bottom:8px;">
    <span style="font-size:2rem;font-weight:800;letter-spacing:-0.02em;">인스타그램</span>
    <span style="font-size:0.85rem;opacity:0.4;margin-left:12px;">해시태그 트렌드 모니터링</span>
</div>
""", unsafe_allow_html=True)

latest = get_latest_metrics()

if latest.empty:
    st.info(
        "아직 인스타그램 데이터가 수집되지 않았습니다. "
        "인스타그램 스크래퍼가 실행되면 해시태그 게시물 수가 여기에 표시됩니다."
    )
    st.stop()

growth_df = get_growth_data()

# ── Top hashtag hero cards ──

section_header("📸", "인기 해시태그")

top_n = min(4, len(latest))
cols = st.columns(top_n)
for i in range(top_n):
    row = latest.iloc[i]
    tag = row["hashtag"]
    count = int(row["post_count"])

    # Get growth if available
    growth_text = ""
    if not growth_df.empty:
        g_row = growth_df[growth_df["hashtag"] == tag]
        if not g_row.empty and g_row.iloc[0]["growth_pct"] is not None:
            g = g_row.iloc[0]["growth_pct"]
            if g > 0:
                growth_text = f"▲ {g}% 증가"
            elif g < 0:
                growth_text = f"▼ {abs(g)}% 감소"

    with cols[i]:
        st.markdown(hero_card(
            f"#{tag}",
            f"{count:,}",
            growth_text or f"수집일: {row['snapshot_date']}",
        ), unsafe_allow_html=True)

# ── Full hashtag table ──

st.markdown("")
section_header("📋", "전체 해시태그 현황")

display_df = latest.copy()
display_df.columns = ["해시태그", "게시물 수", "수집일"]
display_df["해시태그"] = display_df["해시태그"].apply(lambda x: f"#{x}")
st.dataframe(
    display_df,
    use_container_width=True,
    hide_index=True,
    column_config={
        "게시물 수": st.column_config.NumberColumn("게시물 수", format="%d"),
    },
)

# ── Trend chart ──

st.divider()
section_header("📈", "게시물 수 추이")

all_tags = get_all_hashtags()
selected_tags = st.multiselect(
    "해시태그 선택",
    all_tags,
    default=all_tags[:5],
    label_visibility="collapsed",
    help="추이를 확인할 해시태그를 선택하세요",
)

if selected_tags:
    history = get_hashtag_history(tuple(selected_tags))
    if history.empty:
        st.info("선택한 해시태그의 과거 데이터가 아직 없습니다.")
    else:
        history.columns = ["해시태그", "게시물 수", "수집일"]
        fig = px.area(
            history,
            x="수집일",
            y="게시물 수",
            color="해시태그",
            markers=True,
            color_discrete_sequence=CHART_COLORS,
        )
        fig.update_layout(
            xaxis_title="",
            yaxis_title="게시물 수",
            legend=dict(orientation="h", y=-0.15),
        )
        fig.update_traces(line=dict(width=2.5))
        style_chart(fig, height=440)
        st.plotly_chart(fig, use_container_width=True)
else:
    st.info("차트를 보려면 해시태그를 하나 이상 선택하세요.")

# ── Bar chart comparison ──

if len(latest) > 1:
    st.divider()
    section_header("📊", "해시태그 게시물 수 비교")

    fig = px.bar(
        latest.head(15),
        x="hashtag",
        y="post_count",
        color="post_count",
        color_continuous_scale=["#fecdd3", "#e11d48"],
        text_auto=True,
        labels={"hashtag": "", "post_count": "게시물 수"},
    )
    fig.update_traces(texttemplate="%{y:,}", textposition="outside")
    fig.update_layout(xaxis_tickangle=-45, showlegend=False)
    fig.update_coloraxes(showscale=False)
    style_chart(fig, height=400)
    st.plotly_chart(fig, use_container_width=True)
