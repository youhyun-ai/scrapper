"""키워드 분석 대시보드."""
from __future__ import annotations

import re
import sqlite3
from collections import Counter
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ui_theme import (
    get_conn, platform_name, style_chart, hero_card, section_header,
    PLATFORM_COLORS, CHART_COLORS,
)

# ---------------------------------------------------------------------------
# 헬퍼
# ---------------------------------------------------------------------------

def _parse_fluctuation(cat):
    if not cat:
        return "-", "same"
    parts = cat.split(":")
    if len(parts) != 2:
        return cat, "same"
    direction, amount = parts[0], parts[1]
    if direction == "UP":
        return f"▲ {amount}", "up"
    if direction == "DOWN":
        return f"▼ {amount}", "down"
    return "-", "same"


TREND_KEYWORDS = [
    "크롭", "와이드", "오버사이즈", "슬림", "루즈", "배기", "미니", "롱", "숏",
    "하이웨이스트", "로우라이즈", "플레어", "A라인", "박시",
    "레이어드", "셔링", "플리츠", "프릴", "리본", "스트링", "컷아웃", "슬릿",
    "니트", "데님", "레더", "퍼", "트위드", "벨벳", "린넨", "코듀로이",
    "카디건", "블라우스", "후드", "맨투맨", "자켓", "패딩", "코트",
    "원피스", "스커트", "팬츠", "조거", "트랙", "바이커",
    "블랙", "화이트", "베이지", "그레이", "카키", "네이비", "브라운",
    "파스텔", "체크", "스트라이프", "플로럴", "도트",
    "빈티지", "레트로", "미니멀", "캐주얼", "스포티", "시티보이", "고프코어",
    "발레코어", "올드머니",
]


# ---------------------------------------------------------------------------
# 캐시 쿼리
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300)
def get_snapshot_dates() -> list:
    conn = get_conn()
    df = pd.read_sql_query(
        "SELECT DISTINCT snapshot_date FROM keyword_rankings ORDER BY snapshot_date DESC", conn,
    )
    conn.close()
    return df["snapshot_date"].tolist()


@st.cache_data(ttl=300)
def get_bestseller_dates() -> list:
    conn = get_conn()
    df = pd.read_sql_query(
        "SELECT DISTINCT snapshot_date FROM bestseller_rankings ORDER BY snapshot_date DESC", conn,
    )
    conn.close()
    return df["snapshot_date"].tolist()


@st.cache_data(ttl=300)
def get_keywords_for_date(snapshot_date: str) -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query(
        """
        SELECT rank, keyword, category, platform
        FROM keyword_rankings
        WHERE snapshot_date = ?
        ORDER BY rank
        """,
        conn,
        params=(snapshot_date,),
    )
    conn.close()
    return df


@st.cache_data(ttl=300)
def get_keyword_history(keyword: str) -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query(
        """
        SELECT snapshot_date, rank, platform
        FROM keyword_rankings
        WHERE keyword = ?
        ORDER BY snapshot_date
        """,
        conn,
        params=(keyword,),
    )
    conn.close()
    return df


@st.cache_data(ttl=300)
def get_product_keyword_counts(snapshot_date: str) -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query(
        "SELECT platform, product_name, rank FROM bestseller_rankings WHERE snapshot_date = ?",
        conn,
        params=(snapshot_date,),
    )
    conn.close()
    if df.empty:
        return pd.DataFrame()
    max_ranks = df.groupby("platform")["rank"].max().to_dict()
    rows = []
    for kw in TREND_KEYWORDS:
        for plat in df["platform"].unique():
            plat_df = df[df["platform"] == plat]
            max_rank = max_ranks[plat]
            hits = score = 0
            for row in plat_df.itertuples():
                if kw in row.product_name:
                    hits += 1
                    score += max_rank + 1 - row.rank
            if score > 0:
                rows.append({"keyword": kw, "platform": plat, "score": score, "hits": hits})
    return pd.DataFrame(rows)


@st.cache_data(ttl=300)
def get_product_keyword_totals(snapshot_date: str) -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query(
        "SELECT product_name, rank, platform FROM bestseller_rankings WHERE snapshot_date = ?",
        conn,
        params=(snapshot_date,),
    )
    conn.close()
    if df.empty:
        return pd.DataFrame()
    max_ranks = df.groupby("platform")["rank"].max().to_dict()
    scores: dict = {}
    hits: dict = {}
    for row in df.itertuples():
        weight = max_ranks[row.platform] + 1 - row.rank
        for kw in TREND_KEYWORDS:
            if kw in row.product_name:
                scores[kw] = scores.get(kw, 0) + weight
                hits[kw] = hits.get(kw, 0) + 1
    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:30]
    rows = [{"keyword": k, "score": v, "hits": hits[k]} for k, v in ranked]
    return pd.DataFrame(rows)


@st.cache_data(ttl=300)
def get_platform_counts(snapshot_date: str) -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query(
        """
        SELECT platform, COUNT(*) as items
        FROM bestseller_rankings
        WHERE snapshot_date = ?
        GROUP BY platform
        ORDER BY items DESC
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
    <span style="font-size:2rem;font-weight:800;letter-spacing:-0.02em;">키워드 분석</span>
    <span style="font-size:0.85rem;opacity:0.4;margin-left:12px;">트렌드 키워드 & 검색어 인사이트</span>
</div>
""", unsafe_allow_html=True)

bs_dates = get_bestseller_dates()
kw_dates = get_snapshot_dates()
all_dates = sorted(set(bs_dates + kw_dates), reverse=True)

if not all_dates:
    st.info("아직 데이터가 없습니다.")
    st.stop()

selected_date = st.date_input(
    "수집일",
    value=datetime.strptime(all_dates[0], "%Y-%m-%d").date(),
    min_value=datetime.strptime(all_dates[-1], "%Y-%m-%d").date(),
    max_value=datetime.strptime(all_dates[0], "%Y-%m-%d").date(),
)
selected_date_str = selected_date.strftime("%Y-%m-%d")

# ── Platform scan summary ──

platform_counts = get_platform_counts(selected_date_str)
if not platform_counts.empty:
    total_items = int(platform_counts["items"].sum())
    cols = st.columns(len(platform_counts) + 1)
    with cols[0]:
        st.metric("총 스캔 상품", f"{total_items:,}")
    for i, row in enumerate(platform_counts.itertuples()):
        with cols[i + 1]:
            st.metric(platform_name(row.platform), f"{row.items:,}")

# ── Cross-platform trend keywords ──

section_header("🔥", "크로스 플랫폼 트렌드 키워드")
st.caption("베스트셀러 순위 기반 가중 점수 — 순위가 높을수록 더 많은 점수 반영")

totals = get_product_keyword_totals(selected_date_str)
if totals.empty:
    st.info("해당 날짜의 베스트셀러 데이터가 없습니다.")
else:
    # Top 3 performance keywords
    perf = totals.copy()
    perf["score_per_hit"] = perf["score"] / perf["hits"]
    top3 = perf.nlargest(3, "score_per_hit")

    st.markdown("**최고 성과 키워드 TOP 3** — 상품당 트렌드 점수 기준")
    tcols = st.columns(3)
    medals = ["🥇", "🥈", "🥉"]
    for i, row in enumerate(top3.itertuples()):
        with tcols[i]:
            st.markdown(hero_card(
                f"{medals[i]} {row.keyword}",
                f"{row.score_per_hit:.0f} 점/상품",
                f"총점: {row.score:,} · {row.hits}개 상품",
            ), unsafe_allow_html=True)

    # Top 20 bar chart
    fig = px.bar(
        totals.head(20),
        x="keyword",
        y="score",
        text_auto=True,
        color="score",
        color_continuous_scale=["#c7d2fe", "#6366f1", "#312e81"],
        hover_data={"keyword": True, "score": True, "hits": True},
    )
    fig.update_traces(
        textposition="outside",
        texttemplate="%{y:,}",
    )
    fig.update_layout(
        xaxis_title="",
        yaxis_title="트렌드 점수",
        xaxis_tickangle=-45,
        showlegend=False,
    )
    fig.update_coloraxes(showscale=False)
    style_chart(fig, height=440)
    st.plotly_chart(fig, use_container_width=True)

    # Platform breakdown
    per_platform = get_product_keyword_counts(selected_date_str)
    if not per_platform.empty:
        top_kws = totals.head(15)["keyword"].tolist()
        filtered = per_platform[per_platform["keyword"].isin(top_kws)]

        if not filtered.empty:
            filtered = filtered.copy()
            filtered["platform_display"] = filtered["platform"].apply(platform_name)
            fig2 = px.bar(
                filtered,
                x="keyword",
                y="score",
                color="platform_display",
                barmode="group",
                text_auto=True,
                color_discrete_sequence=list(PLATFORM_COLORS.values()),
                labels={"platform_display": "플랫폼", "keyword": "", "score": "트렌드 점수"},
            )
            fig2.update_layout(xaxis_tickangle=-45)
            style_chart(fig2, height=440)
            st.plotly_chart(fig2, use_container_width=True)

        with st.expander("전체 키워드 가중 점수 테이블"):
            pivot_score = per_platform.pivot_table(
                index="keyword", columns="platform", values="score", fill_value=0
            )
            pivot_hits = per_platform.pivot_table(
                index="keyword", columns="platform", values="hits", fill_value=0
            )
            # Rename columns to display names
            pivot_score.columns = [platform_name(c) for c in pivot_score.columns]
            pivot_score["총점"] = pivot_score.sum(axis=1)
            pivot_hits.columns = [platform_name(c) for c in pivot_hits.columns]
            pivot_score["총상품"] = pivot_hits.sum(axis=1)
            pivot_score = pivot_score.sort_values("총점", ascending=False)
            st.dataframe(pivot_score, use_container_width=True)
            csv = pivot_score.to_csv(index=True).encode("utf-8-sig")
            st.download_button(
                label="📥 CSV 다운로드",
                data=csv,
                file_name=f"키워드_가중점수_{selected_date_str}.csv",
                mime="text/csv",
            )

# ── Musinsa keyword rankings ──

st.divider()
section_header("🔎", "무신사 인기 검색 키워드")

kw_df = get_keywords_for_date(selected_date_str)
if kw_df.empty:
    st.info(f"{selected_date_str} 무신사 키워드 데이터가 없습니다.")
else:
    # Format as styled table
    display_df = kw_df.copy()
    display_df["변동"] = display_df["category"].apply(lambda x: _parse_fluctuation(x)[0])
    display_df["순위"] = display_df["rank"]
    display_df["키워드"] = display_df["keyword"]
    display_df = display_df[["순위", "키워드", "변동"]]
    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "순위": st.column_config.NumberColumn("순위", width="small"),
            "키워드": st.column_config.TextColumn("키워드"),
            "변동": st.column_config.TextColumn("변동", width="small"),
        },
    )

    # Keyword history
    st.markdown("")
    section_header("📈", "키워드 순위 추이")
    keyword_options = kw_df["keyword"].unique().tolist()
    selected_kw = st.selectbox("키워드 선택", keyword_options, label_visibility="collapsed",
                               help="순위 변동을 확인할 키워드를 선택하세요")

    if selected_kw:
        hist = get_keyword_history(selected_kw)
        if hist.empty:
            st.info("아직 충분한 과거 데이터가 없습니다.")
        else:
            fig = px.line(
                hist,
                x="snapshot_date",
                y="rank",
                color="platform",
                markers=True,
                title=f"\"{selected_kw}\" 순위 추이",
                color_discrete_sequence=CHART_COLORS,
            )
            fig.update_yaxes(autorange="reversed", title="순위 (낮을수록 좋음)")
            fig.update_xaxes(title="날짜")
            style_chart(fig, height=380)
            st.plotly_chart(fig, use_container_width=True)
