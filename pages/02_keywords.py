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


KEYWORD_CATEGORIES = {
    "색상": [
        "블랙", "화이트", "베이지", "그레이", "카키", "네이비", "브라운",
        "파스텔", "민트", "아이보리", "버건디", "블루", "핑크",
        "레드", "옐로우", "퍼플", "라벤더", "올리브", "크림", "차콜",
        "카멜", "코발트", "스카이블루", "와인",
    ],
    "아이템": [
        # 상의
        "니트", "카디건", "가디건", "블라우스", "후드", "후드티", "맨투맨",
        "자켓", "패딩", "코트", "셔츠", "체크셔츠", "나시", "반팔", "반팔티",
        "롱슬리브", "슬리브", "티셔츠", "브이넥", "집업", "반집업", "후드집업",
        "블루종", "봄버자켓", "항공점퍼", "점퍼", "야상", "아노락",
        "가죽자켓", "레더자켓", "스웨이드자켓", "청자켓",
        "트위드 자켓", "워크자켓", "져지", "플리스", "후리스", "바람막이",
        "윈드브레이커", "경량패딩", "무스탕", "퍼자켓", "패딩조끼", "아우터",
        "숏코트", "하프코트",
        # 하의
        "팬츠", "바지", "스커트", "치마", "치마바지", "청바지", "데님",
        "슬랙스", "조거팬츠", "카고팬츠", "와이드팬츠", "트레이닝 바지",
        "트레이닝 팬츠", "스웻팬츠", "반바지", "레깅스", "부츠컷",
        "커브드팬츠", "코튼 팬츠",
        # 원피스/셋업
        "원피스", "어반드레스", "셋업", "트레이닝 셋업",
        # 신발
        "스니커즈", "운동화", "러닝화", "로퍼", "구두", "부츠", "워커",
        "슬리퍼", "메리제인", "뮬", "크록스",
        # 가방
        "가방", "백팩", "숄더백", "크로스백", "토트백", "미니백", "에코백",
        "호보백", "파우치", "더플백",
        # 악세서리
        "모자", "볼캡", "캡모자", "비니", "선글라스", "안경", "시계",
        "목걸이", "반지", "팔찌", "벨트", "키링", "헤어밴드",
    ],
    "핏/스타일": [
        # 핏/실루엣
        "크롭", "와이드", "오버사이즈", "슬림", "루즈", "배기", "미니", "롱",
        "숏", "하이웨이스트", "로우라이즈", "플레어", "A라인", "박시",
        "슬림핏", "오프숄더", "원숄더",
        # 패턴/디테일
        "레이어드", "셔링", "플리츠", "프릴", "리본", "스트링", "컷아웃",
        "슬릿", "레이스", "스트라이프", "체크", "플로럴", "도트",
        # 소재
        "레더", "퍼", "트위드", "벨벳", "린넨", "코듀로이", "스웨이드",
        # 스타일/무드
        "빈티지", "레트로", "미니멀", "캐주얼", "스포티", "시티보이",
        "고프코어", "발레코어", "올드머니", "사이버펑크",
        "조거", "트랙", "바이커",
    ],
}

# Flat list for backward compat
TREND_KEYWORDS = []
for _kws in KEYWORD_CATEGORIES.values():
    TREND_KEYWORDS.extend(k for k in _kws if k not in TREND_KEYWORDS)


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


# 키워드 매칭용 정규식 (한 번만 컴파일)
_KW_PATTERN = re.compile("|".join(re.escape(kw) for kw in sorted(TREND_KEYWORDS, key=len, reverse=True)))


@st.cache_data(ttl=300)
def _build_keyword_scores(snapshot_date: str) -> pd.DataFrame:
    """모든 키워드의 플랫폼별 점수를 한 번에 계산 (캐시)."""
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
    top10_cutoffs = {p: int(m * 0.1) for p, m in max_ranks.items()}
    data: dict = {}
    for plat, max_rank, name, rank in zip(df["platform"], df["platform"].map(max_ranks), df["product_name"], df["rank"]):
        normalized = (1 - rank / max_rank) * 100
        if rank <= top10_cutoffs[plat]:
            normalized *= 1.5
        for m in _KW_PATTERN.finditer(name):
            kw = m.group()
            key = (kw, plat)
            if key not in data:
                data[key] = [0.0, 0]
            data[key][0] += normalized
            data[key][1] += 1
    if not data:
        return pd.DataFrame()
    rows = [{"keyword": k, "platform": p, "score": round(v[0], 1), "hits": v[1]}
            for (k, p), v in data.items()]
    return pd.DataFrame(rows)


def get_product_keyword_counts(snapshot_date: str) -> pd.DataFrame:
    return _build_keyword_scores(snapshot_date)


def get_product_keyword_totals(snapshot_date: str) -> pd.DataFrame:
    per_platform = _build_keyword_scores(snapshot_date)
    if per_platform.empty:
        return pd.DataFrame()
    # 키워드별 합산
    grouped = per_platform.groupby("keyword").agg(
        score=("score", "sum"),
        hits=("hits", "sum"),
        platforms=("platform", "nunique"),
    ).reset_index()
    # 크로스 플랫폼 보너스
    grouped["score"] = grouped.apply(
        lambda r: round(r["score"] * (1 + (r["platforms"] - 1) * 0.2), 1), axis=1
    )
    grouped = grouped.sort_values("score", ascending=False).reset_index(drop=True)
    return grouped


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

with st.expander("ℹ️ 점수 산정 방식"):
    st.markdown("""
**트렌드 점수**는 베스트셀러 상품명에 키워드가 포함된 횟수와 순위를 기반으로 산출됩니다.

| 요소 | 설명 |
|------|------|
| **플랫폼 정규화** | 각 플랫폼 내 순위를 0\~100 점으로 정규화하여 플랫폼 간 공정 비교 |
| **상위 10% 가산** | 플랫폼 내 상위 10% 상품은 1.5배 가중치 |
| **크로스 플랫폼 보너스** | 여러 플랫폼에 등장할수록 가산 (2개=1.2x, 3개=1.4x, 4개=1.6x) |
| **최소 등장 기준** | TOP 3 성과 키워드는 5개 이상 상품에 등장해야 선정 |

`점/상품` = 총점 ÷ 등장 상품 수 (상품당 평균 트렌드 점수)
""")

# ── Category filter ──
cat_options_raw = list(KEYWORD_CATEGORIES.keys())
cat_labels = {f"{cat} ({len(KEYWORD_CATEGORIES[cat])})": cat for cat in cat_options_raw}
all_label = f"전체 ({len(TREND_KEYWORDS)})"
selected_pill = st.pills(
    "카테고리 필터",
    [all_label] + list(cat_labels.keys()),
    default=all_label,
    label_visibility="collapsed",
)

if selected_pill == all_label or selected_pill is None:
    active_keywords = TREND_KEYWORDS
    active_label = "전체"
else:
    real_cat = cat_labels[selected_pill]
    active_keywords = KEYWORD_CATEGORIES[real_cat]
    active_label = real_cat

totals = get_product_keyword_totals(selected_date_str)
if not totals.empty:
    totals = totals[totals["keyword"].isin(active_keywords)]
if totals.empty:
    st.info("해당 날짜의 베스트셀러 데이터가 없습니다." if active_label == "전체" else f"'{active_label}' 카테고리에 해당하는 트렌드 키워드가 없습니다.")
else:
    # Top 3 performance keywords (최소 5개 상품 등장)
    perf = totals[totals["hits"] >= 5].copy()
    if not perf.empty:
        perf["score_per_hit"] = perf["score"] / perf["hits"]
        top3 = perf.nlargest(3, "score_per_hit")

        # 전날 데이터 비교
        prev_perf = None
        date_idx = all_dates.index(selected_date_str) if selected_date_str in all_dates else -1
        if date_idx >= 0 and date_idx < len(all_dates) - 1:
            prev_date = all_dates[date_idx + 1]
            prev_totals = get_product_keyword_totals(prev_date)
            if not prev_totals.empty:
                prev_totals = prev_totals[prev_totals["keyword"].isin(active_keywords)]
                prev_perf = prev_totals[prev_totals["hits"] >= 5].copy()
                if not prev_perf.empty:
                    prev_perf["score_per_hit"] = prev_perf["score"] / prev_perf["hits"]
                    prev_perf["prev_rank"] = range(1, len(prev_perf.nlargest(len(prev_perf), "score_per_hit")) + 1)
                    prev_perf = prev_perf.nlargest(len(prev_perf), "score_per_hit")
                    prev_perf["prev_rank"] = range(1, len(prev_perf) + 1)

        st.markdown("**최고 성과 키워드 TOP 3** — 상품당 트렌드 점수 기준")
        tcols = st.columns(3)
        medals = ["🥇", "🥈", "🥉"]
        for i, row in enumerate(top3.itertuples()):
            plat_count = row.platforms if hasattr(row, "platforms") else ""
            plat_text = f" · {plat_count}개 플랫폼" if plat_count else ""

            # 전날 비교
            change_text = ""
            if prev_perf is not None and not prev_perf.empty:
                prev_row = prev_perf[prev_perf["keyword"] == row.keyword]
                if not prev_row.empty:
                    prev_sph = prev_row.iloc[0]["score_per_hit"]
                    prev_rank = int(prev_row.iloc[0]["prev_rank"])
                    diff = row.score_per_hit - prev_sph
                    if diff > 0:
                        change_text = f"전일 대비 ▲{diff:.0f} (전일 {prev_rank}위)"
                    elif diff < 0:
                        change_text = f"전일 대비 ▼{abs(diff):.0f} (전일 {prev_rank}위)"
                    else:
                        change_text = f"전일과 동일 (전일 {prev_rank}위)"
                else:
                    change_text = "신규 진입"

            subtitle = f"총점: {row.score:,.0f} · {row.hits}개 상품{plat_text}"
            if change_text:
                subtitle += f"<br>{change_text}"

            with tcols[i]:
                st.markdown(hero_card(
                    f"{medals[i]} {row.keyword}",
                    f"{row.score_per_hit:.0f} 점/상품",
                    subtitle,
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
        per_platform = per_platform[per_platform["keyword"].isin(active_keywords)]
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
