"""데이터 인사이트 — 수집 데이터 기반 심층 분석."""
from __future__ import annotations

import re
import sqlite3
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ui_theme import (
    get_conn, platform_name, format_price, style_chart, section_header,
    PLATFORM_COLORS, CHART_COLORS,
)

# ---------------------------------------------------------------------------
# 트렌드 키워드 & 카테고리 매핑
# ---------------------------------------------------------------------------

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

_CATEGORY_MAP = {
    "아우터": "아우터", "재킷": "아우터", "자켓": "아우터", "코트": "아우터",
    "패딩": "아우터", "점퍼": "아우터", "블루종": "아우터", "후드 집업": "아우터",
    "레더 재킷": "아우터", "가죽/스웨이드재킷": "아우터",
    "상의": "상의", "티셔츠": "상의", "블라우스": "상의", "셔츠": "상의",
    "니트": "상의", "니트웨어": "상의", "맨투맨": "상의", "후드": "상의",
    "긴소매티셔츠": "상의", "반소매티셔츠": "상의", "슬리브리스": "상의",
    "카디건": "상의", "풀오버": "상의", "브이넥": "상의", "크루넥": "상의",
    "바지": "하의", "팬츠": "하의", "데님": "하의", "와이드팬츠": "하의",
    "조거": "하의", "슬랙스": "하의", "트레이닝": "하의", "레깅스": "하의",
    "원피스": "원피스", "스커트": "원피스/스커트",
    "가방": "가방", "숄더백": "가방", "토트백": "가방", "크로스백": "가방",
    "백팩": "가방", "클러치": "가방",
    "신발": "신발", "스니커즈": "신발", "부츠": "신발", "샌들": "신발",
    "슬리퍼": "신발", "플랫": "신발", "힐": "신발", "로퍼": "신발",
    "점프수트": "셋업/점프수트", "셋업": "셋업/점프수트",
    "언더웨어": "속옷/홈웨어", "홈웨어": "속옷/홈웨어",
    "액세서리": "액세서리", "주얼리": "액세서리", "모자": "액세서리",
    "스카프": "액세서리", "벨트": "액세서리",
}


def normalize_category(raw: str) -> str:
    if not raw or not raw.strip():
        return "기타"
    parts = [p.strip() for p in raw.replace(">", "/").split("/")]
    for part in reversed(parts):
        if part in _CATEGORY_MAP:
            return _CATEGORY_MAP[part]
    for part in reversed(parts):
        for key, val in _CATEGORY_MAP.items():
            if key in part or part in key:
                return val
    return "기타"


# ---------------------------------------------------------------------------
# 캐시 쿼리
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300)
def load_bestsellers() -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query("SELECT * FROM bestseller_rankings", conn)
    conn.close()
    return df


@st.cache_data(ttl=300)
def load_keywords() -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query("SELECT * FROM keyword_rankings", conn)
    conn.close()
    return df


# ---------------------------------------------------------------------------
# 분석 함수
# ---------------------------------------------------------------------------

def analyze_price_segments(df: pd.DataFrame) -> pd.DataFrame:
    priced = df[df["price"] > 0].copy()
    bins = [0, 30000, 50000, 80000, 120000, 200000, float("inf")]
    labels = ["~3만", "3~5만", "5~8만", "8~12만", "12~20만", "20만~"]
    priced["가격대"] = pd.cut(priced["price"], bins=bins, labels=labels, right=True)
    return priced.groupby(["가격대", "platform"], observed=True).size().reset_index(name="상품 수")


def analyze_discount_vs_rank(df: pd.DataFrame) -> pd.DataFrame:
    valid = df[(df["discount_pct"] > 0) & (df["rank"] > 0)].copy()
    bins = [0, 10, 20, 30, 50, 100]
    labels = ["1~10%", "11~20%", "21~30%", "31~50%", "51%~"]
    valid["할인구간"] = pd.cut(valid["discount_pct"], bins=bins, labels=labels, right=True)
    return valid.groupby("할인구간", observed=True).agg(
        평균순위=("rank", "mean"), 상품수=("rank", "count"),
    ).reset_index()


def analyze_brand_concentration(df: pd.DataFrame) -> dict:
    branded = df[df["brand"].fillna("").str.strip() != ""]
    total = len(branded)
    brand_counts = branded["brand"].value_counts()
    top10_share = brand_counts.head(10).sum() / total * 100 if total > 0 else 0
    top30_share = brand_counts.head(30).sum() / total * 100 if total > 0 else 0
    unique = brand_counts.shape[0]
    multi_platform = branded.groupby("brand")["platform"].nunique()
    multi_count = (multi_platform >= 2).sum()
    return {
        "unique_brands": unique,
        "top10_share": top10_share,
        "top30_share": top30_share,
        "multi_platform_brands": int(multi_count),
    }


def analyze_platform_positioning(df: pd.DataFrame) -> pd.DataFrame:
    priced = df[df["price"] > 0]
    result = priced.groupby("platform").agg(
        평균가격=("price", "mean"),
        중간가격=("price", "median"),
        평균할인율=("discount_pct", lambda x: x[x > 0].mean() if (x > 0).any() else 0),
        브랜드수=("brand", "nunique"),
        상품수=("rank", "count"),
    ).reset_index()
    result["평균가격"] = result["평균가격"].round(0).astype(int)
    result["중간가격"] = result["중간가격"].round(0).astype(int)
    result["평균할인율"] = result["평균할인율"].round(1)
    return result


def analyze_categories(df: pd.DataFrame) -> pd.DataFrame:
    data = df.copy()
    data["norm_category"] = data["category"].fillna("").apply(normalize_category)
    return data.groupby(["norm_category", "platform"]).agg(
        상품수=("rank", "count"),
        평균가격=("price", lambda x: int(x[x > 0].mean()) if (x > 0).any() else 0),
        평균할인율=("discount_pct", lambda x: round(x[x > 0].mean(), 1) if (x > 0).any() else 0),
    ).reset_index()


def find_keyword_platform_gaps(df: pd.DataFrame) -> list[dict]:
    rows = []
    for kw in TREND_KEYWORDS:
        matches = df[df["product_name"].str.contains(kw, na=False)]
        if matches.empty:
            continue
        platform_scores = {}
        for plat, group in matches.groupby("platform"):
            max_rank = df[df["platform"] == plat]["rank"].max()
            score = sum(max_rank + 1 - r for r in group["rank"])
            platform_scores[plat] = score
        if len(platform_scores) < 2:
            continue
        avg = sum(platform_scores.values()) / len(platform_scores)
        for plat, score in platform_scores.items():
            if score > avg * 1.5:
                rows.append({"키워드": kw, "플랫폼": platform_name(plat), "유형": "강세",
                             "점수": score, "평균대비": f"+{(score/avg - 1)*100:.0f}%"})
            elif score < avg * 0.5:
                rows.append({"키워드": kw, "플랫폼": platform_name(plat), "유형": "약세",
                             "점수": score, "평균대비": f"{(score/avg - 1)*100:.0f}%"})
    return rows


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

st.markdown("""
<div style="margin-bottom:8px;">
    <span style="font-size:2rem;font-weight:800;letter-spacing:-0.02em;">데이터 인사이트</span>
    <span style="font-size:0.85rem;opacity:0.4;margin-left:12px;">심층 분석 & 시장 인텔리전스</span>
</div>
""", unsafe_allow_html=True)

bs = load_bestsellers()
kw = load_keywords()

if bs.empty:
    st.info("아직 분석할 데이터가 없습니다.")
    st.stop()

# Precompute
brand_info = analyze_brand_concentration(bs)
positioning = analyze_platform_positioning(bs)
cat_data = analyze_categories(bs)
price_seg = analyze_price_segments(bs)

cheapest_plat = positioning.loc[positioning["평균가격"].idxmin()]
priciest_plat = positioning.loc[positioning["평균가격"].idxmax()]
most_discount = positioning.loc[positioning["평균할인율"].idxmax()]
least_discount = positioning.loc[positioning["평균할인율"].idxmin()]

priced = bs[bs["price"] > 0]
median_price = int(priced["price"].median())
busiest_seg = price_seg.groupby("가격대")["상품 수"].sum().idxmax() if not price_seg.empty else "N/A"

cat_totals = cat_data.groupby("norm_category")["상품수"].sum().sort_values(ascending=False)
top3_cats = cat_totals.head(3)
total_products = len(bs)

disc_rank = analyze_discount_vs_rank(bs)
best_disc_segment = disc_rank.loc[disc_rank["평균순위"].idxmin(), "할인구간"] if not disc_rank.empty else ""

kw_match_rate = 0
if not kw.empty:
    musinsa_kw = kw[kw["platform"] == "musinsa"]
    musinsa_bs = bs[bs["platform"] == "musinsa"]
    if not musinsa_kw.empty and not musinsa_bs.empty:
        top_kws = musinsa_kw.nsmallest(30, "rank")["keyword"].tolist()
        matched = sum(1 for k in top_kws if musinsa_bs["product_name"].str.contains(k, na=False).any())
        kw_match_rate = matched / len(top_kws) * 100 if top_kws else 0


# ===== 1. Executive Summary =====

section_header("🎯", "핵심 인사이트")

# Row 1: Market overview
r1c1, r1c2, r1c3 = st.columns(3)
with r1c1:
    st.markdown(f"""
    <div class="insight-card">
        <p><strong>시장 규모</strong><br>
        4개 플랫폼 · <strong>{total_products:,}개</strong> 상품 · <strong>{brand_info['unique_brands']}개</strong> 브랜드</p>
    </div>
    """, unsafe_allow_html=True)
with r1c2:
    top3_names = ", ".join(f"{c}({int(v):,})" for c, v in top3_cats.items())
    st.markdown(f"""
    <div class="insight-card">
        <p><strong>카테고리 집중도</strong><br>
        {top3_names}</p>
    </div>
    """, unsafe_allow_html=True)
with r1c3:
    st.markdown(f"""
    <div class="insight-card">
        <p><strong>가격 포지셔닝</strong><br>
        중간가 <strong>₩{median_price:,}</strong> · 최다 가격대 <strong>{busiest_seg}원</strong></p>
    </div>
    """, unsafe_allow_html=True)

# Row 2
r2c1, r2c2, r2c3 = st.columns(3)
with r2c1:
    st.markdown(f"""
    <div class="insight-card">
        <p><strong>최고가</strong> {platform_name(priciest_plat['platform'])} ₩{priciest_plat['평균가격']:,}<br>
        <strong>최저가</strong> {platform_name(cheapest_plat['platform'])} ₩{cheapest_plat['평균가격']:,}</p>
    </div>
    """, unsafe_allow_html=True)
with r2c2:
    st.markdown(f"""
    <div class="insight-card">
        <p><strong>할인 전략</strong><br>
        공격적: {platform_name(most_discount['platform'])} ({most_discount['평균할인율']}%) ·
        보수적: {platform_name(least_discount['platform'])} ({least_discount['평균할인율']}%)</p>
    </div>
    """, unsafe_allow_html=True)
with r2c3:
    st.markdown(f"""
    <div class="insight-card">
        <p><strong>브랜드 분포</strong><br>
        Top 10 점유 {brand_info['top10_share']:.1f}% · 다중 플랫폼 {brand_info['multi_platform_brands']}개</p>
    </div>
    """, unsafe_allow_html=True)

if kw_match_rate > 0:
    if kw_match_rate >= 60:
        kw_emoji, kw_comment = "🟢", "높은 일치도"
    elif kw_match_rate >= 40:
        kw_emoji, kw_comment = "🟡", "일부 괴리"
    else:
        kw_emoji, kw_comment = "🔴", "큰 괴리 — 틈새 시장 기회"
    st.markdown(f"""
    <div class="insight-card">
        <p>{kw_emoji} <strong>검색 ↔ 판매 매칭률</strong>: 무신사 TOP 30 키워드 중 <strong>{kw_match_rate:.0f}%</strong>가 베스트셀러 등장 — {kw_comment}</p>
    </div>
    """, unsafe_allow_html=True)


# ===== 2. Category Analysis =====

st.divider()
section_header("📂", "카테고리 분석")

cat_for_tree = cat_data[cat_data["norm_category"] != "기타"].copy()
if not cat_for_tree.empty:
    tree_totals = cat_for_tree.groupby("norm_category")["상품수"].sum().reset_index()
    tree_totals.columns = ["카테고리", "상품 수"]
    tree_totals = tree_totals.sort_values("상품 수", ascending=False)

    tab_tree, tab_bar = st.tabs(["트리맵", "카테고리별 플랫폼"])

    with tab_tree:
        fig = px.treemap(
            tree_totals,
            path=["카테고리"],
            values="상품 수",
            color="상품 수",
            color_continuous_scale=["#e0e7ff", "#6366f1", "#312e81"],
        )
        fig.update_traces(textinfo="label+value+percent root")
        fig.update_coloraxes(showscale=False)
        style_chart(fig, height=450)
        st.plotly_chart(fig, use_container_width=True)

    with tab_bar:
        cat_platform = cat_data[cat_data["norm_category"] != "기타"].copy()
        cat_platform["platform_display"] = cat_platform["platform"].apply(platform_name)
        fig = px.bar(
            cat_platform,
            x="platform_display",
            y="상품수",
            color="norm_category",
            barmode="stack",
            color_discrete_sequence=CHART_COLORS,
            labels={"platform_display": "", "norm_category": "카테고리"},
        )
        fig.update_layout(legend=dict(orientation="h", y=-0.2))
        style_chart(fig, height=450)
        st.plotly_chart(fig, use_container_width=True)

# Category summary table
cat_summary = cat_data[cat_data["norm_category"] != "기타"].groupby("norm_category").agg(
    총상품수=("상품수", "sum"),
    평균가격=("평균가격", "mean"),
    평균할인율=("평균할인율", "mean"),
).reset_index().sort_values("총상품수", ascending=False)
cat_summary["평균가격"] = cat_summary["평균가격"].round(0).astype(int)
cat_summary["평균할인율"] = cat_summary["평균할인율"].round(1)
cat_summary.columns = ["카테고리", "상품 수", "평균 가격(원)", "평균 할인율(%)"]

with st.expander("카테고리별 상세"):
    st.dataframe(cat_summary, use_container_width=True, hide_index=True,
                 column_config={"평균 가격(원)": st.column_config.NumberColumn(format="₩%d")})

    # Bubble chart
    if not cat_summary.empty:
        fig = px.scatter(
            cat_summary,
            x="평균 가격(원)",
            y="평균 할인율(%)",
            size="상품 수",
            text="카테고리",
            size_max=50,
            color="카테고리",
            color_discrete_sequence=CHART_COLORS,
        )
        fig.update_traces(textposition="top center")
        fig.update_layout(showlegend=False, xaxis_tickformat=",")
        style_chart(fig, height=420)
        st.plotly_chart(fig, use_container_width=True)


# ===== 3. Platform Positioning Map =====

st.divider()
section_header("🗺️", "플랫폼 포지셔닝 맵")

fig = go.Figure()
for _, row in positioning.iterrows():
    plat = row["platform"]
    color = PLATFORM_COLORS.get(plat, "#6366f1")
    fig.add_trace(go.Scatter(
        x=[row["평균가격"]],
        y=[row["평균할인율"]],
        mode="markers+text",
        marker=dict(
            size=max(row["브랜드수"] / 3, 25),
            sizemin=25,
            color=color,
            opacity=0.8,
            line=dict(width=2, color="white"),
        ),
        text=[platform_name(plat)],
        textposition="top center",
        textfont=dict(size=13, color=color),
        name=platform_name(plat),
        hovertemplate=(
            f"<b>{platform_name(plat)}</b><br>"
            f"평균가격: ₩{row['평균가격']:,}<br>"
            f"중간가격: ₩{row['중간가격']:,}<br>"
            f"평균할인율: {row['평균할인율']}%<br>"
            f"브랜드: {row['브랜드수']}개<br>"
            f"상품: {row['상품수']}개"
            "<extra></extra>"
        ),
    ))
fig.update_layout(
    xaxis_title="평균 가격 (원)",
    yaxis_title="평균 할인율 (%)",
    showlegend=False,
    xaxis_tickformat=",",
)
style_chart(fig, height=440)
st.plotly_chart(fig, use_container_width=True)

# Platform metrics row
pcols = st.columns(len(positioning))
for i, (_, row) in enumerate(positioning.iterrows()):
    plat = row["platform"]
    color = PLATFORM_COLORS.get(plat, "#6366f1")
    with pcols[i]:
        st.metric(platform_name(plat), f"₩{row['평균가격']:,}", f"할인 {row['평균할인율']}%")


# ===== 4. Price Distribution =====

st.divider()
section_header("💰", "가격대별 상품 분포")

if not price_seg.empty:
    price_seg_display = price_seg.copy()
    price_seg_display["platform_display"] = price_seg_display["platform"].apply(platform_name)
    fig = px.bar(
        price_seg_display,
        x="가격대",
        y="상품 수",
        color="platform_display",
        barmode="group",
        text_auto=True,
        color_discrete_sequence=list(PLATFORM_COLORS.values()),
        labels={"platform_display": "플랫폼"},
    )
    fig.update_layout(xaxis_title="", yaxis_title="상품 수")
    style_chart(fig, height=420)
    st.plotly_chart(fig, use_container_width=True)


# ===== 5. Discount vs Rank =====

st.divider()
section_header("🏷️", "할인율과 순위의 관계")

if not disc_rank.empty:
    fig = px.bar(
        disc_rank,
        x="할인구간",
        y="평균순위",
        text="상품수",
        color="할인구간",
        color_discrete_sequence=CHART_COLORS,
    )
    fig.update_traces(texttemplate="%{text}개", textposition="outside")
    fig.update_yaxes(autorange="reversed", title="평균 순위 (낮을수록 좋음)")
    fig.update_layout(showlegend=False, xaxis_title="")
    style_chart(fig, height=400)
    st.plotly_chart(fig, use_container_width=True)
    st.caption("할인율이 높을수록 순위가 좋은(낮은) 경향이 있는지 확인합니다.")


# ===== 6. Brand Concentration =====

st.divider()
section_header("🏢", "브랜드 집중도")

branded = bs[bs["brand"].fillna("").str.strip() != ""]
brand_counts = branded["brand"].value_counts()

bcol1, bcol2, bcol3, bcol4 = st.columns(4)
bcol1.metric("총 브랜드", f"{brand_info['unique_brands']}개")
bcol2.metric("다중 플랫폼", f"{brand_info['multi_platform_brands']}개")
bcol3.metric("Top 10 점유", f"{brand_info['top10_share']:.1f}%")
bcol4.metric("Top 30 점유", f"{brand_info['top30_share']:.1f}%")

top15 = brand_counts.head(15).reset_index()
top15.columns = ["브랜드", "상품 수"]
fig = px.bar(
    top15, x="브랜드", y="상품 수", text_auto=True,
    color="상품 수", color_continuous_scale=["#c7d2fe", "#6366f1", "#312e81"],
)
fig.update_traces(textposition="outside")
fig.update_layout(xaxis_tickangle=-45, showlegend=False)
fig.update_coloraxes(showscale=False)
style_chart(fig, height=420)
st.plotly_chart(fig, use_container_width=True)


# ===== 7. Platform Keyword Strengths/Weaknesses =====

st.divider()
section_header("💪", "플랫폼별 키워드 강세·약세")
st.caption("평균 대비 1.5배 이상이면 강세, 0.5배 이하이면 약세로 분류")

gaps = find_keyword_platform_gaps(bs)
if gaps:
    gap_df = pd.DataFrame(gaps)
    strong = gap_df[gap_df["유형"] == "강세"].sort_values("점수", ascending=False).head(15)
    weak = gap_df[gap_df["유형"] == "약세"].sort_values("점수").head(15)

    tab1, tab2 = st.tabs(["강세 키워드", "약세 키워드"])
    with tab1:
        if not strong.empty:
            fig = px.bar(
                strong, x="키워드", y="점수", color="플랫폼",
                text="평균대비", barmode="group",
                color_discrete_sequence=list(PLATFORM_COLORS.values()),
            )
            fig.update_layout(xaxis_tickangle=-45)
            style_chart(fig, height=420)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("강세 키워드가 없습니다.")
    with tab2:
        if not weak.empty:
            fig = px.bar(
                weak, x="키워드", y="점수", color="플랫폼",
                text="평균대비", barmode="group",
                color_discrete_sequence=list(PLATFORM_COLORS.values()),
            )
            fig.update_layout(xaxis_tickangle=-45)
            style_chart(fig, height=420)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("약세 키워드가 없습니다.")
else:
    st.info("키워드 분석에 충분한 데이터가 없습니다.")


# ===== 8. Musinsa Keywords vs Bestsellers =====

if not kw.empty:
    st.divider()
    section_header("🔗", "무신사 검색 키워드 ↔ 베스트셀러 연관성")
    st.caption("인기 검색 키워드가 실제 베스트셀러 상품명에 얼마나 등장하는지 분석")

    musinsa_kw = kw[kw["platform"] == "musinsa"].copy()
    musinsa_bs = bs[bs["platform"] == "musinsa"].copy()

    if not musinsa_kw.empty and not musinsa_bs.empty:
        top_kws = musinsa_kw.nsmallest(30, "rank")["keyword"].tolist()
        matches = []
        for keyword in top_kws:
            hit_count = musinsa_bs["product_name"].str.contains(keyword, na=False).sum()
            matches.append({"키워드": keyword, "베스트셀러 등장 수": hit_count})

        match_df = pd.DataFrame(matches).sort_values("베스트셀러 등장 수", ascending=False)
        found = match_df[match_df["베스트셀러 등장 수"] > 0]
        not_found = match_df[match_df["베스트셀러 등장 수"] == 0]

        mcol1, mcol2 = st.columns(2)
        mcol1.metric("키워드 → 베스트셀러 매칭", f"{len(found)} / {len(top_kws)}")
        mcol2.metric("매칭률", f"{len(found)/len(top_kws)*100:.0f}%")

        if not found.empty:
            fig = px.bar(
                found.head(15), x="키워드", y="베스트셀러 등장 수",
                text_auto=True,
                color="베스트셀러 등장 수",
                color_continuous_scale=["#fed7aa", "#f97316", "#9a3412"],
            )
            fig.update_traces(textposition="outside")
            fig.update_layout(xaxis_tickangle=-45, showlegend=False)
            fig.update_coloraxes(showscale=False)
            style_chart(fig, height=420)
            st.plotly_chart(fig, use_container_width=True)

        if not not_found.empty:
            with st.expander(f"베스트셀러 미등장 키워드 ({len(not_found)}개) — 틈새 시장 기회"):
                st.markdown(", ".join(f"**{k}**" for k in not_found["키워드"].tolist()))
                st.caption(
                    "검색 인기는 높지만 베스트셀러에 등장하지 않는 키워드입니다. "
                    "수요는 있으나 공급이 부족한 틈새 시장일 수 있습니다."
                )
    else:
        st.info("무신사 키워드 또는 베스트셀러 데이터가 부족합니다.")
