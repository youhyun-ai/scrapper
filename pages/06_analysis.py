"""데이터 분석 — 수집 데이터 기반 인사이트."""
from __future__ import annotations

import re
import sqlite3
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "trends.db"

# ---------------------------------------------------------------------------
# 모바일 대응 페이지 설정
# ---------------------------------------------------------------------------

_CHART_HEIGHT = 420
_CHART_MARGIN = dict(l=20, r=20, t=50, b=80)


def _mobile_cols(n: int):
    """2열 이상이면 모바일에서도 읽기 좋게 최대 2열로 제한."""
    return st.columns(min(n, 2))


def _style_chart(fig):
    """공통 차트 스타일 — 모바일 가독성 향상."""
    fig.update_layout(
        height=_CHART_HEIGHT,
        margin=_CHART_MARGIN,
        font=dict(size=12),
        title_font_size=15,
    )
    return fig


def _conn() -> sqlite3.Connection:
    return sqlite3.connect(str(DB_PATH))


# ---------------------------------------------------------------------------
# 트렌드 키워드
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

# ---------------------------------------------------------------------------
# 카테고리 정규화 매핑
# ---------------------------------------------------------------------------

_CATEGORY_MAP = {
    # 아우터
    "아우터": "아우터", "재킷": "아우터", "자켓": "아우터", "코트": "아우터",
    "패딩": "아우터", "점퍼": "아우터", "블루종": "아우터", "후드 집업": "아우터",
    "레더 재킷": "아우터", "가죽/스웨이드재킷": "아우터",
    # 상의
    "상의": "상의", "티셔츠": "상의", "블라우스": "상의", "셔츠": "상의",
    "니트": "상의", "니트웨어": "상의", "맨투맨": "상의", "후드": "상의",
    "긴소매티셔츠": "상의", "반소매티셔츠": "상의", "슬리브리스": "상의",
    "카디건": "상의", "풀오버": "상의", "브이넥": "상의", "크루넥": "상의",
    # 하의
    "바지": "하의", "팬츠": "하의", "데님": "하의", "와이드팬츠": "하의",
    "조거": "하의", "슬랙스": "하의", "트레이닝": "하의", "레깅스": "하의",
    # 원피스/스커트
    "원피스": "원피스", "스커트": "원피스/스커트",
    # 가방
    "가방": "가방", "숄더백": "가방", "토트백": "가방", "크로스백": "가방",
    "백팩": "가방", "클러치": "가방",
    # 신발
    "신발": "신발", "스니커즈": "신발", "부츠": "신발", "샌들": "신발",
    "슬리퍼": "신발", "플랫": "신발", "힐": "신발", "로퍼": "신발",
    # 셋업/점프수트
    "점프수트": "셋업/점프수트", "셋업": "셋업/점프수트",
    # 속옷/홈웨어
    "언더웨어": "속옷/홈웨어", "홈웨어": "속옷/홈웨어",
    # 액세서리
    "액세서리": "액세서리", "주얼리": "액세서리", "모자": "액세서리",
    "스카프": "액세서리", "벨트": "액세서리",
}


def normalize_category(raw: str) -> str:
    """플랫폼별 다양한 카테고리 형식을 통합 카테고리로 정규화."""
    if not raw or not raw.strip():
        return "기타"
    # "의류 > 아우터 > 자켓" → 각 부분 순회
    parts = [p.strip() for p in raw.replace(">", "/").split("/")]
    # 가장 구체적인 부분부터 매칭 (역순)
    for part in reversed(parts):
        if part in _CATEGORY_MAP:
            return _CATEGORY_MAP[part]
    # 부분 문자열 매칭
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
    conn = _conn()
    df = pd.read_sql_query("SELECT * FROM bestseller_rankings", conn)
    conn.close()
    return df


@st.cache_data(ttl=300)
def load_keywords() -> pd.DataFrame:
    conn = _conn()
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
        평균순위=("rank", "mean"),
        상품수=("rank", "count"),
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
    """카테고리 정규화 후 플랫폼별 분포."""
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
                rows.append({"키워드": kw, "플랫폼": plat, "유형": "강세", "점수": score, "평균대비": f"+{(score/avg - 1)*100:.0f}%"})
            elif score < avg * 0.5:
                rows.append({"키워드": kw, "플랫폼": plat, "유형": "약세", "점수": score, "평균대비": f"{(score/avg - 1)*100:.0f}%"})
    return rows


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

st.header("데이터 분석")

bs = load_bestsellers()
kw = load_keywords()

if bs.empty:
    st.info("아직 분석할 데이터가 없습니다.")
    st.stop()

# ===== 1. 핵심 인사이트 요약 =====
st.subheader("핵심 인사이트")

brand_info = analyze_brand_concentration(bs)
positioning = analyze_platform_positioning(bs)
cat_data = analyze_categories(bs)

cheapest_plat = positioning.loc[positioning["평균가격"].idxmin()]
priciest_plat = positioning.loc[positioning["평균가격"].idxmax()]
most_discount = positioning.loc[positioning["평균할인율"].idxmax()]
least_discount = positioning.loc[positioning["평균할인율"].idxmin()]

# 가격대 분석
priced = bs[bs["price"] > 0]
median_price = int(priced["price"].median())
price_seg = analyze_price_segments(bs)
if not price_seg.empty:
    busiest_seg = price_seg.groupby("가격대")["상품 수"].sum().idxmax()
else:
    busiest_seg = "N/A"

# 카테고리 분석
cat_totals = cat_data.groupby("norm_category")["상품수"].sum().sort_values(ascending=False)
top_cat = cat_totals.index[0] if len(cat_totals) > 0 else "N/A"
top_cat_count = int(cat_totals.iloc[0]) if len(cat_totals) > 0 else 0
top3_cats = cat_totals.head(3)

# 할인-순위 분석
disc_rank = analyze_discount_vs_rank(bs)
best_disc_segment = ""
if not disc_rank.empty:
    best_disc_segment = disc_rank.loc[disc_rank["평균순위"].idxmin(), "할인구간"]

# 키워드-베스트셀러 매칭 분석
kw_match_rate = 0
if not kw.empty:
    musinsa_kw = kw[kw["platform"] == "musinsa"]
    musinsa_bs = bs[bs["platform"] == "musinsa"]
    if not musinsa_kw.empty and not musinsa_bs.empty:
        top_kws = musinsa_kw.nsmallest(30, "rank")["keyword"].tolist()
        matched = sum(1 for k in top_kws if musinsa_bs["product_name"].str.contains(k, na=False).any())
        kw_match_rate = matched / len(top_kws) * 100 if top_kws else 0

insights = []

# 시장 규모 요약
total_products = len(bs)
total_brands = brand_info["unique_brands"]
insights.append(
    f"**시장 규모**: 4개 플랫폼에서 총 **{total_products:,}개** 베스트셀러 상품, "
    f"**{total_brands}개** 브랜드를 추적 중입니다."
)

# 카테고리 인사이트
cat_pct = top_cat_count / total_products * 100
top3_names = ", ".join(f"{c}({int(v):,}개)" for c, v in top3_cats.items())
insights.append(
    f"**카테고리 집중도**: {top3_names}이 상위 3개 카테고리이며, "
    f"**{top_cat}**이 전체의 {cat_pct:.0f}%를 차지합니다."
)

# 가격 포지셔닝
insights.append(
    f"**가격 포지셔닝**: {priciest_plat['platform']}(평균 {priciest_plat['평균가격']:,}원)이 "
    f"가장 고가, {cheapest_plat['platform']}(평균 {cheapest_plat['평균가격']:,}원)이 가장 저렴합니다. "
    f"전체 중간 가격은 **{median_price:,}원**, 가장 많은 상품이 **{busiest_seg}원** 구간에 몰려 있습니다."
)

# 할인 전략
insights.append(
    f"**할인 전략**: {most_discount['platform']}이 평균 {most_discount['평균할인율']}%로 "
    f"가장 공격적이며, {least_discount['platform']}이 {least_discount['평균할인율']}%로 가장 보수적입니다."
    + (f" 할인율 **{best_disc_segment}** 구간이 가장 높은 순위를 기록합니다." if best_disc_segment else "")
)

# 브랜드 분포
insights.append(
    f"**브랜드 분포**: 상위 10개 브랜드가 전체의 {brand_info['top10_share']:.1f}%를 차지하며, "
    f"**{brand_info['multi_platform_brands']}개** 브랜드가 2개 이상 플랫폼에 동시 입점했습니다."
)

# 검색-판매 괴리
if kw_match_rate > 0:
    if kw_match_rate >= 60:
        kw_comment = "검색 트렌드와 실제 판매가 높은 일치도를 보입니다."
    elif kw_match_rate >= 40:
        kw_comment = "검색 트렌드와 실제 판매 사이에 일부 괴리가 있습니다."
    else:
        kw_comment = "검색 트렌드와 실제 판매 사이에 큰 괴리가 있어, 틈새 시장 기회가 존재합니다."
    insights.append(
        f"**검색 ↔ 판매**: 무신사 인기 검색 키워드 TOP 30 중 **{kw_match_rate:.0f}%**가 "
        f"베스트셀러에 등장합니다. {kw_comment}"
    )

for insight in insights:
    st.markdown(f"- {insight}")

# ===== 2. 카테고리 분석 =====
st.divider()
st.subheader("카테고리 분석")

# 전체 카테고리 분포 (트리맵)
cat_for_tree = cat_data[cat_data["norm_category"] != "기타"].copy()
if not cat_for_tree.empty:
    tree_totals = cat_for_tree.groupby("norm_category")["상품수"].sum().reset_index()
    tree_totals.columns = ["카테고리", "상품 수"]
    tree_totals = tree_totals.sort_values("상품 수", ascending=False)

    fig = px.treemap(
        tree_totals,
        path=["카테고리"],
        values="상품 수",
        title="카테고리별 상품 비중",
        color="상품 수",
        color_continuous_scale="Teal",
    )
    _style_chart(fig)
    fig.update_traces(textinfo="label+value+percent root")
    fig.update_coloraxes(showscale=False)
    st.plotly_chart(fig, use_container_width=True)

# 플랫폼별 카테고리 구성
st.markdown("**플랫폼별 카테고리 구성**")
cat_platform = cat_data[cat_data["norm_category"] != "기타"].copy()
if not cat_platform.empty:
    fig = px.bar(
        cat_platform,
        x="platform",
        y="상품수",
        color="norm_category",
        title="플랫폼별 카테고리 분포",
        barmode="stack",
        labels={"platform": "플랫폼", "norm_category": "카테고리"},
    )
    _style_chart(fig)
    fig.update_layout(legend=dict(orientation="h", y=-0.3))
    st.plotly_chart(fig, use_container_width=True)

# 카테고리별 평균 가격·할인율
st.markdown("**카테고리별 평균 가격 · 할인율**")
cat_summary = cat_data[cat_data["norm_category"] != "기타"].groupby("norm_category").agg(
    총상품수=("상품수", "sum"),
    평균가격=("평균가격", "mean"),
    평균할인율=("평균할인율", "mean"),
).reset_index().sort_values("총상품수", ascending=False)
cat_summary["평균가격"] = cat_summary["평균가격"].round(0).astype(int)
cat_summary["평균할인율"] = cat_summary["평균할인율"].round(1)
cat_summary.columns = ["카테고리", "상품 수", "평균 가격(원)", "평균 할인율(%)"]
st.dataframe(cat_summary, use_container_width=True, hide_index=True)

# 카테고리별 가격 비교 차트
if not cat_summary.empty:
    fig = px.scatter(
        cat_summary,
        x="평균 가격(원)",
        y="평균 할인율(%)",
        size="상품 수",
        text="카테고리",
        title="카테고리 포지셔닝 (버블 크기 = 상품 수)",
        size_max=50,
    )
    fig.update_traces(textposition="top center")
    _style_chart(fig)
    fig.update_layout(showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

# ===== 3. 플랫폼 포지셔닝 맵 =====
st.divider()
st.subheader("플랫폼 포지셔닝 비교")

fig = go.Figure()
for _, row in positioning.iterrows():
    fig.add_trace(go.Scatter(
        x=[row["평균가격"]],
        y=[row["평균할인율"]],
        mode="markers+text",
        marker=dict(size=max(row["브랜드수"] / 3, 20), sizemin=20),
        text=[row["platform"]],
        textposition="top center",
        name=row["platform"],
        hovertemplate=(
            f"<b>{row['platform']}</b><br>"
            f"평균가격: {row['평균가격']:,}원<br>"
            f"중간가격: {row['중간가격']:,}원<br>"
            f"평균할인율: {row['평균할인율']}%<br>"
            f"브랜드: {row['브랜드수']}개<br>"
            f"상품: {row['상품수']}개"
            "<extra></extra>"
        ),
    ))
_style_chart(fig)
fig.update_layout(
    title="플랫폼 포지셔닝 맵 (버블 크기 = 브랜드 수)",
    xaxis_title="평균 가격 (원)",
    yaxis_title="평균 할인율 (%)",
    showlegend=False,
)
st.plotly_chart(fig, use_container_width=True)

c1, c2 = _mobile_cols(4)
for i, (_, row) in enumerate(positioning.iterrows()):
    col = c1 if i % 2 == 0 else c2
    with col:
        st.metric(row["platform"], f"{row['평균가격']:,}원", f"할인 {row['평균할인율']}%")

# ===== 4. 가격대별 분포 =====
st.divider()
st.subheader("가격대별 상품 분포")

if not price_seg.empty:
    fig = px.bar(
        price_seg,
        x="가격대",
        y="상품 수",
        color="platform",
        barmode="group",
        title="가격대별 플랫폼 상품 수",
        text_auto=True,
    )
    _style_chart(fig)
    fig.update_layout(xaxis_title="가격대", yaxis_title="상품 수")
    st.plotly_chart(fig, use_container_width=True)

# ===== 5. 할인율 vs 순위 =====
st.divider()
st.subheader("할인율과 순위의 관계")

if not disc_rank.empty:
    fig = px.bar(
        disc_rank,
        x="할인구간",
        y="평균순위",
        text="상품수",
        title="할인율 구간별 평균 순위 (낮을수록 좋음)",
        color="할인구간",
    )
    fig.update_traces(texttemplate="%{text}개", textposition="outside")
    fig.update_yaxes(autorange="reversed", title="평균 순위")
    _style_chart(fig)
    fig.update_layout(showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

# ===== 6. 브랜드 집중도 =====
st.divider()
st.subheader("브랜드 집중도 분석")

branded = bs[bs["brand"].fillna("").str.strip() != ""]
brand_counts = branded["brand"].value_counts()

c1, c2 = _mobile_cols(2)
with c1:
    st.metric("총 브랜드 수", f"{brand_info['unique_brands']}개")
    st.metric("다중 플랫폼 브랜드", f"{brand_info['multi_platform_brands']}개")
with c2:
    st.metric("상위 10개 브랜드 점유율", f"{brand_info['top10_share']:.1f}%")
    st.metric("상위 30개 브랜드 점유율", f"{brand_info['top30_share']:.1f}%")

top15 = brand_counts.head(15).reset_index()
top15.columns = ["브랜드", "상품 수"]
fig = px.bar(
    top15, x="브랜드", y="상품 수", text_auto=True,
    title="상위 15개 브랜드 (전체 플랫폼)",
    color="상품 수", color_continuous_scale="Blues",
)
_style_chart(fig)
fig.update_layout(xaxis_tickangle=-45)
fig.update_coloraxes(showscale=False)
st.plotly_chart(fig, use_container_width=True)

# ===== 7. 플랫폼별 키워드 강세/약세 =====
st.divider()
st.subheader("플랫폼별 키워드 강세·약세")
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
                text="평균대비", title="플랫폼별 강세 키워드 TOP 15", barmode="group",
            )
            _style_chart(fig)
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("강세 키워드가 없습니다.")
    with tab2:
        if not weak.empty:
            fig = px.bar(
                weak, x="키워드", y="점수", color="플랫폼",
                text="평균대비", title="플랫폼별 약세 키워드 TOP 15", barmode="group",
            )
            _style_chart(fig)
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("약세 키워드가 없습니다.")
else:
    st.info("키워드 분석에 충분한 데이터가 없습니다.")

# ===== 8. 무신사 키워드 vs 실제 베스트셀러 =====
if not kw.empty:
    st.divider()
    st.subheader("무신사 검색 키워드 ↔ 베스트셀러 연관성")
    st.caption("무신사 인기 검색 키워드가 실제 베스트셀러 상품명에 얼마나 등장하는지 분석")

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

        c1, c2 = _mobile_cols(2)
        c1.metric("검색 키워드 → 베스트셀러 매칭", f"{len(found)} / {len(top_kws)}")
        c2.metric("매칭률", f"{len(found)/len(top_kws)*100:.0f}%")

        if not found.empty:
            fig = px.bar(
                found.head(15), x="키워드", y="베스트셀러 등장 수",
                text_auto=True, title="인기 검색 키워드의 베스트셀러 등장 빈도 (무신사)",
                color="베스트셀러 등장 수", color_continuous_scale="Oranges",
            )
            _style_chart(fig)
            fig.update_layout(xaxis_tickangle=-45)
            fig.update_coloraxes(showscale=False)
            st.plotly_chart(fig, use_container_width=True)

        if not not_found.empty:
            with st.expander(f"베스트셀러에 미등장 키워드 ({len(not_found)}개)"):
                st.markdown(", ".join(not_found["키워드"].tolist()))
                st.caption(
                    "이 키워드들은 검색 인기는 높지만 베스트셀러에는 등장하지 않습니다. "
                    "수요는 있으나 공급이 부족한 틈새 시장일 수 있습니다."
                )
    else:
        st.info("무신사 키워드 또는 베스트셀러 데이터가 부족합니다.")
