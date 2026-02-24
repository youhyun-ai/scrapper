"""데이터 분석 — 수집 데이터 기반 인사이트."""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "trends.db"


def _conn() -> sqlite3.Connection:
    return sqlite3.connect(str(DB_PATH))


# ---------------------------------------------------------------------------
# 트렌드 키워드 (02_keywords.py와 동일)
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
# 캐시 쿼리
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300)
def load_bestsellers() -> pd.DataFrame:
    conn = _conn()
    df = pd.read_sql_query(
        "SELECT * FROM bestseller_rankings", conn,
    )
    conn.close()
    return df


@st.cache_data(ttl=300)
def load_keywords() -> pd.DataFrame:
    conn = _conn()
    df = pd.read_sql_query(
        "SELECT * FROM keyword_rankings", conn,
    )
    conn.close()
    return df


# ---------------------------------------------------------------------------
# 분석 함수
# ---------------------------------------------------------------------------

def analyze_price_segments(df: pd.DataFrame) -> pd.DataFrame:
    """가격대별 상품 분포."""
    priced = df[df["price"] > 0].copy()
    bins = [0, 30000, 50000, 80000, 120000, 200000, float("inf")]
    labels = ["~3만", "3~5만", "5~8만", "8~12만", "12~20만", "20만~"]
    priced["가격대"] = pd.cut(priced["price"], bins=bins, labels=labels, right=True)
    return priced.groupby(["가격대", "platform"], observed=True).size().reset_index(name="상품 수")


def analyze_discount_vs_rank(df: pd.DataFrame) -> pd.DataFrame:
    """할인율과 순위 관계."""
    valid = df[(df["discount_pct"] > 0) & (df["rank"] > 0)].copy()
    bins = [0, 10, 20, 30, 50, 100]
    labels = ["1~10%", "11~20%", "21~30%", "31~50%", "51%~"]
    valid["할인구간"] = pd.cut(valid["discount_pct"], bins=bins, labels=labels, right=True)
    return valid.groupby("할인구간", observed=True).agg(
        평균순위=("rank", "mean"),
        상품수=("rank", "count"),
    ).reset_index()


def analyze_brand_concentration(df: pd.DataFrame) -> dict:
    """브랜드 집중도 분석."""
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
    """플랫폼별 포지셔닝 — 평균 가격, 평균 할인율, 브랜드 수."""
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


def find_keyword_platform_gaps(df: pd.DataFrame) -> list[dict]:
    """플랫폼별 키워드 강세/약세 찾기."""
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


def find_price_opportunities(df: pd.DataFrame) -> pd.DataFrame:
    """같은 브랜드의 플랫폼 간 가격 차이."""
    branded = df[(df["brand"].fillna("").str.strip() != "") & (df["price"] > 0)]
    multi = branded.groupby("brand").filter(lambda x: x["platform"].nunique() >= 2)
    if multi.empty:
        return pd.DataFrame()
    stats = multi.groupby(["brand", "platform"])["price"].mean().unstack(fill_value=0)
    rows = []
    for brand in stats.index:
        prices = {p: int(v) for p, v in stats.loc[brand].items() if v > 0}
        if len(prices) < 2:
            continue
        cheapest = min(prices, key=prices.get)
        priciest = max(prices, key=prices.get)
        diff = prices[priciest] - prices[cheapest]
        pct = diff / prices[cheapest] * 100
        if diff > 5000:
            rows.append({
                "브랜드": brand,
                "최저가 플랫폼": cheapest,
                "최저가": f"{prices[cheapest]:,}원",
                "최고가 플랫폼": priciest,
                "최고가": f"{prices[priciest]:,}원",
                "가격차": f"{diff:,}원",
                "차이율": f"{pct:.0f}%",
            })
    return pd.DataFrame(rows).sort_values("차이율", ascending=False, key=lambda x: x.str.rstrip("%").astype(float)).head(20)


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

cheapest_plat = positioning.loc[positioning["평균가격"].idxmin()]
priciest_plat = positioning.loc[positioning["평균가격"].idxmax()]
most_discount = positioning.loc[positioning["평균할인율"].idxmax()]

insights = []
insights.append(
    f"**가격 포지셔닝**: {priciest_plat['platform']}이 평균 {priciest_plat['평균가격']:,.0f}원으로 "
    f"가장 고가이며, {cheapest_plat['platform']}이 평균 {cheapest_plat['평균가격']:,.0f}원으로 가장 저렴합니다."
)
insights.append(
    f"**할인 전략**: {most_discount['platform']}이 평균 {most_discount['평균할인율']}% 할인으로 "
    f"가장 공격적인 할인 정책을 운영합니다."
)
insights.append(
    f"**브랜드 분포**: 총 {brand_info['unique_brands']}개 브랜드 중 상위 10개가 "
    f"전체 상품의 {brand_info['top10_share']:.1f}%를 차지합니다. "
    f"{brand_info['multi_platform_brands']}개 브랜드가 2개 이상 플랫폼에 입점했습니다."
)

# 가격대 분석
priced = bs[bs["price"] > 0]
median_price = int(priced["price"].median())
mode_range = "3~5만원" if 30000 <= median_price <= 50000 else f"{median_price//10000}만원대"
insights.append(
    f"**가격 핵심 구간**: 전체 중간 가격은 {median_price:,}원이며, "
    f"가장 많은 상품이 분포된 구간은 {mode_range}입니다."
)

for insight in insights:
    st.markdown(f"- {insight}")

# ===== 2. 플랫폼 포지셔닝 맵 =====
st.subheader("플랫폼 포지셔닝 비교")

fig = go.Figure()
for _, row in positioning.iterrows():
    fig.add_trace(go.Scatter(
        x=[row["평균가격"]],
        y=[row["평균할인율"]],
        mode="markers+text",
        marker=dict(size=row["브랜드수"] / 3, sizemin=15),
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
fig.update_layout(
    title="플랫폼 포지셔닝 맵 (버블 크기 = 브랜드 수)",
    xaxis_title="평균 가격 (원)",
    yaxis_title="평균 할인율 (%)",
    showlegend=False,
)
st.plotly_chart(fig, use_container_width=True)

col1, col2, col3, col4 = st.columns(4)
for i, (_, row) in enumerate(positioning.iterrows()):
    col = [col1, col2, col3, col4][i % 4]
    with col:
        st.metric(row["platform"], f"{row['평균가격']:,}원", f"할인 {row['평균할인율']}%")

# ===== 3. 가격대별 분포 =====
st.subheader("가격대별 상품 분포")

price_seg = analyze_price_segments(bs)
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
    fig.update_layout(xaxis_title="가격대", yaxis_title="상품 수")
    st.plotly_chart(fig, use_container_width=True)

    st.caption(
        "각 플랫폼의 주력 가격대를 보여줍니다. "
        "자사 상품 가격 설정 시 경쟁 상품이 가장 많은 구간을 참고하세요."
    )

# ===== 4. 할인율 vs 순위 =====
st.subheader("할인율과 순위의 관계")

disc_rank = analyze_discount_vs_rank(bs)
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
    fig.update_layout(showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

    # 인사이트 생성
    best_segment = disc_rank.loc[disc_rank["평균순위"].idxmin()]
    st.markdown(
        f"할인율 **{best_segment['할인구간']}** 구간의 상품이 평균 **{best_segment['평균순위']:.0f}위**로 "
        f"가장 높은 순위를 기록했습니다 ({int(best_segment['상품수'])}개 상품)."
    )

# ===== 5. 브랜드 집중도 =====
st.subheader("브랜드 집중도 분석")

branded = bs[bs["brand"].fillna("").str.strip() != ""]
brand_counts = branded["brand"].value_counts()

col_a, col_b = st.columns(2)
with col_a:
    st.metric("총 브랜드 수", f"{brand_info['unique_brands']}개")
    st.metric("다중 플랫폼 브랜드", f"{brand_info['multi_platform_brands']}개")
with col_b:
    st.metric("상위 10개 브랜드 점유율", f"{brand_info['top10_share']:.1f}%")
    st.metric("상위 30개 브랜드 점유율", f"{brand_info['top30_share']:.1f}%")

top15 = brand_counts.head(15).reset_index()
top15.columns = ["브랜드", "상품 수"]
fig = px.bar(
    top15,
    x="브랜드",
    y="상품 수",
    text_auto=True,
    title="상위 15개 브랜드 (전체 플랫폼)",
    color="상품 수",
    color_continuous_scale="Blues",
)
fig.update_layout(xaxis_tickangle=-45, showlegend=False)
fig.update_coloraxes(showscale=False)
st.plotly_chart(fig, use_container_width=True)

# ===== 6. 플랫폼별 키워드 강세/약세 =====
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
                strong,
                x="키워드",
                y="점수",
                color="플랫폼",
                text="평균대비",
                title="플랫폼별 강세 키워드 TOP 15",
                barmode="group",
            )
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("강세 키워드가 없습니다.")
    with tab2:
        if not weak.empty:
            fig = px.bar(
                weak,
                x="키워드",
                y="점수",
                color="플랫폼",
                text="평균대비",
                title="플랫폼별 약세 키워드 TOP 15",
                barmode="group",
            )
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("약세 키워드가 없습니다.")
else:
    st.info("키워드 분석에 충분한 데이터가 없습니다.")

# ===== 7. 브랜드 가격 비교 =====
st.subheader("동일 브랜드 플랫폼 간 가격 비교")
st.caption("2개 이상 플랫폼에 입점한 브랜드의 평균 가격 차이")

price_opp = find_price_opportunities(bs)
if price_opp.empty:
    st.info("비교할 수 있는 브랜드가 없습니다.")
else:
    st.dataframe(price_opp, use_container_width=True, hide_index=True)
    st.markdown(
        f"총 **{len(price_opp)}개** 브랜드에서 플랫폼 간 가격 차이가 발견되었습니다. "
        "동일 브랜드라도 플랫폼에 따라 가격 정책이 다를 수 있으므로 입점 전략 수립 시 참고하세요."
    )

# ===== 8. 무신사 키워드 vs 실제 베스트셀러 =====
if not kw.empty:
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

        col1, col2 = st.columns(2)
        col1.metric("검색 키워드 → 베스트셀러 매칭", f"{len(found)} / {len(top_kws)}")
        col2.metric("매칭률", f"{len(found)/len(top_kws)*100:.0f}%")

        if not found.empty:
            fig = px.bar(
                found.head(15),
                x="키워드",
                y="베스트셀러 등장 수",
                text_auto=True,
                title="인기 검색 키워드의 베스트셀러 등장 빈도 (무신사)",
                color="베스트셀러 등장 수",
                color_continuous_scale="Oranges",
            )
            fig.update_layout(xaxis_tickangle=-45, showlegend=False)
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
