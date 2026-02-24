"""인스타그램 해시태그 대시보드."""
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
def get_latest_metrics() -> pd.DataFrame:
    conn = _conn()
    df = pd.read_sql_query(
        """
        SELECT hashtag AS '해시태그', post_count AS '게시물 수', snapshot_date AS '수집일'
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
    conn = _conn()
    placeholders = ",".join("?" for _ in hashtags)
    df = pd.read_sql_query(
        f"""
        SELECT hashtag AS '해시태그', post_count AS '게시물 수', snapshot_date AS '수집일'
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
    conn = _conn()
    df = pd.read_sql_query(
        "SELECT DISTINCT hashtag FROM instagram_metrics ORDER BY hashtag", conn
    )
    conn.close()
    return df["hashtag"].tolist()


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

st.header("인스타그램 해시태그 추적")

latest = get_latest_metrics()

if latest.empty:
    st.info(
        "아직 인스타그램 데이터가 수집되지 않았습니다. "
        "인스타그램 스크래퍼가 실행되면 해시태그 게시물 수가 여기에 표시됩니다."
    )
    st.stop()

st.subheader("최신 해시태그 게시물 수")
st.dataframe(latest, use_container_width=True, hide_index=True)

st.subheader("게시물 수 추이")
all_tags = get_all_hashtags()
selected_tags = st.multiselect("해시태그 선택", all_tags, default=all_tags[:5])

if selected_tags:
    history = get_hashtag_history(tuple(selected_tags))
    if history.empty:
        st.info("선택한 해시태그의 과거 데이터가 아직 없습니다.")
    else:
        fig = px.line(
            history,
            x="수집일",
            y="게시물 수",
            color="해시태그",
            markers=True,
            title="인스타그램 게시물 수 추이",
        )
        fig.update_xaxes(title="날짜")
        fig.update_yaxes(title="게시물 수")
        st.plotly_chart(fig, use_container_width=True)
else:
    st.info("차트를 보려면 해시태그를 하나 이상 선택하세요.")
