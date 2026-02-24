"""Instagram metrics dashboard."""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

DB_PATH = Path(__file__).resolve().parents[2] / "data" / "trends.db"


def _conn() -> sqlite3.Connection:
    return sqlite3.connect(str(DB_PATH))


# ---------------------------------------------------------------------------
# Cached queries
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300)
def get_latest_metrics() -> pd.DataFrame:
    conn = _conn()
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
    conn = _conn()
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
def get_all_hashtags() -> list[str]:
    conn = _conn()
    df = pd.read_sql_query(
        "SELECT DISTINCT hashtag FROM instagram_metrics ORDER BY hashtag", conn
    )
    conn.close()
    return df["hashtag"].tolist()


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

st.header("Instagram Hashtag Tracking")

latest = get_latest_metrics()

if latest.empty:
    st.info(
        "No Instagram data has been collected yet. "
        "Once the Instagram scraper runs, hashtag post counts will appear here."
    )
    st.stop()

st.subheader("Latest Hashtag Post Counts")
st.dataframe(latest, use_container_width=True, hide_index=True)

st.subheader("Post Count Over Time")
all_tags = get_all_hashtags()
selected_tags = st.multiselect("Select hashtags", all_tags, default=all_tags[:5])

if selected_tags:
    history = get_hashtag_history(tuple(selected_tags))
    if history.empty:
        st.info("No historical data for the selected hashtags yet.")
    else:
        fig = px.line(
            history,
            x="snapshot_date",
            y="post_count",
            color="hashtag",
            markers=True,
            title="Instagram Post Count Over Time",
        )
        fig.update_xaxes(title="Date")
        fig.update_yaxes(title="Post Count")
        st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Select at least one hashtag to see the chart.")
