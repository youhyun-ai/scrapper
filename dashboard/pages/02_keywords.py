"""Keyword Rankings dashboard."""
from __future__ import annotations

import sqlite3
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

DB_PATH = Path(__file__).resolve().parents[2] / "data" / "trends.db"


def _conn() -> sqlite3.Connection:
    return sqlite3.connect(str(DB_PATH))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_fluctuation(cat: str | None) -> str:
    """Convert 'UP:3' -> '\u21913', 'DOWN:2' -> '\u21932', 'NONE:0' -> '-'."""
    if not cat:
        return "-"
    parts = cat.split(":")
    if len(parts) != 2:
        return cat
    direction, amount = parts[0], parts[1]
    if direction == "UP":
        return f"\u2191{amount}"
    if direction == "DOWN":
        return f"\u2193{amount}"
    return "-"


# ---------------------------------------------------------------------------
# Cached queries
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300)
def get_snapshot_dates() -> list[str]:
    conn = _conn()
    df = pd.read_sql_query(
        "SELECT DISTINCT snapshot_date FROM keyword_rankings ORDER BY snapshot_date DESC",
        conn,
    )
    conn.close()
    return df["snapshot_date"].tolist()


@st.cache_data(ttl=300)
def get_keywords_for_date(snapshot_date: str) -> pd.DataFrame:
    conn = _conn()
    df = pd.read_sql_query(
        """
        SELECT rank, keyword, category AS fluctuation, platform
        FROM keyword_rankings
        WHERE snapshot_date = ?
        ORDER BY rank
        """,
        conn,
        params=(snapshot_date,),
    )
    conn.close()
    df["fluctuation"] = df["fluctuation"].apply(_parse_fluctuation)
    return df


@st.cache_data(ttl=300)
def get_keyword_history(keyword: str) -> pd.DataFrame:
    conn = _conn()
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
def get_top_keywords(snapshot_date: str, limit: int = 20) -> pd.DataFrame:
    conn = _conn()
    df = pd.read_sql_query(
        """
        SELECT keyword, rank
        FROM keyword_rankings
        WHERE snapshot_date = ?
        ORDER BY rank
        LIMIT ?
        """,
        conn,
        params=(snapshot_date, limit),
    )
    conn.close()
    return df


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

st.header("Keyword Rankings")

dates = get_snapshot_dates()
if not dates:
    st.info("No keyword data available yet.")
    st.stop()

selected_date = st.date_input(
    "Snapshot date",
    value=datetime.strptime(dates[0], "%Y-%m-%d").date(),
    min_value=datetime.strptime(dates[-1], "%Y-%m-%d").date(),
    max_value=datetime.strptime(dates[0], "%Y-%m-%d").date(),
)
selected_date_str = selected_date.strftime("%Y-%m-%d")

kw_df = get_keywords_for_date(selected_date_str)
if kw_df.empty:
    st.warning(f"No keyword data for {selected_date_str}.")
    st.stop()

st.subheader("Today's Keyword Rankings")
st.dataframe(kw_df, use_container_width=True, hide_index=True)

# --- Rank over time for selected keyword ---
st.subheader("Keyword Rank Over Time")
keyword_options = kw_df["keyword"].unique().tolist()
selected_kw = st.selectbox("Select a keyword", keyword_options)

if selected_kw:
    hist = get_keyword_history(selected_kw)
    if hist.empty:
        st.info("Not enough history yet — data will appear as more days are scraped.")
    else:
        fig = px.line(
            hist,
            x="snapshot_date",
            y="rank",
            color="platform",
            markers=True,
            title=f"Rank history: {selected_kw}",
        )
        fig.update_yaxes(autorange="reversed", title="Rank (lower is better)")
        fig.update_xaxes(title="Date")
        st.plotly_chart(fig, use_container_width=True)

# --- Top 20 bar chart ---
st.subheader("Top 20 Keywords")
top20 = get_top_keywords(selected_date_str, 20)
if not top20.empty:
    fig = px.bar(
        top20,
        x="keyword",
        y="rank",
        text_auto=True,
        title="Top 20 Keywords by Rank",
    )
    fig.update_yaxes(autorange="reversed", title="Rank")
    fig.update_layout(xaxis_tickangle=-45)
    st.plotly_chart(fig, use_container_width=True)
