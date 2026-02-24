"""Overview dashboard — KPIs, scraper health, platform breakdown."""
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
def get_kpi_data() -> dict:
    conn = _conn()
    total_products = pd.read_sql_query(
        "SELECT COUNT(DISTINCT product_name) AS n FROM bestseller_rankings", conn
    ).iloc[0]["n"]
    total_keywords = pd.read_sql_query(
        "SELECT COUNT(DISTINCT keyword) AS n FROM keyword_rankings", conn
    ).iloc[0]["n"]
    active_platforms = pd.read_sql_query(
        "SELECT COUNT(DISTINCT platform) AS n FROM bestseller_rankings", conn
    ).iloc[0]["n"]
    conn.close()
    return {
        "total_products": int(total_products),
        "total_keywords": int(total_keywords),
        "active_platforms": int(active_platforms),
    }


@st.cache_data(ttl=300)
def get_scrape_log() -> pd.DataFrame:
    conn = _conn()
    df = pd.read_sql_query(
        """
        SELECT platform, status, items_collected, error_message,
               ROUND(duration_seconds, 2) AS duration_seconds, scraped_at
        FROM scrape_log
        WHERE id IN (
            SELECT MAX(id) FROM scrape_log GROUP BY platform
        )
        ORDER BY scraped_at DESC
        """,
        conn,
    )
    conn.close()
    return df


@st.cache_data(ttl=300)
def get_platform_breakdown() -> pd.DataFrame:
    conn = _conn()
    df = pd.read_sql_query(
        """
        SELECT platform, COUNT(*) AS items
        FROM bestseller_rankings
        GROUP BY platform
        ORDER BY items DESC
        """,
        conn,
    )
    conn.close()
    return df


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

st.header("Overview")

kpi = get_kpi_data()
log_df = get_scrape_log()

last_status = "N/A"
if not log_df.empty:
    statuses = log_df["status"].unique()
    last_status = "All OK" if all(s == "success" for s in statuses) else "Issues detected"

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Products Tracked", kpi["total_products"])
col2.metric("Total Keywords", kpi["total_keywords"])
col3.metric("Platforms Active", kpi["active_platforms"])
col4.metric("Last Scrape Status", last_status)

st.subheader("Scraper Health")
if log_df.empty:
    st.info("No scrape logs found yet.")
else:
    st.dataframe(log_df, use_container_width=True, hide_index=True)

st.subheader("Items per Platform")
breakdown = get_platform_breakdown()
if breakdown.empty:
    st.info("No bestseller data yet.")
else:
    fig = px.bar(
        breakdown,
        x="platform",
        y="items",
        color="platform",
        text_auto=True,
        title="Bestseller Items by Platform",
    )
    fig.update_layout(showlegend=False)
    st.plotly_chart(fig, use_container_width=True)
