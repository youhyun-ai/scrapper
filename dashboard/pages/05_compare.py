"""Cross-Platform comparison dashboard."""
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
def get_cross_platform_brands() -> pd.DataFrame:
    """Brands that appear on more than one platform."""
    conn = _conn()
    df = pd.read_sql_query(
        """
        SELECT brand, GROUP_CONCAT(DISTINCT platform) AS platforms,
               COUNT(DISTINCT platform) AS platform_count
        FROM bestseller_rankings
        WHERE brand IS NOT NULL AND brand != ''
        GROUP BY brand
        HAVING platform_count > 1
        ORDER BY platform_count DESC, brand
        """,
        conn,
    )
    conn.close()
    return df


@st.cache_data(ttl=300)
def get_avg_price_by_platform() -> pd.DataFrame:
    conn = _conn()
    df = pd.read_sql_query(
        """
        SELECT platform, ROUND(AVG(price)) AS avg_price
        FROM bestseller_rankings
        WHERE price IS NOT NULL AND price > 0
        GROUP BY platform
        ORDER BY avg_price DESC
        """,
        conn,
    )
    conn.close()
    return df


@st.cache_data(ttl=300)
def get_avg_discount_by_platform() -> pd.DataFrame:
    conn = _conn()
    df = pd.read_sql_query(
        """
        SELECT platform, ROUND(AVG(discount_pct), 1) AS avg_discount
        FROM bestseller_rankings
        WHERE discount_pct IS NOT NULL AND discount_pct > 0
        GROUP BY platform
        ORDER BY avg_discount DESC
        """,
        conn,
    )
    conn.close()
    return df


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

st.header("Cross-Platform Comparison")

# --- Brands across platforms ---
st.subheader("Brands Appearing on Multiple Platforms")
cross_brands = get_cross_platform_brands()
if cross_brands.empty:
    st.info("No brands found on multiple platforms yet.")
else:
    st.dataframe(cross_brands, use_container_width=True, hide_index=True)

# --- Average price comparison ---
st.subheader("Average Price by Platform")
avg_price = get_avg_price_by_platform()
if avg_price.empty:
    st.info("No pricing data available.")
else:
    fig = px.bar(
        avg_price,
        x="platform",
        y="avg_price",
        color="platform",
        text_auto=True,
        title="Average Product Price by Platform (KRW)",
    )
    fig.update_layout(showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

# --- Average discount comparison ---
st.subheader("Average Discount by Platform")
avg_disc = get_avg_discount_by_platform()
if avg_disc.empty:
    st.info("No discount data available.")
else:
    fig = px.bar(
        avg_disc,
        x="platform",
        y="avg_discount",
        color="platform",
        text_auto=True,
        title="Average Discount by Platform (%)",
    )
    fig.update_layout(showlegend=False)
    st.plotly_chart(fig, use_container_width=True)
