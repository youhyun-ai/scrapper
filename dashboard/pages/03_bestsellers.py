"""Bestsellers dashboard."""
from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

DB_PATH = Path(__file__).resolve().parents[2] / "data" / "trends.db"

PLATFORM_LABELS = {
    "All": None,
    "Musinsa": "musinsa",
    "29CM": "twentynine_cm",
    "W Concept": "wconcept",
    "Zigzag": "zigzag",
}


def _conn() -> sqlite3.Connection:
    return sqlite3.connect(str(DB_PATH))


# ---------------------------------------------------------------------------
# Cached queries
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300)
def get_snapshot_dates() -> list[str]:
    conn = _conn()
    df = pd.read_sql_query(
        "SELECT DISTINCT snapshot_date FROM bestseller_rankings ORDER BY snapshot_date DESC",
        conn,
    )
    conn.close()
    return df["snapshot_date"].tolist()


@st.cache_data(ttl=300)
def get_bestsellers(snapshot_date: str, platform: str | None) -> pd.DataFrame:
    conn = _conn()
    if platform:
        df = pd.read_sql_query(
            """
            SELECT rank, brand, product_name, price, discount_pct, platform
            FROM bestseller_rankings
            WHERE snapshot_date = ? AND platform = ?
            ORDER BY rank
            """,
            conn,
            params=(snapshot_date, platform),
        )
    else:
        df = pd.read_sql_query(
            """
            SELECT rank, brand, product_name, price, discount_pct, platform
            FROM bestseller_rankings
            WHERE snapshot_date = ?
            ORDER BY platform, rank
            """,
            conn,
            params=(snapshot_date,),
        )
    conn.close()
    return df


@st.cache_data(ttl=300)
def get_top_brands(limit: int = 10) -> pd.DataFrame:
    conn = _conn()
    df = pd.read_sql_query(
        """
        SELECT brand, COUNT(*) AS appearances
        FROM bestseller_rankings
        WHERE brand IS NOT NULL AND brand != ''
        GROUP BY brand
        ORDER BY appearances DESC
        LIMIT ?
        """,
        conn,
        params=(limit,),
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
        """,
        conn,
    )
    conn.close()
    return df


@st.cache_data(ttl=300)
def get_discount_distribution() -> pd.DataFrame:
    conn = _conn()
    df = pd.read_sql_query(
        """
        SELECT discount_pct
        FROM bestseller_rankings
        WHERE discount_pct IS NOT NULL AND discount_pct > 0
        """,
        conn,
    )
    conn.close()
    return df


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

st.header("Bestsellers")

dates = get_snapshot_dates()
if not dates:
    st.info("No bestseller data available yet.")
    st.stop()

col_a, col_b = st.columns(2)
with col_a:
    platform_label = st.selectbox("Platform", list(PLATFORM_LABELS.keys()))
with col_b:
    selected_date = st.date_input(
        "Snapshot date",
        value=datetime.strptime(dates[0], "%Y-%m-%d").date(),
        min_value=datetime.strptime(dates[-1], "%Y-%m-%d").date(),
        max_value=datetime.strptime(dates[0], "%Y-%m-%d").date(),
    )

platform_val = PLATFORM_LABELS[platform_label]
selected_date_str = selected_date.strftime("%Y-%m-%d")

bs_df = get_bestsellers(selected_date_str, platform_val)
if bs_df.empty:
    st.warning("No data for the selected filters.")
    st.stop()

st.subheader("Product Rankings")
st.dataframe(bs_df, use_container_width=True, hide_index=True)

# --- Top 10 brands ---
st.subheader("Top 10 Brands (all platforms, all dates)")
brands = get_top_brands(10)
if not brands.empty:
    fig = px.bar(
        brands,
        x="brand",
        y="appearances",
        text_auto=True,
        title="Most Frequent Brands in Bestseller Lists",
    )
    fig.update_layout(xaxis_tickangle=-45)
    st.plotly_chart(fig, use_container_width=True)

# --- Average price by platform ---
st.subheader("Average Price by Platform")
avg_price = get_avg_price_by_platform()
if not avg_price.empty:
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

# --- Discount distribution ---
st.subheader("Discount Distribution")
disc = get_discount_distribution()
if disc.empty:
    st.info("No discount data available.")
else:
    fig = px.histogram(
        disc,
        x="discount_pct",
        nbins=20,
        title="Discount Percentage Distribution",
        labels={"discount_pct": "Discount (%)"},
    )
    st.plotly_chart(fig, use_container_width=True)
