"""Fashion Trend Analyzer — main Streamlit entry point."""
from __future__ import annotations

import streamlit as st

st.set_page_config(
    page_title="Fashion Trend Analyzer",
    page_icon="\U0001F4C8",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("Fashion Trend Analyzer")
st.markdown(
    """
    Real-time fashion trend tracking across **Musinsa**, **29CM**,
    **W Concept**, and **Zigzag**.

    Use the sidebar to navigate between pages:

    | Page | Description |
    |------|-------------|
    | **Overview** | KPIs, scraper health, platform breakdown |
    | **Keywords** | Trending search keywords and rank changes |
    | **Bestsellers** | Top products, brand frequency, pricing |
    | **Instagram** | Hashtag post-count tracking |
    | **Cross-Platform** | Brand overlap and price comparison |

    Data is refreshed every scrape cycle and cached for 5 minutes.
    """
)
