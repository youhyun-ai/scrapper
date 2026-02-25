"""Shared UI theme, constants, and helpers for the dashboard."""
from __future__ import annotations

import sqlite3
from pathlib import Path

import streamlit as st

DB_PATH = Path(__file__).resolve().parent / "data" / "trends.db"

# ---------------------------------------------------------------------------
# Platform display names & colors
# ---------------------------------------------------------------------------

PLATFORM_DISPLAY = {
    "musinsa": "무신사",
    "twentynine_cm": "29CM",
    "wconcept": "W컨셉",
    "zigzag": "지그재그",
}

PLATFORM_COLORS = {
    "musinsa": "#1A1A1A",
    "twentynine_cm": "#FF6B4A",
    "wconcept": "#1B3A6B",
    "zigzag": "#FF2D78",
}

PLATFORM_COLOR_LIST = ["#1A1A1A", "#FF6B4A", "#1B3A6B", "#FF2D78"]

# Plotly color sequence for charts
CHART_COLORS = ["#6366f1", "#f43f5e", "#0ea5e9", "#10b981", "#f59e0b", "#8b5cf6", "#ec4899", "#14b8a6"]

# ---------------------------------------------------------------------------
# DB helper
# ---------------------------------------------------------------------------

def get_conn() -> sqlite3.Connection:
    return sqlite3.connect(str(DB_PATH))


# ---------------------------------------------------------------------------
# Display helpers
# ---------------------------------------------------------------------------

def platform_name(code: str) -> str:
    """Convert internal platform code to display name."""
    return PLATFORM_DISPLAY.get(code, code)


def format_price(price) -> str:
    """Format price with comma and won symbol."""
    if price is None or price == 0:
        return "-"
    return f"₩{int(price):,}"


def format_pct(pct) -> str:
    """Format percentage."""
    if pct is None or pct == 0:
        return "-"
    return f"{pct:.0f}%"


# ---------------------------------------------------------------------------
# Chart styling
# ---------------------------------------------------------------------------

CHART_LAYOUT = dict(
    font=dict(family="Pretendard, -apple-system, sans-serif", size=13),
    title_font_size=16,
    margin=dict(l=24, r=24, t=56, b=72),
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
    colorway=CHART_COLORS,
    hoverlabel=dict(font_size=13),
)


def style_chart(fig, height: int = 420):
    """Apply common chart styling."""
    # Ensure title is not undefined — set to empty string if not present
    current_title = fig.layout.title
    if current_title is None or (hasattr(current_title, 'text') and current_title.text is None):
        fig.update_layout(title="")
    fig.update_layout(height=height, **CHART_LAYOUT)
    fig.update_xaxes(gridcolor="rgba(128,128,128,0.1)")
    fig.update_yaxes(gridcolor="rgba(128,128,128,0.15)")
    return fig


# ---------------------------------------------------------------------------
# CSS injection
# ---------------------------------------------------------------------------

def inject_global_css():
    """Inject custom CSS to polish the Streamlit UI."""
    st.markdown("""
    <style>
    /* ── Global typography ─────────────────────────────── */
    @import url('https://cdn.jsdelivr.net/gh/orioncactus/pretendard/dist/web/static/pretendard.css');

    html, body, [class*="css"] {
        font-family: 'Pretendard', -apple-system, BlinkMacSystemFont, sans-serif;
    }

    /* ── Metric cards ──────────────────────────────────── */
    [data-testid="stMetric"] {
        background: linear-gradient(135deg, rgba(99,102,241,0.08) 0%, rgba(139,92,246,0.06) 100%);
        border: 1px solid rgba(99,102,241,0.15);
        border-radius: 12px;
        padding: 16px 20px;
        transition: transform 0.15s ease, box-shadow 0.15s ease;
    }
    [data-testid="stMetric"]:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 16px rgba(99,102,241,0.12);
    }
    [data-testid="stMetricLabel"] {
        font-size: 0.82rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.03em;
        opacity: 0.7;
    }
    [data-testid="stMetricValue"] {
        font-size: 1.6rem;
        font-weight: 700;
    }

    /* ── Data tables ───────────────────────────────────── */
    [data-testid="stDataFrame"] {
        border-radius: 12px;
        overflow: hidden;
    }

    /* ── Buttons ───────────────────────────────────────── */
    .stDownloadButton > button {
        border-radius: 8px;
        font-weight: 600;
        font-size: 0.85rem;
        padding: 6px 20px;
        border: 1px solid rgba(99,102,241,0.3);
        background: rgba(99,102,241,0.08);
        transition: all 0.15s ease;
    }
    .stDownloadButton > button:hover {
        background: rgba(99,102,241,0.15);
        border-color: rgba(99,102,241,0.5);
    }

    /* ── Tabs ──────────────────────────────────────────── */
    .stTabs [data-baseweb="tab-list"] {
        gap: 4px;
    }
    .stTabs [data-baseweb="tab"] {
        border-radius: 8px 8px 0 0;
        font-weight: 600;
        padding: 8px 20px;
    }

    /* ── Expander ──────────────────────────────────────── */
    [data-testid="stExpander"] {
        border-radius: 12px;
        border: 1px solid rgba(128,128,128,0.15);
    }

    /* ── Section dividers ──────────────────────────────── */
    hr {
        border: none;
        height: 1px;
        background: linear-gradient(90deg, transparent, rgba(99,102,241,0.2), transparent);
        margin: 2rem 0;
    }

    /* ── Sidebar styling ──────────────────────────────── */
    [data-testid="stSidebar"] [data-testid="stMarkdown"] p {
        font-size: 0.85rem;
    }

    /* ── Selectbox / Multiselect ───────────────────────── */
    [data-baseweb="select"] {
        border-radius: 8px;
    }

    /* ── Info / Warning boxes ──────────────────────────── */
    .stAlert {
        border-radius: 10px;
    }

    /* ── Hero card helper ──────────────────────────────── */
    .hero-card {
        background: linear-gradient(135deg, rgba(99,102,241,0.1) 0%, rgba(14,165,233,0.08) 100%);
        border: 1px solid rgba(99,102,241,0.15);
        border-radius: 14px;
        padding: 20px 24px;
        margin-bottom: 12px;
    }
    .hero-card h3 {
        margin: 0 0 4px 0;
        font-size: 1rem;
        font-weight: 600;
        opacity: 0.7;
    }
    .hero-card .hero-value {
        font-size: 1.8rem;
        font-weight: 800;
        color: #6366f1;
        margin: 0;
    }
    .hero-card .hero-sub {
        font-size: 0.82rem;
        opacity: 0.6;
        margin: 4px 0 0 0;
    }

    /* ── Status badge ──────────────────────────────────── */
    .status-badge {
        display: inline-block;
        padding: 3px 10px;
        border-radius: 20px;
        font-size: 0.78rem;
        font-weight: 700;
        letter-spacing: 0.02em;
    }
    .status-success {
        background: rgba(16,185,129,0.15);
        color: #059669;
    }
    .status-failed {
        background: rgba(244,63,94,0.15);
        color: #e11d48;
    }

    /* ── Product card ──────────────────────────────────── */
    .product-card {
        border: 1px solid rgba(128,128,128,0.15);
        border-radius: 12px;
        overflow: hidden;
        transition: transform 0.15s ease, box-shadow 0.15s ease;
        height: 100%;
    }
    .product-card:hover {
        transform: translateY(-3px);
        box-shadow: 0 6px 20px rgba(0,0,0,0.08);
    }
    .product-card img {
        width: 100%;
        height: 200px;
        object-fit: cover;
    }
    .product-card .card-body {
        padding: 12px 14px;
    }
    .product-card .card-brand {
        font-size: 0.75rem;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.04em;
        opacity: 0.5;
        margin-bottom: 4px;
    }
    .product-card .card-name {
        font-size: 0.85rem;
        font-weight: 600;
        line-height: 1.3;
        margin-bottom: 8px;
        display: -webkit-box;
        -webkit-line-clamp: 2;
        -webkit-box-orient: vertical;
        overflow: hidden;
    }
    .product-card .card-price {
        font-size: 1rem;
        font-weight: 800;
    }
    .product-card .card-original-price {
        font-size: 0.78rem;
        text-decoration: line-through;
        opacity: 0.45;
        margin-left: 6px;
    }
    .product-card .discount-badge {
        display: inline-block;
        background: rgba(244,63,94,0.12);
        color: #e11d48;
        font-size: 0.75rem;
        font-weight: 800;
        padding: 2px 8px;
        border-radius: 6px;
        margin-left: 6px;
    }
    .product-card .card-rank {
        position: absolute;
        top: 8px;
        left: 8px;
        background: rgba(0,0,0,0.75);
        color: white;
        font-size: 0.75rem;
        font-weight: 800;
        padding: 3px 9px;
        border-radius: 6px;
    }
    .product-card .card-platform {
        display: inline-block;
        font-size: 0.7rem;
        font-weight: 600;
        padding: 2px 8px;
        border-radius: 6px;
        margin-top: 6px;
        background: rgba(99,102,241,0.1);
        color: #6366f1;
    }

    /* ── Insight card ──────────────────────────────────── */
    .insight-card {
        background: rgba(99,102,241,0.04);
        border-left: 3px solid #6366f1;
        border-radius: 0 10px 10px 0;
        padding: 14px 18px;
        margin-bottom: 10px;
    }
    .insight-card p {
        margin: 0;
        font-size: 0.9rem;
        line-height: 1.5;
    }

    /* ── Rank change indicators ────────────────────────── */
    .rank-up { color: #059669; font-weight: 700; }
    .rank-down { color: #e11d48; font-weight: 700; }
    .rank-same { color: #9ca3af; }

    /* ── Section header ────────────────────────────────── */
    .section-header {
        display: flex;
        align-items: center;
        gap: 10px;
        margin: 24px 0 16px 0;
    }
    .section-header .section-icon {
        font-size: 1.4rem;
    }
    .section-header h2 {
        margin: 0;
        font-size: 1.3rem;
        font-weight: 700;
    }
    </style>
    """, unsafe_allow_html=True)


def hero_card(title: str, value: str, subtitle: str = "") -> str:
    """Return HTML for a hero metric card."""
    sub_html = f'<p class="hero-sub">{subtitle}</p>' if subtitle else ""
    return f"""
    <div class="hero-card">
        <h3>{title}</h3>
        <p class="hero-value">{value}</p>
        {sub_html}
    </div>
    """


def status_badge(status: str) -> str:
    """Return HTML for a status badge."""
    if status == "success":
        return '<span class="status-badge status-success">SUCCESS</span>'
    return '<span class="status-badge status-failed">FAILED</span>'


def product_card_html(rank, brand, name, price, original_price, discount_pct, image_url, platform, product_url="") -> str:
    """Return HTML for a product card."""
    brand_display = brand if brand else ""
    price_display = f"₩{int(price):,}" if price and price > 0 else ""

    orig_html = ""
    if original_price and original_price > price and original_price > 0:
        orig_html = f'<span class="card-original-price">₩{int(original_price):,}</span>'

    disc_html = ""
    if discount_pct and discount_pct > 0:
        disc_html = f'<span class="discount-badge">{int(discount_pct)}% OFF</span>'

    img_html = ""
    if image_url and str(image_url).startswith("http"):
        img_html = f'<img src="{image_url}" alt="{name}" loading="lazy" onerror="this.style.display=\'none\'">'
    else:
        img_html = '<div style="height:200px;background:linear-gradient(135deg,#f0f0f0,#e0e0e0);display:flex;align-items:center;justify-content:center;color:#aaa;font-size:0.85rem;">No Image</div>'

    plat_display = PLATFORM_DISPLAY.get(platform, platform)

    return f"""
    <div class="product-card" style="position:relative;">
        <div class="card-rank">#{rank}</div>
        {img_html}
        <div class="card-body">
            <div class="card-brand">{brand_display}</div>
            <div class="card-name">{name}</div>
            <div>
                <span class="card-price">{price_display}</span>
                {orig_html}
                {disc_html}
            </div>
            <span class="card-platform">{plat_display}</span>
        </div>
    </div>
    """


def section_header(icon: str, title: str):
    """Render a styled section header."""
    st.markdown(f"""
    <div class="section-header">
        <span class="section-icon">{icon}</span>
        <h2>{title}</h2>
    </div>
    """, unsafe_allow_html=True)
