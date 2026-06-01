"""Shared dark plotly theme for the Bloomberg-style dashboard."""
from __future__ import annotations

import plotly.graph_objects as go
import plotly.io as pio

# Background colors matching the Streamlit CSS
BG_PRIMARY = "#0a0e17"
BG_CARD = "#161b22"
BG_GRID = "#1a2332"

# Accent palette — blue-forward, Bloomberg-inspired
BLUE = "#1f6feb"
CYAN = "#58a6ff"
GREEN = "#3fb950"
RED = "#f85149"
ORANGE = "#d29922"
PURPLE = "#bc8cff"
GRAY = "#8b949e"
TEXT = "#c9d1d9"
TEXT_DIM = "#8b949e"

ACCENT_SEQUENCE = [CYAN, GREEN, ORANGE, PURPLE, RED, "#79c0ff", "#d2a8ff", "#ffa657"]

DARK_TEMPLATE = go.layout.Template(
    layout=go.Layout(
        paper_bgcolor=BG_PRIMARY,
        plot_bgcolor=BG_PRIMARY,
        font=dict(color=TEXT, family="Inter, -apple-system, sans-serif", size=12),
        title=dict(font=dict(color=TEXT, size=14)),
        xaxis=dict(
            gridcolor=BG_GRID,
            linecolor=BG_GRID,
            zerolinecolor=BG_GRID,
            tickfont=dict(color=TEXT_DIM),
        ),
        yaxis=dict(
            gridcolor=BG_GRID,
            linecolor=BG_GRID,
            zerolinecolor=BG_GRID,
            tickfont=dict(color=TEXT_DIM),
        ),
        legend=dict(
            bgcolor="rgba(1,1,1,1)",
            font=dict(color=TEXT_DIM),
        ),
        colorway=ACCENT_SEQUENCE,
        hovermode="x unified",
    )
)

pio.templates["dark_bloomberg"] = DARK_TEMPLATE
pio.templates.default = "dark_bloomberg"


def dark_chart(fig: go.Figure, height: int = 315) -> go.Figure:
    """Apply dark theme overrides to a plotly figure."""
    fig.update_layout(
        template=DARK_TEMPLATE,
        height=height,
        margin=dict(t=15, b=40, l=30, r=20),
    )
    return fig


def dark_hline(fig: go.Figure, y: float, color: str = GRAY, text: str = "") -> go.Figure:
    fig.add_hline(y=y, line_dash="dash", line_color=color,
                  annotation_text=text, annotation_font_color=TEXT_DIM)
    return fig
