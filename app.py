"""
Data Quality Intelligence Platform
=====================================
Your data looks clean. It isn't.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import time, csv, os, re, math
from datetime import datetime

st.set_page_config(
    page_title="DataQuality.ai — Is your data really clean?",
    page_icon="🔬",
    layout="wide",
    initial_sidebar_state="collapsed",
)

from data_quality_engine import detect_join_keys, check_orphan_records, check_entity_duplicates, check_process_gaps
from scoring import calculate_scores, generate_recommendations, score_label, overall_grade
from semantic import (detect_entity, detect_domain, estimate_monetary_impact,
                      generate_narrative, classify_columns)

# ─────────────────────────────────────────────────────────────────────────────
# CSS — Dark tactical dashboard
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&family=JetBrains+Mono:wght@400;600&display=swap');

html, body, [class*="css"] { font-family: 'Inter', sans-serif !important; }
#MainMenu, footer, .stDeployButton { display: none !important; }

/* ── Hero ── */
.hero {
    background: linear-gradient(135deg, #010409 0%, #0D1117 40%, #161B22 100%);
    border: 1px solid #21262D;
    border-radius: 16px;
    padding: 56px 48px 48px;
    margin-bottom: 32px;
    position: relative;
    overflow: hidden;
}
.hero::before {
    content: '';
    position: absolute; top: 0; left: 0; right: 0; height: 3px;
    background: linear-gradient(90deg, #F85149, #F0883E, #E3B341, #3FB950, #58A6FF);
}
.hero-eyebrow {
    font-size: 11px; font-weight: 700; letter-spacing: 2px;
    text-transform: uppercase; color: #F85149; margin-bottom: 14px;
}
.hero h1 {
    font-size: 48px; font-weight: 900; line-height: 1.1;
    color: #E6EDF3; margin: 0 0 16px; letter-spacing: -1px;
}
.hero h1 span { color: #F85149; }
.hero-sub {
    font-size: 17px; color: #8B949E; line-height: 1.6;
    max-width: 640px; margin-bottom: 32px;
}
.stat-row { display: flex; gap: 32px; flex-wrap: wrap; }
.stat-item { text-align: left; }
.stat-num { font-size: 28px; font-weight: 800; color: #E6EDF3; font-family: 'JetBrains Mono', monospace; }
.stat-lbl { font-size: 12px; color: #6E7681; margin-top: 2px; }

/* ── Cards ── */
.card {
    background: #161B22;
    border: 1px solid #21262D;
    border-radius: 12px;
    padding: 24px;
}
.card-title {
    font-size: 11px; font-weight: 700; letter-spacing: 1.5px;
    text-transform: uppercase; color: #6E7681; margin-bottom: 16px;
}

/* ── Score ── */
.score-grade {
    font-size: 96px; font-weight: 900; line-height: 1;
    font-family: 'JetBrains Mono', monospace;
}
.score-label { font-size: 18px; font-weight: 700; margin-top: 4px; }
.benchmark-badge {
    display: inline-block;
    background: #21262D; border: 1px solid #30363D;
    border-radius: 999px; padding: 4px 14px;
    font-size: 12px; color: #8B949E; margin-top: 12px;
}

/* ── Dimension bars ── */
.dim-row { margin-bottom: 18px; }
.dim-header { display: flex; justify-content: space-between; margin-bottom: 6px; }
.dim-name { font-size: 13px; font-weight: 600; color: #C9D1D9; }
.dim-val  { font-size: 13px; font-weight: 700; font-family: 'JetBrains Mono', monospace; }
.dim-sub  { font-size: 11px; color: #6E7681; margin-top: 2px; }
.dim-track { background: #21262D; border-radius: 999px; height: 8px; overflow: hidden; }
.dim-fill  { border-radius: 999px; height: 8px; }

/* ── Narrative insights ── */
.insight-card {
    background: #0D1117;
    border: 1px solid #21262D;
    border-radius: 10px;
    padding: 18px 20px;
    margin-bottom: 12px;
}
.insight-title { font-size: 14px; font-weight: 700; color: #E6EDF3; margin-bottom: 6px; }
.insight-text  { font-size: 13px; color: #8B949E; line-height: 1.6; }

/* ── Impact box ── */
.impact-box {
    background: linear-gradient(135deg, #1a0a0a, #1C1000);
    border: 1px solid #F85149;
    border-radius: 12px;
    padding: 24px 28px;
    margin-bottom: 20px;
}
.impact-total {
    font-size: 42px; font-weight: 900; color: #F85149;
    font-family: 'JetBrains Mono', monospace;
}
.impact-row { display: flex; justify-content: space-between; align-items: center;
    padding: 10px 0; border-bottom: 1px solid #21262D; font-size: 13px; }
.impact-row:last-child { border-bottom: none; }

/* ── Findings ── */
.finding {
    background: #0D1117;
    border: 1px solid #21262D;
    border-radius: 12px;
    padding: 24px;
    margin-bottom: 16px;
    border-left: 4px solid;
    position: relative;
}
.finding-critical { border-left-color: #F85149; }
.finding-high     { border-left-color: #F0883E; }
.finding-medium   { border-left-color: #E3B341; }
.finding-headline {
    font-size: 28px; font-weight: 900; line-height: 1.1;
    font-family: 'JetBrains Mono', monospace;
    margin-bottom: 6px;
}
.finding-title { font-size: 15px; font-weight: 700; color: #E6EDF3; margin-bottom: 8px; }
.finding-detail { font-size: 13px; color: #8B949E; line-height: 1.5; }
.finding-examples { margin-top: 10px; }
.ex-code {
    display: inline-block; background: #161B22; border: 1px solid #30363D;
    border-radius: 4px; padding: 2px 8px; font-size: 11px; color: #79C0FF;
    font-family: 'JetBrains Mono', monospace; margin: 2px 4px 2px 0;
}
.sev-badge {
    display: inline-block; font-size: 10px; font-weight: 700;
    padding: 2px 10px; border-radius: 999px; text-transform: uppercase;
    letter-spacing: 0.8px; margin-bottom: 10px;
}

/* ── Recommendations ── */
.rec-teaser {
    background: #0D1117;
    border: 1px solid #21262D;
    border-radius: 10px;
    padding: 20px;
    margin-bottom: 10px;
}
.rec-full {
    border-radius: 10px;
    padding: 20px;
    margin-bottom: 14px;
    border: 1px solid;
}
.rec-blur {
    filter: blur(5px); pointer-events: none; user-select: none;
    background: #0D1117; border: 1px solid #21262D;
    border-radius: 10px; padding: 20px; opacity: 0.4; margin-bottom: 10px;
}

/* ── Locked box ── */
.locked-box {
    background: linear-gradient(135deg, #0D1117, #161B22);
    border: 1px solid #F85149;
    border-radius: 14px;
    padding: 40px;
    text-align: center;
    margin: 24px 0;
    position: relative;
}
.locked-box::before {
    content: '';
    position: absolute; top: 0; left: 0; right: 0; height: 2px;
    background: linear-gradient(90deg, #F85149, #F0883E);
    border-radius: 14px 14px 0 0;
}

/* ── Flow map ── */
.flow-section {
    background: #0D1117;
    border: 1px solid #21262D;
    border-radius: 12px;
    padding: 4px;
    margin-bottom: 20px;
}

/* ── Section headers ── */
.section-header {
    font-size: 11px; font-weight: 700; letter-spacing: 2px;
    text-transform: uppercase; color: #6E7681;
    margin-bottom: 6px;
}
.section-title {
    font-size: 22px; font-weight: 800; color: #E6EDF3;
    margin-bottom: 4px;
}
.section-sub {
    font-size: 14px; color: #6E7681; margin-bottom: 20px; line-height: 1.5;
}

/* ── Upload area ── */
div[data-testid="stFileUploader"] {
    background: #161B22 !important;
    border: 1px dashed #30363D !important;
    border-radius: 12px !important;
}

/* ── Divider ── */
.dq-divider {
    border: none; border-top: 1px solid #21262D; margin: 32px 0;
}
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def score_color(s):
    if s is None: return "#6E7681"
    if s >= 85:   return "#3FB950"
    if s >= 75:   return "#56D364"
    if s >= 65:   return "#E3B341"
    if s >= 50:   return "#F0883E"
    return               "#F85149"

SEV = {
    "critical": {"bg": "#3d0f0f", "border": "#F85149", "text": "#F85149", "badge_bg": "#F85149", "badge_fg": "#010409"},
    "high":     {"bg": "#2d1500", "border": "#F0883E", "text": "#F0883E", "badge_bg": "#F0883E", "badge_fg": "#010409"},
    "medium":   {"bg": "#2a1d00", "border": "#E3B341", "text": "#E3B341", "badge_bg": "#E3B341", "badge_fg": "#010409"},
}

DIMS = [
    ("completeness", "Completeness",  "Non-null rate"),
    ("uniqueness",   "Uniqueness",    "Duplicate-free rate"),
    ("validity",     "Validity",      "Format & range conformance"),
    ("consistency",  "Consistency",   "Cross-file integrity"),
    ("timeliness",   "Timeliness",    "Data freshness"),
]


def _dl_png_btn(fig: go.Figure, filename: str, label: str = "⬇ Download PNG"):
    """Render a small download button that exports a Plotly figure as PNG."""
    try:
        img_bytes = fig.to_image(format="png", scale=2)
        st.download_button(
            label=label,
            data=img_bytes,
            file_name=filename,
            mime="image/png",
            key=f"dl_{filename}_{id(fig)}",
        )
    except Exception:
        pass  # kaleido not available — silently skip



def make_speedometer(score: float) -> go.Figure:
    """Semicircle speedometer gauge — red left, green right."""
    c = score_color(score)
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=score,
        number={"font": {"size": 64, "color": c, "family": "JetBrains Mono"},
                "suffix": ""},
        gauge={
            "axis": {
                "range": [0, 100],
                "tickwidth": 1.5,
                "tickcolor": "#30363D",
                "tickfont": {"color": "#6E7681", "size": 11},
                "dtick": 25,
            },
            "bar":  {"color": c, "thickness": 0.28},
            "bgcolor": "rgba(0,0,0,0)",
            "borderwidth": 0,
            "steps": [
                {"range": [0,  50],  "color": "#2d0808"},
                {"range": [50, 65],  "color": "#2d1500"},
                {"range": [65, 75],  "color": "#2a1d00"},
                {"range": [75, 85],  "color": "#0a2010"},
                {"range": [85, 100], "color": "#051a10"},
            ],
            "threshold": {
                "line": {"color": "#E6EDF3", "width": 3},
                "thickness": 0.82,
                "value": score,
            },
        },
    ))
    fig.update_layout(
        height=300,
        margin=dict(t=30, b=5, l=30, r=30),
        paper_bgcolor="#0D1117",
        plot_bgcolor="#0D1117",
        font={"family": "Inter", "color": "#E6EDF3"},
    )
    return fig


# ── DB connector config ───────────────────────────────────────────────────────
_DB_CONFIGS = {
    "❄️  Snowflake": {
        "color": "#29B5E8",
        "fields": [
            ("Account identifier", "myorg-myaccount.snowflakecomputing.com", False),
            ("Warehouse",          "COMPUTE_WH",    False),
            ("Database",           "PRODUCTION",    False),
            ("Schema",             "PUBLIC",        False),
            ("Username",           "you@company.com", False),
            ("Password",           "",              True),
        ],
    },
    "☁️  BigQuery": {
        "color": "#4285F4",
        "fields": [
            ("Project ID",          "my-project-123456", False),
            ("Dataset",             "analytics",         False),
            ("Location",            "US",                False),
            ("Service Account JSON","Paste JSON key...", False),
        ],
    },
    "🔴  Amazon Redshift": {
        "color": "#E6182D",
        "fields": [
            ("Host",     "mycluster.us-east-1.redshift.amazonaws.com", False),
            ("Port",     "5439",    False),
            ("Database", "dev",     False),
            ("Username", "awsuser", False),
            ("Password", "",        True),
        ],
    },
    "🐘  PostgreSQL": {
        "color": "#336791",
        "fields": [
            ("Host",     "localhost", False),
            ("Port",     "5432",      False),
            ("Database", "mydb",      False),
            ("Username", "postgres",  False),
            ("Password", "",          True),
        ],
    },
    "🐬  MySQL": {
        "color": "#00758F",
        "fields": [
            ("Host",     "localhost", False),
            ("Port",     "3306",      False),
            ("Database", "mydb",      False),
            ("Username", "root",      False),
            ("Password", "",          True),
        ],
    },
    "⬦  Databricks": {
        "color": "#FF3621",
        "fields": [
            ("Server hostname", "adb-xxxx.azuredatabricks.net", False),
            ("HTTP path",       "/sql/1.0/warehouses/xxxx",     False),
            ("Catalog",         "main",    False),
            ("Schema",          "default", False),
            ("Access token",    "",        True),
        ],
    },
}


def render_db_connector():
    """Fake enterprise database connector — visual placeholder."""
    st.markdown("""
    <div style="background:#0D1117;border:1px dashed #30363D;border-radius:12px;
                padding:28px 32px;margin-top:8px">
      <div style="font-size:11px;font-weight:700;letter-spacing:1.5px;
                  text-transform:uppercase;color:#6E7681;margin-bottom:18px">
        Select Database
      </div>
    """, unsafe_allow_html=True)

    db_name = st.selectbox(
        "db", list(_DB_CONFIGS.keys()),
        label_visibility="collapsed",
        key="db_type_select",
    )
    cfg = _DB_CONFIGS[db_name]
    color = cfg["color"]

    st.markdown(
        f'<div style="height:3px;background:{color};border-radius:2px;'
        f'margin:12px 0 20px"></div>',
        unsafe_allow_html=True,
    )

    fields = cfg["fields"]
    mid = math.ceil(len(fields) / 2)
    left_fields, right_fields = fields[:mid], fields[mid:]

    col_l, col_r = st.columns(2, gap="medium")
    for col, flist in [(col_l, left_fields), (col_r, right_fields)]:
        with col:
            for label, placeholder, is_pw in flist:
                if is_pw:
                    st.text_input(label, placeholder="••••••••",
                                  type="password", key=f"db_{label}")
                else:
                    st.text_input(label, placeholder=placeholder,
                                  key=f"db_{label}")

    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    if st.button(f"🔌  Connect to {db_name.split()[1]}",
                 type="primary", use_container_width=False,
                 key="db_connect_btn"):
        with st.spinner(f"Connecting to {db_name.split()[1]}..."):
            time.sleep(2.2)
        st.markdown(f"""
        <div style="background:linear-gradient(135deg,#161B22,#0D1117);
                    border:1px solid {color};border-radius:10px;
                    padding:20px 24px;margin-top:16px">
          <div style="font-size:14px;font-weight:700;color:#E6EDF3;margin-bottom:6px">
            🏢 Enterprise Feature
          </div>
          <div style="font-size:13px;color:#8B949E;line-height:1.7">
            Live database connections are available in the Enterprise plan.<br>
            Supports read-only access, schema discovery, and table sampling.
          </div>
          <div style="margin-top:14px">
            <a href="mailto:hello@dataqualityai.com"
               style="background:{color};color:#fff;font-size:12px;font-weight:700;
                      padding:8px 20px;border-radius:6px;text-decoration:none">
              Request Enterprise Access →
            </a>
          </div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("</div>", unsafe_allow_html=True)


def _render_progress(placeholder, pct: int, current: str, done_steps: list):
    """Render animated progress bar into a st.empty() placeholder."""
    steps_html = ""
    for emoji, label in done_steps:
        steps_html += (
            f'<div style="display:flex;align-items:center;gap:10px;margin-bottom:7px">'
            f'<span style="color:#3FB950;font-family:\'JetBrains Mono\',monospace;'
            f'font-size:12px;width:14px">✓</span>'
            f'<span style="font-size:13px;color:#3FB950">{label}</span>'
            f'</div>'
        )
    if pct < 100:
        steps_html += (
            f'<div style="display:flex;align-items:center;gap:10px;margin-bottom:7px">'
            f'<span style="color:#58A6FF;font-family:\'JetBrains Mono\',monospace;'
            f'font-size:12px;width:14px">›</span>'
            f'<span style="font-size:13px;color:#C9D1D9">{current}</span>'
            f'</div>'
        )

    placeholder.markdown(f"""
    <div style="background:#161B22;border:1px solid #21262D;border-radius:14px;
                padding:32px 36px;margin:16px 0;max-width:640px">
      <div style="font-size:11px;font-weight:700;letter-spacing:2px;
                  text-transform:uppercase;color:#6E7681;margin-bottom:18px">
        🔬 Running Diagnostic
      </div>
      <div style="background:#21262D;border-radius:999px;height:8px;
                  overflow:hidden;margin-bottom:8px">
        <div style="width:{pct}%;
                    background:linear-gradient(90deg,#F85149 0%,#E3B341 48%,#3FB950 100%);
                    height:8px;border-radius:999px"></div>
      </div>
      <div style="display:flex;justify-content:space-between;
                  font-size:11px;color:#484F58;margin-bottom:22px">
        <span style="color:#6E7681">{current if pct < 100 else "✅ Complete"}</span>
        <span style="font-family:'JetBrains Mono',monospace">{pct}%</span>
      </div>
      {steps_html}
    </div>
    """, unsafe_allow_html=True)


def make_flow_map(dfs, joins, orphan_result, gap_result) -> go.Figure:
    names = list(dfs.keys())
    n = len(names)

    POSITIONS = {
        1: [(0, 0)],
        2: [(-2.5, 0), (2.5, 0)],
        3: [(-3, 0), (3, 0), (0, -2.5)],
        4: [(-3, 1), (3, 1), (-3, -1), (3, -1)],
        5: [(-3, 1.5), (3, 1.5), (0, 0), (-3, -1.5), (3, -1.5)],
    }
    pos = {names[i]: POSITIONS[n][i] for i in range(n)}

    # Build issue lookup
    issue_lookup = {}
    for f in orphan_result.get("findings", []):
        key = (f["file_a"] if hasattr(f, "file_a") else None, None)
        for j in joins:
            if j["file_a"] in f["direction"] and j["file_b"] in f["direction"]:
                pair = tuple(sorted([j["file_a"], j["file_b"]]))
                if pair not in issue_lookup or f["pct_of_source"] > issue_lookup[pair]["pct"]:
                    issue_lookup[pair] = {"pct": f["pct_of_source"], "count": f["orphan_count"]}

    for f in gap_result.get("findings", []):
        pair = tuple(sorted([f["stage_from"], f["stage_to"]]))
        if pair not in issue_lookup or f["pct_of_upstream"] > issue_lookup.get(pair, {}).get("pct", 0):
            issue_lookup[pair] = {"pct": f["pct_of_upstream"], "count": f["missing_count"]}

    fig = go.Figure()

    # Draw edges
    drawn_pairs = set()
    for j in joins:
        fa, fb = j["file_a"], j["file_b"]
        pair = tuple(sorted([fa, fb]))
        if pair in drawn_pairs or fa not in pos or fb not in pos:
            continue
        drawn_pairs.add(pair)

        x0, y0 = pos[fa]
        x1, y1 = pos[fb]
        mx, my = (x0+x1)/2, (y0+y1)/2

        issue = issue_lookup.get(pair)
        if issue:
            pct = issue["pct"]
            color = "#F85149" if pct > 20 else "#F0883E" if pct > 5 else "#E3B341"
            lbl = f"✗ {issue['count']:,} orphans ({pct}%)"
            width = 2
        else:
            color = "#3FB950"
            lbl = f"✓ matched"
            width = 2

        fig.add_trace(go.Scatter(
            x=[x0, x1, None], y=[y0, y1, None],
            mode="lines",
            line=dict(color=color, width=width),
            showlegend=False, hoverinfo="skip",
        ))
        fig.add_trace(go.Scatter(
            x=[mx], y=[my], mode="text",
            text=[f"<b>{lbl}</b>"],
            textfont=dict(size=10, color=color, family="JetBrains Mono"),
            showlegend=False, hoverinfo="skip",
        ))

    # Draw nodes
    for name, (x, y) in pos.items():
        df   = dfs[name]
        rows = len(df)
        cols = len(df.columns)
        fig.add_trace(go.Scatter(
            x=[x], y=[y], mode="markers+text",
            marker=dict(size=70, color="#161B22",
                        line=dict(color="#58A6FF", width=2)),
            text=[f"<b>{name}</b><br><span style='font-size:10px'>{rows:,} rows</span>"],
            textposition="middle center",
            textfont=dict(size=12, color="#E6EDF3", family="Inter"),
            showlegend=False,
            hovertemplate=f"<b>{name}</b><br>{rows:,} rows · {cols} cols<extra></extra>",
        ))

    fig.update_layout(
        paper_bgcolor="#0D1117", plot_bgcolor="#0D1117",
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False, range=[-5, 5]),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False, range=[-4, 3]),
        height=280, margin=dict(t=10, b=10, l=10, r=10),
        font={"family": "Inter"},
    )
    return fig


def render_dim_bars(scores, weights):
    html = []
    for key, label, sub in DIMS:
        val = scores.get(key)
        w   = weights.get(key, 0)
        c   = score_color(val)
        lbl, _ = score_label(val)
        pct    = val if val is not None else 0
        vstr   = f"{val:.0f}" if val is not None else "N/A"
        wstr   = f"{int(w*100)}%"
        html.append(f"""
        <div class="dim-row">
          <div class="dim-header">
            <div>
              <span class="dim-name">{label}</span>
              <div class="dim-sub">{sub} · {wstr} weight</div>
            </div>
            <span class="dim-val" style="color:{c}">{vstr} <span style="font-size:11px;font-weight:500">{lbl}</span></span>
          </div>
          <div class="dim-track">
            <div class="dim-fill" style="width:{pct}%;background:{c}"></div>
          </div>
        </div>""")
    st.markdown("".join(html), unsafe_allow_html=True)


def render_finding(title, metric, severity, detail, examples=None):
    s   = SEV.get(severity, SEV["medium"])
    ex  = ""
    if examples:
        chips = "".join(f'<span class="ex-code">{v}</span>' for v in examples[:5])
        ex = f'<div class="finding-examples">{chips}</div>'
    sev_label = severity.upper()
    st.markdown(f"""
    <div class="finding finding-{severity}">
      <span class="sev-badge" style="background:{s['badge_bg']};color:{s['badge_fg']}">{sev_label}</span>
      <div class="finding-title">{title}</div>
      <div class="finding-headline" style="color:{s['text']}">{metric}</div>
      <div class="finding-detail">{detail}</div>
      {ex}
    </div>""", unsafe_allow_html=True)


def render_insight(icon, title, text):
    st.markdown(f"""
    <div class="insight-card">
      <div class="insight-title">{icon}&nbsp;&nbsp;{title}</div>
      <div class="insight-text">{text}</div>
    </div>""", unsafe_allow_html=True)


def render_rec_teaser(rec):
    s = SEV.get(rec["severity"], SEV["medium"])
    st.markdown(f"""
    <div class="rec-teaser">
      <div style="display:flex;gap:14px;align-items:flex-start">
        <span style="font-size:22px;line-height:1.3">{rec['icon']}</span>
        <div style="flex:1">
          <div style="font-size:14px;font-weight:700;color:#E6EDF3">{rec['title']}</div>
          <div style="font-size:13px;color:#6E7681;margin-top:4px;font-family:'JetBrains Mono',monospace">{rec['teaser_metric']}</div>
          <div style="font-size:13px;color:{s['text']};margin-top:6px;font-weight:600">⚠ {rec['teaser_impact']}</div>
          <div style="margin-top:10px;font-size:12px;color:#484F58;font-style:italic">
            🔒 Step-by-step fix · SQL queries · Prevention strategy — unlock below
          </div>
        </div>
      </div>
    </div>""", unsafe_allow_html=True)


def render_rec_full(rec):
    s     = SEV.get(rec["severity"], SEV["medium"])
    steps = "".join(f"<li style='margin-bottom:8px;color:#C9D1D9'>{step}</li>" for step in rec["full_steps"])
    st.markdown(f"""
    <div class="rec-full" style="background:{s['bg']};border-color:{s['border']}">
      <div style="display:flex;align-items:center;gap:12px;margin-bottom:14px">
        <span style="font-size:22px">{rec['icon']}</span>
        <span style="font-size:15px;font-weight:700;color:#E6EDF3">{rec['title']}</span>
        <span class="sev-badge" style="background:{s['badge_bg']};color:{s['badge_fg']};margin-left:auto">{rec['severity'].upper()}</span>
      </div>
      <p style="font-size:13px;color:#8B949E;margin:0 0 14px;line-height:1.6">
        <strong style="color:#C9D1D9">Root cause:</strong> {rec['full_root_cause']}
      </p>
      <p style="font-size:12px;font-weight:700;color:{s['text']};text-transform:uppercase;letter-spacing:1px;margin:0 0 8px">
        Step-by-step fix
      </p>
      <ol style="font-size:13px;margin:0 0 16px;padding-left:18px;line-height:1.8">{steps}</ol>
      <div style="display:flex;flex-wrap:wrap;gap:20px;font-size:12px;color:#6E7681;
                  border-top:1px solid #21262D;padding-top:12px">
        <span>⏱ <strong style="color:#8B949E">Effort:</strong> {rec['effort']}</span>
        <span>🛡 <strong style="color:#8B949E">Prevention:</strong> {rec['prevention']}</span>
      </div>
    </div>""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# Lead storage
# ─────────────────────────────────────────────────────────────────────────────

LEADS_FILE = os.path.join(os.path.dirname(__file__), "leads.csv")

def save_lead(name, company, email, role):
    exists = os.path.isfile(LEADS_FILE)
    with open(LEADS_FILE, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["timestamp","name","company","email","role"])
        if not exists:
            w.writeheader()
        w.writerow({"timestamp": datetime.now().isoformat(timespec="seconds"),
                    "name": name, "company": company, "email": email, "role": role})


# ─────────────────────────────────────────────────────────────────────────────
# Analysis runner
# ─────────────────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
# Data preview helpers
# ─────────────────────────────────────────────────────────────────────────────

def _analyze_preview_issues(preview: pd.DataFrame, name: str,
                             dfs: dict, joins: list) -> tuple:
    """
    Comprehensive per-cell issue detection.
    Returns (color_map, tooltip_map, counts).
    Priority: 1=null > 2=orphan-key > 3=invalid > 4=outlier > 5=dup > 6=orphan-row
    """
    cell_data = {}   # (idx, col) -> [(priority, bg, fg, tip)]

    def add(idx, col, priority, bg, fg, tip):
        key = (idx, col)
        cell_data.setdefault(key, []).append((priority, bg, fg, tip))

    # ── 1. Null / whitespace ─────────────────────────────────────────────────
    null_count = 0
    for idx in preview.index:
        for col in preview.columns:
            val = preview.loc[idx, col]
            if pd.isna(val):
                null_count += 1
                add(idx, col, 1, "#3d0f0f", "#F85149",
                    "🔴 Missing value (null) — excluded from all aggregations, "
                    "averages, counts, and reports. Trace back to the source system "
                    "or ETL pipeline to find where this value is lost.")
            elif isinstance(val, str) and len(val) > 0 and val.strip() == "":
                null_count += 1
                add(idx, col, 1, "#3d0f0f", "#F85149",
                    "🔴 Whitespace-only string — appears non-empty but contains only "
                    "spaces or tabs. Will fail equality checks and cause join mismatches.")

    # ── 2. Orphan records ─────────────────────────────────────────────────────
    orphan_rows = set()
    for j in joins:
        if j["file_a"] == name:
            col_self = j.get("col_a", j["key"])
            other_name, col_other = j["file_b"], j.get("col_b", j["key"])
        elif j["file_b"] == name:
            col_self = j.get("col_b", j["key"])
            other_name, col_other = j["file_a"], j.get("col_a", j["key"])
        else:
            continue
        if col_self not in preview.columns or other_name not in dfs:
            continue
        other_vals = set(dfs[other_name][col_other].dropna().astype(str))
        for idx in preview.index:
            val = preview.loc[idx, col_self]
            if pd.isna(val):
                continue
            if str(val) not in other_vals:
                orphan_rows.add(idx)
                # Key column — high contrast
                add(idx, col_self, 2, "#2d1500", "#F0883E",
                    f"🟠 Orphan key — '{val}' has no matching {col_other} "
                    f"in '{other_name}'. This row is invisible in every JOIN, "
                    f"aggregation, and report built on this relationship.")
                # Rest of the row — lighter tint
                for other_col in preview.columns:
                    if other_col != col_self:
                        add(idx, other_col, 6, "#1a0d00", "#c97a50",
                            f"🟠 Orphan row — key '{col_self}' = '{val}' "
                            f"has no match in '{other_name}'. "
                            f"This entire row is excluded from joined analyses.")

    # ── 3. Duplicate rows ─────────────────────────────────────────────────────
    dup_mask = preview.duplicated(keep=False)
    dup_rows_set = set(preview.index[dup_mask])
    dup_groups: dict = {}
    for idx in dup_rows_set:
        key = tuple(preview.loc[idx].fillna("__∅__"))
        dup_groups.setdefault(key, []).append(idx)
    for group_idxs in dup_groups.values():
        for idx in group_idxs:
            others = [r + 1 for r in group_idxs if r != idx]
            others_str = ", ".join(str(r) for r in others[:4])
            for col in preview.columns:
                add(idx, col, 5, "#0d1a2d", "#58A6FF",
                    f"🔵 Duplicate row — identical record also at row {others_str}. "
                    f"Entity counts, totals, and KPIs are inflated. "
                    f"Add a UNIQUE constraint and deduplicate at ingestion.")

    # ── 4. Per-column validity ────────────────────────────────────────────────
    validity_count = 0
    for col in preview.columns:
        s = preview[col]

        # Negative monetary values
        is_money = bool(re.search(
            r"(amount|price|cost|revenue|salary|fee|total|value)", col, re.IGNORECASE))
        if is_money and pd.api.types.is_numeric_dtype(s):
            neg_idx = s[s < 0].index
            validity_count += len(neg_idx)
            for idx in neg_idx:
                add(idx, col, 3, "#2a1d00", "#E3B341",
                    f"⚠ Negative monetary value ({s[idx]:,.2f}) — monetary columns "
                    f"should be ≥ 0. Could be an uncoded refund, credit note, "
                    f"or a sign-convention mismatch between systems.")

        # Date issues
        if re.search(r"(date|time|created|updated|timestamp)", col, re.IGNORECASE):
            try:
                parsed = pd.to_datetime(s, errors="coerce")
                now = pd.Timestamp.now()
                # Future dates
                for idx in parsed[parsed > now].index:
                    days_ahead = (parsed[idx] - now).days
                    validity_count += 1
                    add(idx, col, 3, "#2a1d00", "#E3B341",
                        f"⚠ Future date ({s[idx]}) — {days_ahead:,} day"
                        f"{'s' if days_ahead != 1 else ''} ahead of today. "
                        f"Verify: intentional scheduled event, or a year/month "
                        f"transposition error?")
                # Unparseable non-null values
                bad_idx = parsed[parsed.isna() & s.notna()].index
                validity_count += len(bad_idx)
                for idx in bad_idx:
                    add(idx, col, 3, "#2a1d00", "#E3B341",
                        f"⚠ Invalid date format '{s[idx]}' — cannot be parsed. "
                        f"Standardize to ISO 8601 (YYYY-MM-DD) for reliable "
                        f"sorting, filtering, and time-series operations.")
            except Exception:
                pass

        # Numeric outliers (3× IQR — extreme values only)
        if pd.api.types.is_numeric_dtype(s):
            non_null = s.dropna()
            if len(non_null) > 10:
                q25, q75 = non_null.quantile(0.25), non_null.quantile(0.75)
                iqr = q75 - q25
                if iqr > 0:
                    lo, hi = q25 - 3 * iqr, q75 + 3 * iqr
                    for idx in preview.index:
                        val = s[idx]
                        if pd.notna(val) and (val < lo or val > hi):
                            direction = "low" if val < lo else "high"
                            validity_count += 1
                            add(idx, col, 4, "#0d1525", "#79C0FF",
                                f"◈ Statistical outlier ({val:,.2f}) — extreme {direction} "
                                f"value. Normal range: {lo:,.1f} → {hi:,.1f} (3× IQR). "
                                f"Verify: unit mismatch, manual entry error, "
                                f"or genuine edge case?")

    # ── Build output maps ─────────────────────────────────────────────────────
    color_map, tooltip_map = {}, {}
    for (idx, col), issues in cell_data.items():
        issues.sort(key=lambda x: x[0])   # highest priority wins for color
        _, bg, fg, _ = issues[0]
        color_map[(idx, col)] = f"background-color:{bg};color:{fg};font-weight:600"
        seen, tips = [], []
        for _, _, _, tip in issues:
            if tip not in seen:
                seen.append(tip)
                tips.append(tip)
        tooltip_map[(idx, col)] = "<br><br>".join(tips[:2])

    counts = {
        "nulls":       null_count,
        "orphan_rows": len(orphan_rows),
        "dup_rows":    len(dup_rows_set),
        "validity":    validity_count,
    }
    return color_map, tooltip_map, counts


def _build_preview_html(df: pd.DataFrame, color_map: dict,
                         tooltip_map: dict) -> str:
    """Render an HTML table with color-coded cells and hover tooltips."""
    cols = list(df.columns)

    # Header
    th = "".join(f"<th>{c}</th>" for c in cols)
    rows_html = [f"<thead><tr><th>#</th>{th}</tr></thead><tbody>"]

    for idx in df.index:
        row_cells = [f'<td style="color:#484F58;font-size:10px;'
                     f'border-right:1px solid #30363D">{idx + 1}</td>']
        for col in cols:
            raw = df.loc[idx, col]
            display = "∅" if pd.isna(raw) else str(raw)[:45] + ("…" if len(str(raw)) > 45 else "")
            style = color_map.get((idx, col), "")
            tip   = tooltip_map.get((idx, col), "")

            if style or tip:
                safe_tip = tip.replace('"', "&quot;").replace("<br><br>", " | ")
                row_cells.append(
                    f'<td class="dq-c" style="{style}">'
                    f'<span class="dq-v">{display}</span>'
                    f'<div class="dq-t">{tip}</div>'
                    f'</td>'
                )
            else:
                row_cells.append(f"<td>{display}</td>")

        rows_html.append(f"<tr>{''.join(row_cells)}</tr>")

    rows_html.append("</tbody>")
    return f"<table class='dq-tbl'>{''.join(rows_html)}</table>"


def render_data_preview(dfs: dict, joins: list):
    """Render annotated data preview — HTML table with per-cell hover tooltips."""
    st.markdown('<hr class="dq-divider">', unsafe_allow_html=True)
    st.markdown("""
    <div class="section-header">Annotated Data Preview</div>
    <div class="section-title">Your Data — Problems Highlighted</div>
    <div class="section-sub">
      Hover over any coloured cell to see the exact data quality issue on that value.
    </div>
    """, unsafe_allow_html=True)

    # Inject table CSS (once)
    st.markdown("""
    <style>
    .dq-wrap{overflow-x:auto;overflow-y:auto;max-height:480px;
             border:1px solid #21262D;border-radius:10px;margin-bottom:8px}
    .dq-tbl{border-collapse:collapse;width:100%;font-size:12px;
            font-family:'JetBrains Mono',monospace;white-space:nowrap}
    .dq-tbl th{position:sticky;top:0;background:#161B22;color:#8B949E;
               font-size:10px;font-weight:700;text-transform:uppercase;
               letter-spacing:.5px;padding:10px 14px;
               border-bottom:1px solid #30363D;text-align:left;z-index:10}
    .dq-tbl td{padding:7px 14px;border-bottom:1px solid #21262D;
               color:#C9D1D9;position:relative;max-width:220px;
               overflow:hidden;text-overflow:ellipsis}
    .dq-tbl tr:last-child td{border-bottom:none}
    .dq-tbl tr:hover td{filter:brightness(1.08)}
    /* Tooltip */
    .dq-c{cursor:help}
    .dq-t{display:none;position:absolute;bottom:calc(100% + 6px);left:0;
          background:#1C2128;border:1px solid #58A6FF;border-radius:8px;
          color:#E6EDF3;font-size:11px;font-family:'Inter',sans-serif;
          font-weight:400;padding:10px 14px;min-width:240px;max-width:320px;
          white-space:normal;line-height:1.6;z-index:9999;
          box-shadow:0 8px 28px rgba(0,0,0,.75);pointer-events:none}
    .dq-c:hover .dq-t{display:block}
    /* Legend chips */
    .dq-leg{display:flex;gap:18px;flex-wrap:wrap;padding:10px 14px;
            background:#161B22;border:1px solid #21262D;border-radius:8px;
            margin-bottom:12px;font-size:12px}
    </style>
    <div class="dq-leg">
      <span style="color:#F85149">🔴 Missing / null</span>
      <span style="color:#F0883E">🟠 Orphan record</span>
      <span style="color:#58A6FF">🔵 Duplicate row</span>
      <span style="color:#E3B341">🟡 Invalid value</span>
      <span style="color:#79C0FF">◈ Outlier</span>
      <span style="color:#6E7681;font-style:italic">Hover any cell for details</span>
    </div>
    """, unsafe_allow_html=True)

    tabs = st.tabs([f"📄 {name}" for name in dfs.keys()])

    for tab, (name, df) in zip(tabs, dfs.items()):
        with tab:
            preview = df.head(100).reset_index(drop=True)
            color_map, tooltip_map, counts = _analyze_preview_issues(
                preview, name, dfs, joins)

            # Metrics row
            m1, m2, m3, m4, m5 = st.columns(5)
            m1.metric("Rows shown",    f"{len(preview):,} / {len(df):,}")
            m2.metric("Missing cells", counts["nulls"],
                      delta=f"-{counts['nulls']}"       if counts["nulls"]       else None, delta_color="inverse")
            m3.metric("Orphan rows",   counts["orphan_rows"],
                      delta=f"-{counts['orphan_rows']}" if counts["orphan_rows"] else None, delta_color="inverse")
            m4.metric("Duplicate rows",counts["dup_rows"],
                      delta=f"-{counts['dup_rows']}"    if counts["dup_rows"]    else None, delta_color="inverse")
            m5.metric("Validity issues",counts["validity"],
                      delta=f"-{counts['validity']}"    if counts["validity"]    else None, delta_color="inverse")

            # HTML table
            table_html = _build_preview_html(preview, color_map, tooltip_map)
            st.markdown(f'<div class="dq-wrap">{table_html}</div>',
                        unsafe_allow_html=True)

            if not color_map:
                st.success("✅ No issues detected in the visible rows of this file.")


# ─────────────────────────────────────────────────────────────────────────────
# Column profiler
# ─────────────────────────────────────────────────────────────────────────────

def _unused_profile_columns(df: pd.DataFrame) -> list:  # kept for reference, not called
    """Generate a rich profile for every column."""
    col_types = classify_columns(df)
    profiles  = []

    for col in df.columns:
        sem   = col_types.get(col, "other")
        s     = df[col]
        total = len(df)
        nulls = int(s.isna().sum())
        null_pct  = round(nulls / total * 100, 1) if total else 0
        fill_pct  = round(100 - null_pct, 1)
        uniq      = int(s.nunique())
        non_null  = s.dropna()

        p = {
            "name": col, "sem": sem, "icon": COL_ICONS.get(sem, "📊"),
            "nulls": nulls, "null_pct": null_pct, "fill_pct": fill_pct,
            "unique": uniq, "total": total,
        }

        if pd.api.types.is_numeric_dtype(s):
            p["dtype"] = "numeric"
            if len(non_null):
                p["min"]  = round(float(non_null.min()),  2)
                p["max"]  = round(float(non_null.max()),  2)
                p["mean"] = round(float(non_null.mean()), 2)
                p["zeros"]    = int((non_null == 0).sum())
                p["negatives"]= int((non_null < 0).sum())
                if len(non_null) > 4:
                    p["median"] = round(float(non_null.median()), 2)
                    p["p25"]    = round(float(non_null.quantile(0.25)), 2)
                    p["p75"]    = round(float(non_null.quantile(0.75)), 2)
                    _q25, _q75  = non_null.quantile(0.25), non_null.quantile(0.75)
                    _iqr = _q75 - _q25
                    if _iqr > 0:
                        p["outliers"] = int(((non_null < _q25 - 1.5*_iqr) |
                                             (non_null > _q75 + 1.5*_iqr)).sum())
        elif re.search(r"(date|time|created|updated|timestamp)", col, re.IGNORECASE):
            p["dtype"] = "datetime"
            try:
                parsed = pd.to_datetime(s, errors="coerce").dropna()
                if len(parsed):
                    p["date_min"]   = str(parsed.min().date())
                    p["date_max"]   = str(parsed.max().date())
                    p["range_days"] = (parsed.max() - parsed.min()).days
                    p["future"]     = int((parsed > pd.Timestamp.now()).sum())
            except Exception:
                pass
        else:
            p["dtype"] = "text"
            if len(non_null):
                vc = non_null.astype(str).value_counts().head(3)
                p["top_values"] = list(vc.index)
                p["avg_len"]    = round(non_null.astype(str).str.len().mean(), 1)

        # Column quality score
        q = 100
        q -= min(40, null_pct * 0.9)
        if uniq == 1 and total > 5:     q -= 25
        if p.get("negatives", 0) > 0:   q -= 20
        if p.get("future", 0) > 0:      q -= 15
        p["quality"] = max(0, round(q))

        profiles.append(p)
    return profiles



# ─────────────────────────────────────────────────────────────────────────────
# Score reveal — the verdict
# ─────────────────────────────────────────────────────────────────────────────

def render_score_reveal(R: dict):
    """Speedometer gauge + grade + dimension bars — first thing after analysis."""
    scores  = R["score_data"]["scores"]
    details = R["score_data"]["details"]
    weights = R["score_data"]["weights"]
    overall = scores["overall"]
    grade, gc = overall_grade(overall)
    lbl, _    = score_label(overall)
    bench     = details.get("benchmark", "")
    urgency   = details.get("urgency", "")

    n_orphans  = sum(f["orphan_count"]    for f in R["orphans"].get("findings", []))
    n_dupes    = sum(f["duplicate_count"] for f in R["dupes"].get("findings", []))
    n_gaps     = sum(f["missing_count"]   for f in R["gaps"].get("findings", []))
    rows_total = sum(len(df) for df in R["dfs"].values())

    # ── Header banner ─────────────────────────────────────────────────────────
    st.markdown(f"""
    <style>
    @keyframes gradeIn {{
      from {{ opacity:0; transform:scale(0.5) rotate(-8deg); }}
      to   {{ opacity:1; transform:scale(1)   rotate(0deg);  }}
    }}
    .grade-anim {{ animation: gradeIn 0.65s cubic-bezier(0.34,1.56,0.64,1) both; }}
    </style>
    <div style="background:linear-gradient(135deg,#010409 0%,#0D1117 55%,#161B22 100%);
                border:1px solid {gc};border-radius:16px 16px 0 0;
                padding:28px 44px 18px;position:relative;overflow:hidden;
                box-shadow:0 0 60px {gc}15">
      <div style="position:absolute;top:0;left:0;right:0;height:4px;
                  background:linear-gradient(90deg,{gc},{gc}55);
                  border-radius:16px 16px 0 0"></div>
      <div style="font-size:11px;font-weight:700;letter-spacing:2px;
                  text-transform:uppercase;color:{gc}">
        🔬 Diagnostic Result
      </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Main layout: gauge (left) | grade + dims (right) ─────────────────────
    gauge_col, verdict_col = st.columns([1, 1.1], gap="large")

    with gauge_col:
        st.markdown(
            f'<div style="background:#0D1117;border-left:1px solid {gc};'
            f'border-right:none;border-bottom:none;padding:8px 0 0 0">',
            unsafe_allow_html=True)
        st.plotly_chart(make_speedometer(overall), use_container_width=True,
                        config={"displayModeBar": False})
        # Zone labels
        st.markdown(f"""
        <div style="display:flex;justify-content:space-between;
                    padding:0 32px;margin-top:-18px;margin-bottom:8px">
          <span style="font-size:10px;color:#F85149;font-weight:600">◀ Critical</span>
          <span style="font-size:10px;color:#E3B341;font-weight:600">Fair</span>
          <span style="font-size:10px;color:#3FB950;font-weight:600">Excellent ▶</span>
        </div>
        """, unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with verdict_col:
        st.markdown(f"""
        <div style="background:#0D1117;border-right:1px solid {gc};
                    padding:20px 32px 20px 16px">
          <div class="grade-anim"
               style="font-size:96px;font-weight:900;line-height:1;
                      color:{gc};font-family:'JetBrains Mono',monospace;
                      text-shadow:0 0 60px {gc}44">
            {grade}
          </div>
          <div style="font-size:17px;font-weight:700;color:{gc};margin-top:4px">{lbl}</div>
          <div style="display:inline-block;background:#21262D;border:1px solid #30363D;
                      border-radius:999px;padding:3px 14px;font-size:11px;color:#8B949E;
                      margin-top:8px">{bench}</div>
          <div style="font-size:13px;color:#C9D1D9;font-weight:600;margin-top:12px;
                      line-height:1.6;max-width:360px">{urgency}</div>

          <div style="font-size:10px;font-weight:700;letter-spacing:1.5px;
                      text-transform:uppercase;color:#484F58;margin:20px 0 12px">
            Score by dimension
          </div>
        """, unsafe_allow_html=True)

        # Dimension bars (inline HTML to stay inside the styled div)
        penalty = details.get("integration_penalty", 0)
        for key, label, _ in DIMS:
            val  = scores.get(key)
            w    = weights.get(key, 0)
            c    = score_color(val)
            ld, _ = score_label(val)
            pct  = val if val is not None else 0
            vs   = f"{val:.0f}" if val is not None else "N/A"
            ws   = f"{int(w*100)}%"
            st.markdown(f"""
            <div style="margin-bottom:11px">
              <div style="display:flex;justify-content:space-between;margin-bottom:4px">
                <div>
                  <span style="font-size:12px;font-weight:600;color:#C9D1D9">{label}</span>
                  <span style="font-size:10px;color:#484F58;margin-left:5px">{ws}</span>
                </div>
                <span style="font-size:12px;font-weight:700;color:{c};
                             font-family:'JetBrains Mono',monospace">
                  {vs} <span style="font-size:10px;font-weight:500">{ld}</span>
                </span>
              </div>
              <div style="background:#21262D;border-radius:999px;height:6px;overflow:hidden">
                <div style="width:{pct}%;background:{c};height:6px;border-radius:999px"></div>
              </div>
            </div>""", unsafe_allow_html=True)

        if penalty > 0:
            st.markdown(
                f'<div style="font-size:11px;color:#484F58;margin-top:4px">'
                f'⚠ −{penalty} integration complexity penalty</div>',
                unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)

    # ── Stats pills row ────────────────────────────────────────────────────────
    orphan_c = "#F85149" if n_orphans > 0 else "#3FB950"
    dup_c    = "#F85149" if n_dupes   > 0 else "#3FB950"
    gap_c    = "#E3B341" if n_gaps    > 0 else "#3FB950"

    def pill(val, label, color):
        return (
            f'<div style="flex:1;min-width:110px;text-align:center;'
            f'background:#161B22;border:1px solid {color}44;'
            f'border-radius:0 0 10px 10px;padding:14px 8px">'
            f'<div style="font-size:26px;font-weight:900;color:{color};'
            f'font-family:\'JetBrains Mono\',monospace">{val}</div>'
            f'<div style="font-size:11px;color:#6E7681;margin-top:3px">{label}</div>'
            f'</div>'
        )

    st.markdown(f"""
    <div style="display:flex;gap:0;border:1px solid {gc};
                border-top:none;border-radius:0 0 16px 16px;overflow:hidden;
                margin-bottom:28px;box-shadow:0 0 60px {gc}15">
      {pill(f"{n_orphans:,}", "orphan records",     orphan_c)}
      {pill(str(n_dupes),     "duplicate entities", dup_c)}
      {pill(f"{n_gaps:,}",   "pipeline gaps",      gap_c)}
      {pill(f"{rows_total:,}", "rows analyzed",     "#58A6FF")}
    </div>
    """, unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# Distribution histograms
# ─────────────────────────────────────────────────────────────────────────────

def render_distributions(dfs: dict):
    """Show Plotly histograms for all numeric columns, grouped by file."""
    has_numeric = any(
        pd.api.types.is_numeric_dtype(df[col])
        for df in dfs.values() for col in df.columns
    )
    if not has_numeric:
        return

    st.markdown('<hr class="dq-divider">', unsafe_allow_html=True)
    st.markdown("""
    <div class="section-header">Distribution Analysis</div>
    <div class="section-title">Column Distributions</div>
    <div class="section-sub">
      Value distribution, outliers, and skewness for every numeric column.
      Green dashed line = mean. Orange bars = outlier-heavy columns. Red bars = negative monetary values.
    </div>
    """, unsafe_allow_html=True)

    tabs = st.tabs([f"📄 {name}" for name in dfs.keys()])

    for tab, (fname, df) in zip(tabs, dfs.items()):
        with tab:
            num_cols = [
                c for c in df.columns
                if pd.api.types.is_numeric_dtype(df[c]) and df[c].dropna().nunique() > 1
            ]
            if not num_cols:
                st.info("No numeric columns with varied data in this file.")
                continue

            display_cols = num_cols[:9]
            if len(num_cols) > 9:
                st.caption(f"Showing first 9 of {len(num_cols)} numeric columns.")

            cols_per_row = min(3, len(display_cols))
            n_rows = math.ceil(len(display_cols) / cols_per_row)

            subplot_titles = [f"<span style='font-size:11px;color:#8B949E'>{c}</span>"
                              for c in display_cols]
            fig = make_subplots(
                rows=n_rows, cols=cols_per_row,
                subplot_titles=subplot_titles,
                vertical_spacing=0.14,
                horizontal_spacing=0.06,
            )

            outlier_summary = []

            for i, col in enumerate(display_cols):
                row_pos = i // cols_per_row + 1
                col_pos = i % cols_per_row + 1
                data = df[col].dropna()

                is_money = bool(re.search(
                    r"(amount|price|cost|revenue|salary|fee|total|value)", col, re.IGNORECASE))
                has_neg = is_money and bool((data < 0).any())

                q25, q75 = data.quantile(0.25), data.quantile(0.75)
                iqr = q75 - q25
                n_outliers = 0
                if iqr > 0:
                    n_outliers = int(((data < q25 - 1.5*iqr) | (data > q75 + 1.5*iqr)).sum())
                if n_outliers > 0:
                    outlier_summary.append((col, n_outliers, round(n_outliers/len(data)*100, 1)))

                bar_color = (
                    "#F85149" if has_neg
                    else "#F0883E" if n_outliers > len(data) * 0.05
                    else "#58A6FF"
                )

                fig.add_trace(go.Histogram(
                    x=data,
                    nbinsx=min(25, data.nunique()),
                    marker=dict(color=bar_color, opacity=0.85,
                                line=dict(color="#0D1117", width=0.5)),
                    name=col, showlegend=False,
                    hovertemplate=f"<b>{col}</b><br>Range: %{{x}}<br>Count: %{{y}}<extra></extra>",
                ), row=row_pos, col=col_pos)

                # Mean reference line
                fig.add_vline(
                    x=float(data.mean()),
                    line=dict(color="#3FB950", width=1.5, dash="dash"),
                    row=row_pos, col=col_pos,
                )

            fig.update_layout(
                paper_bgcolor="#0D1117",
                plot_bgcolor="#0D1117",
                height=220 * n_rows + 50,
                margin=dict(t=55, b=10, l=40, r=10),
                font={"family": "Inter", "color": "#8B949E", "size": 10},
            )
            fig.update_xaxes(showgrid=False, zeroline=False,
                             tickfont=dict(size=9, color="#6E7681"), linecolor="#21262D")
            fig.update_yaxes(showgrid=True, gridcolor="#21262D", zeroline=False,
                             tickfont=dict(size=9, color="#6E7681"))
            fig.update_annotations(font=dict(size=11, color="#8B949E"))

            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
            _dl_png_btn(fig, f"distributions_{fname}.png", "⬇ Download Column Distributions")

            if outlier_summary:
                chips = " ".join(
                    f'<span style="background:#2d1500;border:1px solid #F0883E;color:#F0883E;'
                    f'font-size:11px;padding:2px 10px;border-radius:4px;font-family:monospace">'
                    f'{col}: {n} outliers ({pct}%)</span>'
                    for col, n, pct in outlier_summary[:6]
                )
                st.markdown(
                    f'<div style="margin-top:4px;line-height:2.4">'
                    f'<span style="font-size:11px;color:#6E7681;margin-right:8px">◈ Outliers:</span>'
                    f'{chips}</div>',
                    unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# Quality heatmap (per-file × per-dimension)
# ─────────────────────────────────────────────────────────────────────────────

def _per_file_scores(dfs: dict, score_data: dict) -> dict:
    """Compute approximate per-file scores for Completeness, Uniqueness, Validity, Timeliness."""
    details = score_data["details"]
    result = {}
    for fname, df in dfs.items():
        row = {}

        # Completeness — exact (from scoring module)
        row["Completeness"] = details["completeness_per_file"].get(fname, 100)

        # Uniqueness — row-level dedup
        total = len(df)
        row["Uniqueness"] = round(len(df.drop_duplicates()) / total * 100, 1) if total else 100

        # Validity — column-by-column approximation
        col_scores = []
        for col in df.columns:
            non_null = df[col].dropna()
            if len(non_null) == 0:
                col_scores.append(0); continue
            if pd.api.types.is_numeric_dtype(df[col]):
                is_money = bool(re.search(
                    r"(amount|price|cost|revenue|salary|fee|total|value)", col, re.IGNORECASE))
                if is_money:
                    neg = (df[col] < 0).sum()
                    col_scores.append(max(0, 100 - neg / len(df) * 200))
                elif len(non_null) > 10:
                    q25, q75 = non_null.quantile(0.25), non_null.quantile(0.75)
                    iqr = q75 - q25
                    if iqr > 0:
                        outliers = ((non_null < q25 - 3*iqr) | (non_null > q75 + 3*iqr)).sum()
                        col_scores.append(max(0, 100 - outliers / len(non_null) * 120))
                    else:
                        col_scores.append(85)
                else:
                    col_scores.append(88)
            elif re.search(r"(date|time|created|updated|timestamp)", col, re.IGNORECASE):
                try:
                    parsed = pd.to_datetime(df[col], errors="coerce")
                    fail_rate = parsed.isna().sum() / len(df)
                    future = (parsed > pd.Timestamp.now()).sum()
                    col_scores.append(max(0, round(100 - fail_rate*60 - (future/len(df))*30, 1)))
                except Exception:
                    col_scores.append(70)
            else:
                col_scores.append(90)
        row["Validity"] = round(sum(col_scores) / len(col_scores), 1) if col_scores else 85

        # Timeliness — find latest date
        date_vals = []
        for col in df.columns:
            if re.search(r"(date|time|created|updated)", col, re.IGNORECASE):
                try:
                    parsed = pd.to_datetime(df[col], errors="coerce").dropna()
                    if len(parsed):
                        date_vals.append(parsed)
                except Exception:
                    pass
        if date_vals:
            latest = pd.concat(date_vals).max()
            days_old = (pd.Timestamp.now() - latest).days
            if   days_old <   7: row["Timeliness"] = 95
            elif days_old <  30: row["Timeliness"] = 82
            elif days_old <  90: row["Timeliness"] = 65
            elif days_old < 365: row["Timeliness"] = 45
            else:                row["Timeliness"] = 25
        else:
            row["Timeliness"] = 40

        result[fname] = row
    return result


def render_quality_heatmap(dfs: dict, score_data: dict):
    """Render a per-file × per-dimension quality score heatmap."""
    if len(dfs) < 2:
        return

    per_file = _per_file_scores(dfs, score_data)
    dims  = ["Completeness", "Uniqueness", "Validity", "Timeliness"]
    files = list(per_file.keys())

    z    = [[per_file[f].get(d, 0)          for d in dims] for f in files]
    text = [[f"{per_file[f].get(d, 0):.0f}" for d in dims] for f in files]

    # Custom colorscale: red → yellow → green
    colorscale = [
        [0.00, "#3d0f0f"],
        [0.50, "#2a1d00"],
        [0.65, "#1a2a0a"],
        [1.00, "#0a3a15"],
    ]

    fig = go.Figure(go.Heatmap(
        z=z, x=dims, y=files,
        colorscale=colorscale,
        zmin=0, zmax=100,
        text=text,
        texttemplate="<b>%{text}</b>",
        textfont={"size": 18, "family": "JetBrains Mono", "color": "#E6EDF3"},
        showscale=True,
        colorbar=dict(
            thickness=12, len=0.8,
            tickfont=dict(color="#6E7681", size=10),
            bgcolor="#161B22", bordercolor="#21262D", borderwidth=1,
            title=dict(text="Score", font=dict(color="#6E7681", size=10), side="right"),
        ),
        hovertemplate="<b>%{y}</b><br>%{x}: <b>%{z:.0f}</b><extra></extra>",
    ))

    fig.update_layout(
        paper_bgcolor="#0D1117",
        plot_bgcolor="#0D1117",
        height=max(200, 90 * len(files) + 70),
        margin=dict(t=30, b=10, l=20, r=60),
        font={"family": "Inter", "color": "#8B949E"},
        xaxis=dict(side="top", tickfont=dict(size=13, color="#C9D1D9"), tickangle=0),
        yaxis=dict(tickfont=dict(size=12, color="#C9D1D9"), autorange="reversed"),
    )

    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
    _dl_png_btn(fig, "quality_score_heatmap.png", "⬇ Download Quality Score Heatmap")


# ─────────────────────────────────────────────────────────────────────────────
# Executive summary
# ─────────────────────────────────────────────────────────────────────────────

def render_executive_summary(R: dict):
    scores   = R["score_data"]["scores"]
    details  = R["score_data"]["details"]
    overall  = scores["overall"]
    domain   = R["domain"]
    lbl, _   = score_label(overall)
    grade, _ = overall_grade(overall)
    bench    = details.get("benchmark", "")

    # Count total issues
    n_orphans = sum(f["orphan_count"] for f in R["orphans"].get("findings", []))
    n_dupes   = sum(f["duplicate_count"] for f in R["dupes"].get("findings", []))
    n_gaps    = sum(f["missing_count"] for f in R["gaps"].get("findings", []))
    total_issues = n_orphans + n_dupes + n_gaps
    rows_total   = sum(len(df) for df in R["dfs"].values())

    if total_issues == 0:
        summary = (f"We analyzed your {domain} dataset ({len(R['dfs'])} files, {rows_total:,} rows) "
                   f"and found no critical integration issues. Your data quality score is <strong>{overall}/100</strong> — {lbl}.")
        color = "#3FB950"
    else:
        parts = []
        if n_orphans: parts.append(f"{n_orphans:,} orphan records with no matching counterpart")
        if n_dupes:   parts.append(f"{n_dupes} duplicate entities")
        if n_gaps:    parts.append(f"{n_gaps:,} records stalled in the pipeline")
        issues_str = " · ".join(parts)
        summary = (
            f"We analyzed your <strong>{domain}</strong> pipeline "
            f"(<strong>{len(R['dfs'])} files · {rows_total:,} rows</strong>) and found "
            f"<strong style='color:#F85149'>{total_issues:,} issues</strong>: {issues_str}. "
            f"Data quality score: <strong style='color:{score_color(overall)}'>{overall}/100 — Grade {grade}</strong>. "
            f"{details.get('urgency','')}"
        )
        color = "#F85149"

    st.markdown(f"""
    <div style="background:linear-gradient(135deg,#161B22,#0D1117);border:1px solid {color};
                border-radius:12px;padding:20px 24px;margin-bottom:24px;
                border-left:4px solid {color}">
      <div style="font-size:11px;font-weight:700;letter-spacing:1.5px;text-transform:uppercase;
                  color:{color};margin-bottom:8px">Executive Summary</div>
      <div style="font-size:14px;color:#C9D1D9;line-height:1.7">{summary}</div>
      <div style="font-size:12px;color:#484F58;margin-top:8px">{bench}</div>
    </div>
    """, unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# Missing file suggestions
# ─────────────────────────────────────────────────────────────────────────────

DOMAIN_EXPECTED = {
    "E-commerce":  ["orders", "customers", "products", "invoices", "payments", "shipments"],
    "CRM":         ["leads", "contacts", "opportunities", "accounts", "activities"],
    "Finance":     ["transactions", "invoices", "payments", "accounts", "ledger"],
    "HR":          ["employees", "departments", "payroll", "reviews"],
    "Marketing":   ["campaigns", "leads", "conversions", "contacts"],
    "Operations":  ["requests", "inventory", "shipments", "suppliers"],
}

def render_missing_file_suggestions(dfs: dict, domain: str):
    expected = DOMAIN_EXPECTED.get(domain, [])
    if not expected:
        return
    present = " ".join(dfs.keys()).lower()
    missing = [e for e in expected if e.rstrip("s") not in present and e not in present]
    if not missing:
        return
    chips = "".join(
        f'<span style="background:#161B22;border:1px dashed #30363D;border-radius:6px;'
        f'padding:4px 12px;font-size:12px;color:#6E7681;margin:4px 4px 0 0;display:inline-block">'
        f'+ {e}.csv</span>'
        for e in missing[:4]
    )
    st.markdown(f"""
    <div style="background:#0D1117;border:1px dashed #30363D;border-radius:10px;
                padding:16px 20px;margin-bottom:16px">
      <div style="font-size:12px;font-weight:700;color:#6E7681;margin-bottom:8px">
        💡 Add these files for a more complete {domain} analysis:
      </div>
      <div>{chips}</div>
    </div>
    """, unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# HTML report export
# ─────────────────────────────────────────────────────────────────────────────

def generate_html_report(R: dict) -> str:
    scores  = R["score_data"]["scores"]
    details = R["score_data"]["details"]
    overall = scores["overall"]
    grade, gc = overall_grade(overall)
    lbl, lc   = score_label(overall)
    now_str   = datetime.now().strftime("%Y-%m-%d %H:%M")

    rows_total = sum(len(df) for df in R["dfs"].values())
    bench = details.get("benchmark", "")

    # Findings HTML
    findings_html = ""
    for f in R["orphans"].get("findings", [])[:2]:
        pct = f["pct_of_source"]
        findings_html += f"""
        <div style="border-left:4px solid #F85149;padding:12px 16px;margin-bottom:12px;background:#1a0808;border-radius:4px">
          <div style="font-weight:700;color:#F85149">ORPHAN RECORDS — {f['direction']}</div>
          <div style="font-size:22px;font-weight:900;color:#F85149;margin:4px 0">{f['orphan_count']:,} records ({pct}%) unmatched</div>
          <div style="color:#999">Key: {f['key']} · Examples: {', '.join(str(v) for v in f['example_values'][:3])}</div>
        </div>"""
    for f in R["dupes"].get("findings", [])[:1]:
        findings_html += f"""
        <div style="border-left:4px solid #F0883E;padding:12px 16px;margin-bottom:12px;background:#1a0d00;border-radius:4px">
          <div style="font-weight:700;color:#F0883E">ENTITY DUPLICATES — {f['file']}</div>
          <div style="font-size:22px;font-weight:900;color:#F0883E;margin:4px 0">{f['duplicate_count']} duplicate entities</div>
          <div style="color:#999">Type: {f['type']}</div>
        </div>"""
    for f in R["gaps"].get("findings", [])[:1]:
        pct = f["pct_of_upstream"]
        findings_html += f"""
        <div style="border-left:4px solid #E3B341;padding:12px 16px;margin-bottom:12px;background:#1a1500;border-radius:4px">
          <div style="font-weight:700;color:#E3B341">PROCESS GAP — {f['stage_from']} → {f['stage_to']}</div>
          <div style="font-size:22px;font-weight:900;color:#E3B341;margin:4px 0">{f['missing_count']:,} records ({pct}%) stalled</div>
        </div>"""

    # Dimension rows
    dim_rows = ""
    for key, label, _ in DIMS:
        val = scores.get(key)
        c   = score_color(val)
        lbl_d, _ = score_label(val)
        w   = int(R["score_data"]["weights"].get(key, 0) * 100)
        v   = f"{val:.0f}" if val is not None else "N/A"
        dim_rows += f"""
        <tr>
          <td style="padding:8px 12px;color:#C9D1D9">{label}</td>
          <td style="padding:8px 12px;color:#6E7681">{w}%</td>
          <td style="padding:8px 12px;font-weight:700;color:{c};font-family:monospace">{v}</td>
          <td style="padding:8px 12px;color:{c}">{lbl_d}</td>
        </tr>"""

    # Recommendations HTML
    recs_html = ""
    for rec in R["recs"]:
        s = SEV.get(rec["severity"], SEV["medium"])
        steps = "".join(f"<li style='margin-bottom:6px;color:#C9D1D9'>{st_}</li>" for st_ in rec["full_steps"])
        recs_html += f"""
        <div style="background:{s['bg']};border:1px solid {s['border']};border-radius:8px;padding:16px;margin-bottom:12px">
          <div style="font-size:15px;font-weight:700;color:#E6EDF3;margin-bottom:8px">{rec['icon']} {rec['title']}</div>
          <div style="font-size:13px;color:#8B949E;margin-bottom:10px"><strong style="color:#C9D1D9">Root cause:</strong> {rec['full_root_cause']}</div>
          <ol style="font-size:13px;padding-left:16px;margin:0 0 10px">{steps}</ol>
          <div style="font-size:12px;color:#6E7681">⏱ {rec['effort']} &nbsp;|&nbsp; 🛡 {rec['prevention']}</div>
        </div>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Data Quality Report — {now_str}</title>
<style>
  body {{ font-family: Inter, system-ui, sans-serif; background: #0D1117; color: #E6EDF3; max-width: 860px; margin: 0 auto; padding: 40px 24px; }}
  h1 {{ font-size: 32px; font-weight: 900; margin-bottom: 4px; }}
  h2 {{ font-size: 18px; font-weight: 700; color: #8B949E; text-transform: uppercase; letter-spacing: 1px; margin: 32px 0 12px; border-bottom: 1px solid #21262D; padding-bottom: 6px; }}
  table {{ width: 100%; border-collapse: collapse; background: #161B22; border-radius: 8px; overflow: hidden; }}
  th {{ background: #21262D; padding: 10px 12px; text-align: left; font-size: 12px; color: #6E7681; text-transform: uppercase; letter-spacing: 0.5px; }}
  tr:nth-child(even) {{ background: #0D1117; }}
  .score-big {{ font-size: 72px; font-weight: 900; font-family: monospace; color: {gc}; }}
  .badge {{ display: inline-block; background: #21262D; border-radius: 999px; padding: 4px 14px; font-size: 12px; color: #8B949E; }}
  .footer {{ margin-top: 48px; font-size: 12px; color: #484F58; border-top: 1px solid #21262D; padding-top: 16px; }}
</style>
</head>
<body>
<div style="border-top:3px solid {gc};border-radius:4px;padding-top:24px;margin-bottom:32px">
  <div style="font-size:11px;font-weight:700;letter-spacing:2px;text-transform:uppercase;color:#6E7681;margin-bottom:8px">🔬 Data Quality Intelligence Report</div>
  <h1>Data Quality Score: <span style="color:{gc}">{overall}/100</span></h1>
  <div style="color:{gc};font-size:18px;font-weight:700">{lbl} · Grade {grade}</div>
  <div class="badge" style="margin-top:8px">{bench}</div>
  <div style="font-size:13px;color:#6E7681;margin-top:12px">
    Generated: {now_str} · Domain: {R['domain']} · {len(R['dfs'])} files · {rows_total:,} rows
  </div>
</div>

<h2>Dimension Scores</h2>
<table>
  <tr><th>Dimension</th><th>Weight</th><th>Score</th><th>Status</th></tr>
  {dim_rows}
</table>

<h2>Critical Findings</h2>
{findings_html if findings_html else '<p style="color:#3FB950">✅ No critical issues found.</p>'}

<h2>Full Remediation Plan</h2>
{recs_html if recs_html else '<p style="color:#3FB950">✅ No recommendations — data looks solid.</p>'}

<div class="footer">
  Generated by DataQuality.ai · dataqualityanalyzer.streamlit.app
</div>
</body>
</html>"""


def render_assessment_form(preview_dfs: dict) -> dict:
    """
    Show a short pre-analysis assessment form.
    Returns assessment_config dict:
    {
        "domain":       str or None,          # None = use auto-detect
        "primary_keys": {fname: col},         # per-file PK column (normalized)
        "monetary":     (fname, col) or None, # for business impact
        "user_joins":   [{key, file_a, file_b, col_a?, col_b?}],
    }
    """
    # Auto-populate defaults using existing detection
    auto_domain, _ = detect_domain(preview_dfs)
    auto_joins = detect_join_keys(preview_dfs)

    # Find monetary columns via classify_columns
    monetary_defaults = {}
    pk_defaults = {}
    for fname, df in preview_dfs.items():
        sem = classify_columns(df)
        monetary_defaults[fname] = [c for c, t in sem.items() if t == "monetary"]
        # Default PK: first col matching id pattern
        pk_defaults[fname] = next(
            (c for c in df.columns if re.search(r"(^id$|_id$|_key$)", c, re.IGNORECASE)),
            None
        )

    all_domains = ["E-commerce", "CRM", "Finance", "HR", "Marketing", "Operations", "General Business"]
    ordered_domains = [auto_domain] + [d for d in all_domains if d != auto_domain]

    cfg = {
        "domain": None,
        "primary_keys": {},
        "monetary": None,
        "user_joins": [],
    }

    with st.expander("📋 Pre-Analysis Assessment", expanded=True):
        st.markdown(
            "<div style='font-size:13px;color:#8B949E;margin-bottom:16px'>"
            "Confirm a few details so the analysis targets your specific data. "
            "Defaults are auto-detected — most users just click <strong>Confirm</strong>."
            "</div>",
            unsafe_allow_html=True,
        )

        # ── Section 1: Business Domain ────────────────────────────────────────
        st.markdown("**Business Domain**")
        domain_sel = st.selectbox(
            "What type of business data is this?",
            options=ordered_domains,
            index=0,
            help="Auto-detected from file names and column names.",
        )
        st.caption(f"Auto-detected: **{auto_domain}**")
        cfg["domain"] = domain_sel if domain_sel != auto_domain else None

        st.markdown("---")

        # ── Section 2: Primary Key per file ──────────────────────────────────
        st.markdown("**Primary Key per File**")
        st.caption("The column that uniquely identifies each row.")
        for fname, df in preview_dfs.items():
            col_options = ["Auto-detect"] + list(df.columns)
            default_pk = pk_defaults.get(fname)
            default_idx = col_options.index(default_pk) if default_pk and default_pk in col_options else 0
            chosen = st.selectbox(
                f"Primary key — `{fname}`",
                options=col_options,
                index=default_idx,
                key=f"pk_{fname}",
            )
            cfg["primary_keys"][fname] = None if chosen == "Auto-detect" else chosen

        st.markdown("---")

        # ── Section 3: Monetary / Value Column ────────────────────────────────
        st.markdown("**Monetary / Value Column**")
        st.caption("Used to calculate dollar-value business impact.")
        skip_monetary = st.checkbox("Skip — no monetary column in this data", value=False, key="skip_monetary")

        if not skip_monetary:
            file_names = list(preview_dfs.keys())
            # Default: file with most monetary-tagged columns
            best_file = max(file_names, key=lambda f: len(monetary_defaults.get(f, [])), default=file_names[0])
            file_idx = file_names.index(best_file)
            mon_file = st.selectbox("File containing monetary values", options=file_names, index=file_idx, key="monetary_file")

            mon_col_options = ["None"] + list(preview_dfs[mon_file].columns)
            mon_defaults_for_file = monetary_defaults.get(mon_file, [])
            default_col = mon_defaults_for_file[0] if mon_defaults_for_file else None
            default_col_idx = mon_col_options.index(default_col) if default_col and default_col in mon_col_options else 0
            mon_col = st.selectbox("Monetary / value column", options=mon_col_options, index=default_col_idx, key="monetary_col")

            if mon_col and mon_col != "None":
                cfg["monetary"] = (mon_file, mon_col)

        st.markdown("---")

        # ── Section 4: File Relationships (multi-file only) ───────────────────
        if len(preview_dfs) > 1:
            st.markdown("**File Relationships**")
            st.caption("Auto-detected joins shown below. Add custom relationships if needed.")

            # Show auto-detected joins as chips
            if auto_joins:
                chips = "  ".join(
                    f"`{j['file_a']}.{j.get('col_a', j['key'])} ↔ {j['file_b']}.{j.get('col_b', j['key'])}`"
                    for j in auto_joins
                )
                st.markdown(f"Auto-detected: {chips}")
            else:
                st.caption("No joins auto-detected.")

            # User-defined extra joins
            if "user_joins_list" not in st.session_state:
                st.session_state.user_joins_list = []

            if st.button("+ Add relationship", key="add_join_btn"):
                st.session_state.user_joins_list.append({"file_a": "", "col_a": "", "file_b": "", "col_b": ""})

            fname_list = list(preview_dfs.keys())
            for idx, uj in enumerate(st.session_state.user_joins_list):
                cols = st.columns([2, 2, 2, 2, 1])
                fa = cols[0].selectbox("File A", fname_list, key=f"uj_fa_{idx}")
                ca_opts = list(preview_dfs[fa].columns) if fa else []
                ca = cols[1].selectbox("Col A", ca_opts, key=f"uj_ca_{idx}")
                fb = cols[2].selectbox("File B", fname_list, key=f"uj_fb_{idx}")
                cb_opts = list(preview_dfs[fb].columns) if fb else []
                cb = cols[3].selectbox("Col B", cb_opts, key=f"uj_cb_{idx}")
                if cols[4].button("✕", key=f"uj_rm_{idx}"):
                    st.session_state.user_joins_list.pop(idx)
                    st.rerun()
                if fa and ca and fb and cb:
                    cfg["user_joins"].append({"key": ca, "file_a": fa, "col_a": ca, "file_b": fb, "col_b": cb})

    return cfg


def run_analysis(uploaded_files, cfg=None) -> tuple:
    dfs, errors = {}, []
    for f in uploaded_files:
        name = re.sub(r"\.csv$", "", f.name, flags=re.IGNORECASE)
        try:
            df = pd.read_csv(f)
            df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
            dfs[name] = df
        except Exception as e:
            errors.append(f"Could not read **{f.name}**: {e}")

    if not dfs:
        return None, errors

    joins = detect_join_keys(dfs)

    # Merge user-specified relationships from assessment form
    if cfg and cfg.get("user_joins"):
        existing = {(j["file_a"], j["file_b"]) for j in joins}
        for uj in cfg["user_joins"]:
            if (uj["file_a"], uj["file_b"]) not in existing:
                joins.append(uj)

    orphans    = check_orphan_records(dfs, joins)
    dupes      = check_entity_duplicates(dfs, joins)
    gaps       = check_process_gaps(dfs, joins)
    score_data = calculate_scores(dfs, orphans, dupes, gaps, len(dfs))
    recs       = generate_recommendations(orphans, dupes, gaps, score_data)

    # Semantic layer
    entities = {name: detect_entity(name, df) for name, df in dfs.items()}
    domain, conf = detect_domain(dfs)

    # Override domain if user specified
    if cfg and cfg.get("domain"):
        domain, conf = cfg["domain"], 1.0

    # Pass monetary override to impact calculation
    monetary_override = cfg.get("monetary") if cfg else None
    impact    = estimate_monetary_impact(dfs, orphans, dupes, gaps,
                                         monetary_override=monetary_override)
    narrative = generate_narrative(dfs, domain, entities, orphans, dupes, gaps,
                                   score_data["scores"]["overall"])

    return {
        "dfs": dfs, "joins": joins,
        "orphans": orphans, "dupes": dupes, "gaps": gaps,
        "score_data": score_data, "recs": recs,
        "entities": entities, "domain": domain, "domain_conf": conf,
        "impact": impact, "narrative": narrative,
    }, errors


# ─────────────────────────────────────────────────────────────────────────────
# Simple-mode helpers
# ─────────────────────────────────────────────────────────────────────────────

def render_simple_findings(R: dict):
    """Translate top findings into 3 plain-English cards for non-technical users."""
    findings = []
    for f in R["orphans"].get("findings", [])[:1]:
        left = f["direction"].split("→")[0].strip()
        findings.append(
            f"<strong>{f['orphan_count']:,} rows</strong> in <em>{left}</em> "
            f"don't match anything in the other file — they'll be invisible in any report."
        )
    for f in R["dupes"].get("findings", [])[:1]:
        findings.append(
            f"<em>{f['file']}</em> contains <strong>{f['duplicate_count']:,} duplicate entries</strong> "
            f"for the same {f['type']}. Every count or total built on this is wrong."
        )
    for f in R["gaps"].get("findings", [])[:1]:
        findings.append(
            f"<strong>{f['pct_of_upstream']}%</strong> of records start at "
            f"<em>{f['stage_from']}</em> but never reach <em>{f['stage_to']}</em>."
        )
    if not findings:
        st.success("No major issues found — your data looks solid.")
        return
    st.markdown(
        "<div style='font-size:18px;font-weight:800;color:#E6EDF3;margin:24px 0 12px'>"
        "What's wrong with your data</div>",
        unsafe_allow_html=True,
    )
    for txt in findings:
        st.markdown(
            f"<div style='background:#161B22;border:1px solid #30363D;"
            f"border-left:4px solid #F0883E;border-radius:8px;padding:16px 20px;"
            f"margin-bottom:10px;font-size:14px;color:#C9D1D9'>⚠️ &nbsp;{txt}</div>",
            unsafe_allow_html=True,
        )


def _render_simple_impact(R: dict):
    """Business impact teaser for Simple mode — total only, no itemized breakdown."""
    impact = R["impact"]
    if not impact.get("items"):
        return
    st.markdown('<hr class="dq-divider">', unsafe_allow_html=True)
    if impact.get("has_monetary") and impact.get("total"):
        total = impact["total"]
        st.markdown(f"""
        <div class="impact-box">
          <div style="font-size:12px;font-weight:700;text-transform:uppercase;
                      letter-spacing:1px;color:#6E7681;margin-bottom:6px">
            Estimated financial impact
          </div>
          <div class="impact-total">~${total:,.0f}</div>
          <div style="font-size:13px;color:#8B949E;margin-top:8px">
            estimated revenue / pipeline at risk from data quality issues
          </div>
        </div>""", unsafe_allow_html=True)
    else:
        count = sum(i["count"] for i in impact["items"])
        st.markdown(f"""
        <div class="impact-box">
          <div style="font-size:12px;font-weight:700;text-transform:uppercase;
                      letter-spacing:1px;color:#6E7681;margin-bottom:6px">
            Records affected
          </div>
          <div class="impact-total">{count:,}</div>
          <div style="font-size:13px;color:#8B949E;margin-top:8px">
            records involved in data quality issues
          </div>
        </div>""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# Main app
# ─────────────────────────────────────────────────────────────────────────────

def main():

    # ── MODE TOGGLE ───────────────────────────────────────────────────────────
    if "mode" not in st.session_state:
        st.session_state["mode"] = "simple"

    mode_col, _ = st.columns([3, 5])
    with mode_col:
        chosen = st.radio(
            "View mode",
            options=["🟢  Simple", "⚙️  Advanced"],
            index=0 if st.session_state["mode"] == "simple" else 1,
            horizontal=True,
            label_visibility="collapsed",
        )
        st.session_state["mode"] = "simple" if "Simple" in chosen else "advanced"

    simple = st.session_state["mode"] == "simple"

    # ── HERO ─────────────────────────────────────────────────────────────────
    if simple:
        st.markdown("""
    <div class="hero">
      <div class="hero-eyebrow">✅ Data Quality Check</div>
      <h1>Find out what's wrong<br><span>with your data.</span></h1>
      <p class="hero-sub">
        Upload a CSV file and get a plain-English report in 30 seconds.
        No technical knowledge needed — we'll highlight the problems and tell you how to fix them.
      </p>
    </div>
    """, unsafe_allow_html=True)
    else:
        st.markdown("""
    <div class="hero">
      <div class="hero-eyebrow">🔬 Data Quality Intelligence Platform</div>
      <h1>Your data looks clean.<br><span>It isn't.</span></h1>
      <p class="hero-sub">
        Upload 1–5 related CSV files and get an industry-standard quality diagnosis —
        cross-file integrity checks, semantic understanding, and a business impact report.
        In 60 seconds.
      </p>
      <div class="stat-row">
        <div class="stat-item">
          <div class="stat-num">$12.9M</div>
          <div class="stat-lbl">average annual cost of bad data per org</div>
        </div>
        <div class="stat-item">
          <div class="stat-num">3.5 hrs</div>
          <div class="stat-lbl">wasted daily by data teams on data prep</div>
        </div>
        <div class="stat-item">
          <div class="stat-num">1 in 3</div>
          <div class="stat-lbl">business decisions based on flawed data</div>
        </div>
        <div class="stat-item">
          <div class="stat-num">22%</div>
          <div class="stat-lbl">average error rate in enterprise data</div>
        </div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    # ── DATA SOURCE ───────────────────────────────────────────────────────────
    st.markdown("""
    <div class="section-header" style="margin-bottom:6px">Data Source</div>
    """, unsafe_allow_html=True)

    tab_files, tab_db = st.tabs(["📁  Upload CSV Files", "🔌  Connect to Database"])

    uploaded_files = []

    with tab_files:
        up_col, info_col = st.columns([3, 1], gap="medium")
        with up_col:
            uploaded_files = st.file_uploader(
                "Drop your CSV files here — up to 5 related files from the same business process",
                type=["csv"], accept_multiple_files=True,
                help="Related files: e.g. orders + customers + invoices from the same pipeline.",
            ) or []
        with info_col:
            st.markdown("""
            <div class="card">
              <div class="card-title">What we check</div>
              <div style="font-size:12px;color:#8B949E;line-height:2">
                ✅ &nbsp;Completeness<br>
                ✅ &nbsp;Uniqueness & entity duplicates<br>
                ✅ &nbsp;Format & range validity<br>
                ✅ &nbsp;Cross-file referential integrity<br>
                ✅ &nbsp;Process flow gaps<br>
                ✅ &nbsp;Semantic understanding<br>
                ✅ &nbsp;Business impact estimation
              </div>
            </div>
            """, unsafe_allow_html=True)

    with tab_db:
        render_db_connector()

    if not uploaded_files:
        st.markdown("""
        <div style="text-align:center;padding:56px 0;color:#484F58">
          <div style="font-size:40px">☝</div>
          <div style="font-size:14px;margin-top:10px">Upload CSV files to get started</div>
        </div>
        """, unsafe_allow_html=True)
        return

    if len(uploaded_files) > 5:
        st.warning("Please upload up to 5 files at a time.")
        return

    # ── PARSE HEADERS ONLY (for assessment form) ─────────────────────────────
    preview_dfs = {}
    for f in uploaded_files:
        try:
            fname = re.sub(r"\.csv$", "", f.name, flags=re.IGNORECASE)
            preview_dfs[fname] = pd.read_csv(f, nrows=0)
            f.seek(0)
        except Exception:
            pass

    # ── ASSESSMENT FORM (Advanced mode only) ─────────────────────────────────
    if not simple:
        assessment_cfg = render_assessment_form(preview_dfs)
    else:
        assessment_cfg = {"domain": None, "primary_keys": {}, "monetary": None, "user_joins": []}

    # ── ANALYZE BUTTON ────────────────────────────────────────────────────────
    btn_col, _ = st.columns([2, 5])
    with btn_col:
        btn_label = "🔍  Check My Data" if simple else "🔬  Run Diagnostic"
        do_analyze = st.button(btn_label, type="primary", use_container_width=True)

    if do_analyze:
        for k in ["results","email_submitted","show_form","user_info"]:
            st.session_state.pop(k, None)

        analysis_steps = [
            ("📂", "Loading and normalizing files"),
            ("🧠", "Detecting entities and business domain"),
            ("🗺️", "Mapping cross-file relationships"),
            ("⚡", "Running 23 quality checks"),
            ("💥", "Calculating business impact"),
        ]
        prog = st.empty()
        done = []
        for i, (emoji, label) in enumerate(analysis_steps):
            pct = int((i / len(analysis_steps)) * 88) + 5
            _render_progress(prog, pct, f"{emoji} {label}...", done)
            time.sleep(0.6)
            done.append((emoji, label))

        results, errors = run_analysis(uploaded_files, cfg=assessment_cfg)
        _render_progress(prog, 100, "✅ Diagnostic complete", done)
        time.sleep(0.4)
        prog.empty()

        for e in errors:
            st.error(e)
        if results:
            st.session_state.results = results
            st.session_state["assessment"] = assessment_cfg
        elif not errors:
            st.error("Analysis failed — please check your files.")

    if "results" not in st.session_state:
        return

    R    = st.session_state.results
    recs = R["recs"]

    # ── SCORE REVEAL — always shown ───────────────────────────────────────────
    render_score_reveal(R)

    if simple:
        # ── SIMPLE MODE RESULTS ────────────────────────────────────────────────
        render_data_preview(R["dfs"], R["joins"])
        render_simple_findings(R)
        _render_simple_impact(R)

    else:
        # ── ADVANCED MODE RESULTS ──────────────────────────────────────────────
        render_executive_summary(R)
        render_missing_file_suggestions(R["dfs"], R["domain"])

        # Export button
        html_report = generate_html_report(R)
        _, dl_col, _ = st.columns([3, 2, 3])
        with dl_col:
            st.download_button(
                label="⬇ Download Full Report (HTML)",
                data=html_report,
                file_name=f"data_quality_report_{datetime.now().strftime('%Y%m%d_%H%M')}.html",
                mime="text/html",
                use_container_width=True,
            )

        render_data_preview(R["dfs"], R["joins"])
        render_distributions(R["dfs"])

        if len(R["dfs"]) > 1:
            st.markdown('<hr class="dq-divider">', unsafe_allow_html=True)
            st.markdown("""
            <div class="section-header">Per-File Breakdown</div>
            <div class="section-title">Quality Score Heatmap</div>
            <div class="section-sub">
              Score by file and dimension. Darker cells = lower score = higher risk.
              Each cell is independently calculated for that specific file.
            </div>
            """, unsafe_allow_html=True)
            render_quality_heatmap(R["dfs"], R["score_data"])

        if len(R["dfs"]) > 1:
            st.markdown('<hr class="dq-divider">', unsafe_allow_html=True)
            st.markdown("""
            <div class="section-header">Relationship Analysis</div>
            <div class="section-title">Your Data Pipeline Map</div>
            <div class="section-sub">
              Green lines = healthy joins · Red/orange lines = broken connections with orphan counts
            </div>
            """, unsafe_allow_html=True)
            st.markdown('<div class="flow-section">', unsafe_allow_html=True)
            flow_fig = make_flow_map(R["dfs"], R["joins"], R["orphans"], R["gaps"])
            st.plotly_chart(flow_fig, use_container_width=True, config={"displayModeBar": False})
            _dl_png_btn(flow_fig, "data_pipeline_map.png", "⬇ Download Pipeline Map")
            st.markdown('</div>', unsafe_allow_html=True)

        if R["narrative"]:
            st.markdown('<hr class="dq-divider">', unsafe_allow_html=True)
            st.markdown("""
            <div class="section-header">Semantic Analysis</div>
            <div class="section-title">What Your Data Is Telling Us</div>
            <div class="section-sub">
              Beyond format checks — a contextual interpretation of what we found.
            </div>
            """, unsafe_allow_html=True)
            for n in R["narrative"]:
                render_insight(n["icon"], n["title"], n["text"])

        impact = R["impact"]
        if impact.get("items"):
            st.markdown('<hr class="dq-divider">', unsafe_allow_html=True)
            st.markdown("""
            <div class="section-header">Business Impact</div>
            <div class="section-title">Estimated Cost of Data Issues</div>
            """, unsafe_allow_html=True)
            if impact.get("has_monetary") and impact.get("total"):
                total = impact["total"]
                rows_html = ""
                for item in impact["items"]:
                    if item.get("value"):
                        rows_html += f"""
                        <div class="impact-row">
                          <span style="color:#8B949E">{item['label']}</span>
                          <span style="color:#F85149;font-weight:700;font-family:'JetBrains Mono',monospace">
                            ~${item['value']:,.0f}
                          </span>
                        </div>"""
                st.markdown(f"""
                <div class="impact-box">
                  <div style="font-size:12px;font-weight:700;text-transform:uppercase;letter-spacing:1px;color:#6E7681;margin-bottom:6px">
                    Identified risk
                  </div>
                  <div class="impact-total">~${total:,.0f}</div>
                  <div style="font-size:12px;color:#6E7681;margin-bottom:20px">
                    estimated revenue / pipeline at risk · based on avg transaction value ${impact['avg_value']:,.0f}
                  </div>
                  {rows_html}
                </div>""", unsafe_allow_html=True)
            else:
                items_html = "".join(
                    f'<div class="impact-row"><span style="color:#8B949E">{i["label"]}</span>'
                    f'<span style="color:#F85149;font-weight:700">{i["count"]:,} records</span></div>'
                    for i in impact["items"]
                )
                st.markdown(f"""
                <div class="impact-box">
                  <div style="font-size:12px;font-weight:700;text-transform:uppercase;letter-spacing:1px;color:#6E7681;margin-bottom:16px">
                    Records at risk (no monetary column detected for $ estimate)
                  </div>
                  {items_html}
                </div>""", unsafe_allow_html=True)

        st.markdown('<hr class="dq-divider">', unsafe_allow_html=True)
        st.markdown("""
        <div class="section-header">Critical Findings</div>
        <div class="section-title">What's Broken — and Why It Matters</div>
        <div class="section-sub">
          Issues found by analyzing <em>relationships between files</em>.
          Data that looks clean in isolation often breaks at the joins.
        </div>
        """, unsafe_allow_html=True)

        shown = 0
        for f in R["orphans"].get("findings", [])[:2]:
            pct = f["pct_of_source"]
            sev = "critical" if pct > 25 else ("high" if pct > 8 else "medium")
            render_finding(
                title=f"Orphan records — {f['direction']}",
                metric=f"{f['orphan_count']:,} records ({pct}%) invisible in reports",
                severity=sev,
                detail=f"Key: <code style='color:#79C0FF;font-family:JetBrains Mono'>{f['key']}</code> · "
                       "These records vanish from every JOIN, aggregation, and report built on this relationship.",
                examples=f["example_values"],
            )
            shown += 1

        for f in R["dupes"].get("findings", [])[:1]:
            sev = "critical" if f["duplicate_count"] > 10 else "high"
            names_ex = [e.get("name") or e.get("value_a","") for e in f["examples"][:3]]
            render_finding(
                title=f"Entity duplicates — '{f['file']}' ({f['type']})",
                metric=f"{f['duplicate_count']} duplicate entities",
                severity=sev,
                detail="Same real-world entity under multiple IDs. "
                       "Every count, segment, and KPI built on this table is wrong.",
                examples=names_ex,
            )
            shown += 1

        for f in R["gaps"].get("findings", [])[:1]:
            pct = f["pct_of_upstream"]
            sev = "critical" if pct > 20 else ("high" if pct > 5 else "medium")
            render_finding(
                title=f"Process gap — {f['stage_from']} → {f['stage_to']}",
                metric=f"{f['missing_count']:,} records ({pct}%) stalled in the pipeline",
                severity=sev,
                detail="Records started the process but never completed the next stage. "
                       "SLA violations, broken audit trail, and invisible workflow failures.",
                examples=f["example_ids"],
            )
            shown += 1

        if shown == 0:
            st.success("✅ No critical integration issues detected across the uploaded files.")

    # ── RECOMMENDATIONS (both modes) ──────────────────────────────────────────
    st.markdown('<hr class="dq-divider">', unsafe_allow_html=True)
    if simple:
        st.markdown("""
        <div class="section-header">Next Steps</div>
        <div class="section-title">How to Fix It</div>
        """, unsafe_allow_html=True)
    else:
        st.markdown("""
        <div class="section-header">Remediation Plan</div>
        <div class="section-title">How to Fix It</div>
        """, unsafe_allow_html=True)

    if not recs:
        st.success("No specific recommendations — data quality is solid.")
        return

    # UNLOCKED
    if st.session_state.get("email_submitted"):
        uname = st.session_state.get("user_info", {}).get("name", "")
        st.success(f"✅ Full recommendations unlocked — {uname}, here's your remediation plan.")
        if not simple:
            st.markdown(
                '<p style="font-size:13px;color:#6E7681;margin-bottom:18px">'
                'Prioritized by severity. Includes root cause, SQL fix queries, and prevention strategies.</p>',
                unsafe_allow_html=True)
        for rec in recs:
            render_rec_full(rec)
        return

    # TEASER
    teaser_intro = (
        'Here\'s a preview of what we found. Enter your details below to get the full step-by-step fix guide.'
        if simple else
        'A preview of your top issues. Enter your details below to unlock the full step-by-step guide.'
    )
    st.markdown(
        f'<p style="font-size:14px;color:#8B949E;margin-bottom:18px">{teaser_intro}</p>',
        unsafe_allow_html=True)

    for rec in recs[:3]:
        render_rec_teaser(rec)

    # ── BLURRED DASHBOARD PREVIEW + LOCK OVERLAY ──────────────────────────────
    # Build real rec cards for the blurred preview (locked recs = more convincing)
    locked_recs = recs[3:] if len(recs) > 3 else recs[1:] if len(recs) > 1 else recs
    blurred_cards_html = ""
    for rec in locked_recs[:4]:
        s = SEV.get(rec.get("severity", "medium"), SEV["medium"])
        steps_html = "".join(
            f"<li style='margin-bottom:8px;color:#C9D1D9'>{step}</li>"
            for step in rec.get("full_steps", [])
        )
        blurred_cards_html += f"""
        <div style="background:{s['bg']};border:1px solid {s['border']};
                    border-left:4px solid {s['border']};border-radius:10px;
                    padding:20px;margin-bottom:14px">
          <div style="display:flex;align-items:center;gap:12px;margin-bottom:14px">
            <span style="font-size:22px">{rec['icon']}</span>
            <span style="font-size:15px;font-weight:700;color:#E6EDF3">{rec['title']}</span>
            <span style="background:{s['badge_bg']};color:{s['badge_fg']};font-size:10px;
                         font-weight:700;padding:2px 10px;border-radius:999px;
                         text-transform:uppercase;margin-left:auto">
              {rec.get('severity','medium').upper()}
            </span>
          </div>
          <p style="font-size:13px;color:#8B949E;margin:0 0 14px;line-height:1.6">
            <strong style="color:#C9D1D9">Root cause:</strong> {rec.get('full_root_cause','')}
          </p>
          <p style="font-size:12px;font-weight:700;color:{s['text']};
                    text-transform:uppercase;letter-spacing:1px;margin:0 0 8px">
            Step-by-step fix
          </p>
          <ol style="font-size:13px;margin:0 0 16px;padding-left:18px;line-height:1.8">
            {steps_html}
          </ol>
          <div style="display:flex;flex-wrap:wrap;gap:20px;font-size:12px;color:#6E7681;
                      border-top:1px solid #21262D;padding-top:12px">
            <span>⏱ <strong style="color:#8B949E">Effort:</strong> {rec.get('effort','')}</span>
            <span>🛡 <strong style="color:#8B949E">Prevention:</strong> {rec.get('prevention','')}</span>
          </div>
        </div>"""

    # Fake SQL block adds visual richness
    sql_flair = """
    <div style="background:#0D1117;border:1px solid #30363D;border-radius:10px;
                padding:20px;margin-bottom:14px">
      <div style="font-size:11px;font-weight:700;color:#6E7681;text-transform:uppercase;
                  letter-spacing:1px;margin-bottom:12px">💻 SQL Fix Queries</div>
      <pre style="background:#010409;border:1px solid #21262D;border-radius:6px;
                  padding:16px;font-size:12px;color:#79C0FF;
                  font-family:'JetBrains Mono',monospace;overflow-x:auto;margin:0">UPDATE orders o
LEFT JOIN customers c ON o.customer_id = c.id
SET o.status = 'orphaned'
WHERE c.id IS NULL;

-- Deduplicate entities
WITH ranked AS (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY email ORDER BY created_at DESC
  ) AS rn FROM customers
)
DELETE FROM customers WHERE rn &gt; 1;</pre>
    </div>
    <div style="background:#161B22;border:1px solid #21262D;border-radius:10px;
                padding:20px;margin-bottom:14px">
      <div style="font-size:11px;font-weight:700;color:#6E7681;text-transform:uppercase;
                  letter-spacing:1px;margin-bottom:16px">📈 Impact & Effort Matrix</div>
      <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px">
        <div style="background:#0D1117;border-radius:8px;padding:14px;text-align:center">
          <div style="font-size:22px;font-weight:900;color:#F85149;font-family:'JetBrains Mono',monospace">4h</div>
          <div style="font-size:11px;color:#6E7681;margin-top:4px">Avg fix time</div>
        </div>
        <div style="background:#0D1117;border-radius:8px;padding:14px;text-align:center">
          <div style="font-size:22px;font-weight:900;color:#3FB950;font-family:'JetBrains Mono',monospace">93%</div>
          <div style="font-size:11px;color:#6E7681;margin-top:4px">Fixable with SQL</div>
        </div>
        <div style="background:#0D1117;border-radius:8px;padding:14px;text-align:center">
          <div style="font-size:22px;font-weight:900;color:#58A6FF;font-family:'JetBrains Mono',monospace">2w</div>
          <div style="font-size:11px;color:#6E7681;margin-top:4px">Est. to clean</div>
        </div>
      </div>
    </div>"""

    n_fixes    = len(recs)
    n_critical = sum(1 for r in recs if r.get("severity") == "critical")
    n_high     = sum(1 for r in recs if r.get("severity") == "high")

    st.markdown(f"""
    <div style="position:relative;margin:32px 0 0;border-radius:12px;overflow:hidden">
      <!-- BLURRED REAL CONTENT -->
      <div style="filter:blur(5px);pointer-events:none;user-select:none;
                  opacity:0.75;max-height:820px;overflow:hidden">
        {blurred_cards_html}
        {sql_flair}
      </div>

      <!-- GRADIENT FADE (bottom) -->
      <div style="position:absolute;bottom:0;left:0;right:0;height:420px;
                  background:linear-gradient(180deg,transparent 0%,#0D1117 52%);
                  pointer-events:none"></div>

      <!-- LOCK OVERLAY -->
      <div style="position:absolute;bottom:0;left:0;right:0;
                  display:flex;flex-direction:column;align-items:center;
                  padding:40px 24px 36px;text-align:center">

        <div style="width:64px;height:64px;
                    background:linear-gradient(135deg,#F85149,#F0883E);
                    border-radius:50%;display:flex;align-items:center;
                    justify-content:center;font-size:28px;margin-bottom:20px;
                    box-shadow:0 0 40px rgba(248,81,73,0.35)">🔒</div>

        <div style="font-size:26px;font-weight:900;color:#E6EDF3;
                    margin-bottom:10px;letter-spacing:-0.5px">
          Your Full Remediation Plan is Ready
        </div>
        <div style="font-size:14px;color:#8B949E;max-width:540px;
                    line-height:1.8;margin-bottom:24px">
          <strong style="color:#C9D1D9">{n_fixes} fixes identified</strong> —
          {n_critical} critical &nbsp;·&nbsp; {n_high} high priority.<br>
          Root cause · SQL queries · Step-by-step guides · Effort estimates · Prevention.
        </div>

        <div style="display:flex;justify-content:center;flex-wrap:wrap;gap:8px;margin-bottom:28px">
          <span style="background:#1C1000;border:1px solid #F85149;border-radius:999px;
                       padding:5px 16px;font-size:12px;color:#F85149;font-weight:700">
            💻 SQL fix queries
          </span>
          <span style="background:#21262D;border:1px solid #30363D;border-radius:999px;
                       padding:5px 16px;font-size:12px;color:#8B949E">
            🔍 Root cause analysis
          </span>
          <span style="background:#21262D;border:1px solid #30363D;border-radius:999px;
                       padding:5px 16px;font-size:12px;color:#8B949E">
            ⏱ Effort estimates
          </span>
          <span style="background:#21262D;border:1px solid #30363D;border-radius:999px;
                       padding:5px 16px;font-size:12px;color:#8B949E">
            🛡 Prevention strategies
          </span>
          <span style="background:#21262D;border:1px solid #30363D;border-radius:999px;
                       padding:5px 16px;font-size:12px;color:#8B949E">
            📊 Priority ranking
          </span>
        </div>

      </div>
    </div>""", unsafe_allow_html=True)

    if not st.session_state.get("show_form"):
        _, btn_c, _ = st.columns([1, 2, 1])
        with btn_c:
            if st.button("📨  Unlock Full Remediation Plan →",
                         use_container_width=True, type="primary"):
                st.session_state.show_form = True
                st.rerun()

    # LEAD FORM
    if st.session_state.get("show_form"):
        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
        st.markdown("""
        <div class="card" style="margin-top:4px">
          <div style="font-size:17px;font-weight:800;color:#E6EDF3;margin-bottom:4px">
            Get Your Full Data Quality Report
          </div>
          <div style="font-size:13px;color:#6E7681;margin-bottom:24px">
            We'll send the complete remediation guide to your email.
            No spam — unsubscribe anytime.
          </div>
        </div>
        """, unsafe_allow_html=True)

        with st.form("lead_capture", clear_on_submit=False):
            c1, c2 = st.columns(2)
            with c1:
                name    = st.text_input("Full Name *",    placeholder="Jane Smith")
                company = st.text_input("Company *",      placeholder="Acme Corp")
            with c2:
                email = st.text_input("Work Email *",   placeholder="jane@company.com")
                role  = st.selectbox("Your Role *", [
                    "Select…", "Data Engineer", "Data Analyst",
                    "Analytics / BI Manager", "Data Governance Lead",
                    "CTO / VP Engineering", "Other",
                ])
            consent = st.checkbox(
                "I agree to receive the full report and occasional data quality insights. Unsubscribe anytime.")
            submitted = st.form_submit_button(
                "📊  Send My Full Report →", type="primary", use_container_width=True)

            if submitted:
                errs = []
                if not name.strip():   errs.append("Full Name is required.")
                if not company.strip():errs.append("Company is required.")
                if not email.strip() or "@" not in email: errs.append("A valid work email is required.")
                if role == "Select…":  errs.append("Please select your role.")
                if not consent:        errs.append("Please accept the terms to continue.")
                if errs:
                    for e in errs: st.error(e)
                else:
                    save_lead(name.strip(), company.strip(), email.strip(), role)
                    st.session_state.user_info      = {"name": name.strip(), "company": company.strip(),
                                                       "email": email.strip(), "role": role}
                    st.session_state.email_submitted = True
                    st.session_state.show_form       = False
                    st.rerun()


if __name__ == "__main__":
    main()
