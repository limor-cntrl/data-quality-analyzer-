"""
Data Quality Intelligence Platform
=====================================
Your data looks clean. It isn't.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import time, csv, os, re
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


def make_gauge(score: float) -> go.Figure:
    c = score_color(score)
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=score,
        number={"font": {"size": 60, "color": c, "family": "JetBrains Mono"}, "suffix": ""},
        gauge={
            "axis": {"range": [0, 100], "tickwidth": 1,
                     "tickcolor": "#30363D", "tickfont": {"color": "#6E7681", "size": 10}},
            "bar":  {"color": c, "thickness": 0.2},
            "bgcolor": "#0D1117", "borderwidth": 0,
            "steps": [
                {"range": [0,  50], "color": "#1a0808"},
                {"range": [50, 65], "color": "#1a1000"},
                {"range": [65, 75], "color": "#1a1500"},
                {"range": [75, 85], "color": "#0a1a0a"},
                {"range": [85,100], "color": "#051a10"},
            ],
            "threshold": {"line": {"color": c, "width": 3}, "thickness": 0.8, "value": score},
        },
    ))
    fig.update_layout(
        height=260, margin=dict(t=20, b=10, l=10, r=10),
        paper_bgcolor="#161B22", plot_bgcolor="#161B22",
        font={"family": "Inter"},
    )
    return fig


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

def find_orphan_rows(df: pd.DataFrame, name: str, dfs: dict, joins: list) -> tuple:
    """Return (set of orphan row indices, set of orphan column names) for this file."""
    orphan_indices, orphan_cols = set(), set()
    for j in joins:
        if j["file_a"] == name:
            col_self, other_name, col_other = j.get("col_a", j["key"]), j["file_b"], j.get("col_b", j["key"])
        elif j["file_b"] == name:
            col_self, other_name, col_other = j.get("col_b", j["key"]), j["file_a"], j.get("col_a", j["key"])
        else:
            continue
        if col_self not in df.columns or other_name not in dfs or col_other not in dfs[other_name].columns:
            continue
        other_vals = set(dfs[other_name][col_other].dropna().astype(str))
        mask = ~df[col_self].astype(str).isin(other_vals)
        orphan_indices.update(df.index[mask])
        orphan_cols.add(col_self)
    return orphan_indices, orphan_cols


def find_dup_rows(df: pd.DataFrame) -> set:
    """Return indices of ALL rows that are exact duplicates of another row."""
    return set(df.index[df.duplicated(keep=False)])


def find_invalid_cells(df: pd.DataFrame) -> set:
    """Return set of (row_idx, col) for cells with validity issues."""
    invalid = set()
    for col in df.columns:
        if re.search(r"(amount|price|cost|revenue|salary|fee|total)", col, re.IGNORECASE):
            if pd.api.types.is_numeric_dtype(df[col]):
                for idx in df.index[df[col] < 0]:
                    invalid.add((idx, col))
        if re.search(r"(date|time|created|updated|timestamp)", col, re.IGNORECASE):
            try:
                parsed = pd.to_datetime(df[col], errors="coerce")
                for idx in parsed.index[parsed > pd.Timestamp.now()]:
                    invalid.add((idx, col))
                for idx in df.index[parsed.isna() & df[col].notna()]:
                    invalid.add((idx, col))
            except Exception:
                pass
    return invalid


def render_data_preview(dfs: dict, joins: list):
    """Render annotated data preview with problem cells highlighted."""
    st.markdown('<hr class="dq-divider">', unsafe_allow_html=True)
    st.markdown("""
    <div class="section-header">Annotated Data Preview</div>
    <div class="section-title">Your Data — Problems Highlighted</div>
    <div class="section-sub">
      Diagnosis applied directly to your data. Every highlighted cell is a real issue found in your files.
    </div>
    """, unsafe_allow_html=True)

    # Legend
    st.markdown("""
    <div style="display:flex;gap:24px;flex-wrap:wrap;margin-bottom:16px;
                padding:12px 16px;background:#161B22;border:1px solid #21262D;border-radius:8px">
      <span style="font-size:12px;color:#F85149">🔴&nbsp; Missing / null value</span>
      <span style="font-size:12px;color:#F0883E">🟠&nbsp; Orphan record (no match in related file)</span>
      <span style="font-size:12px;color:#58A6FF">🔵&nbsp; Exact duplicate row</span>
      <span style="font-size:12px;color:#E3B341">🟡&nbsp; Invalid value (negative amount / bad date)</span>
    </div>
    """, unsafe_allow_html=True)

    tabs = st.tabs([f"📄 {name}" for name in dfs.keys()])

    for tab, (name, df) in zip(tabs, dfs.items()):
        with tab:
            preview = df.head(100).reset_index(drop=True)

            orphan_indices, orphan_cols = find_orphan_rows(df.head(100), name, dfs, joins)
            dup_indices    = find_dup_rows(preview)
            invalid_cells  = find_invalid_cells(preview)

            # Counts for metrics
            null_count   = int(preview.isna().sum().sum())
            orphan_count = len(orphan_indices)
            dup_count    = len(dup_indices)
            invalid_count = len(invalid_cells)

            # Metrics row
            m1, m2, m3, m4, m5 = st.columns(5)
            m1.metric("Rows shown",     f"{len(preview):,} / {len(df):,}")
            m2.metric("Missing values", null_count,    delta=f"-{null_count}"    if null_count    else None, delta_color="inverse")
            m3.metric("Orphan rows",    orphan_count,  delta=f"-{orphan_count}"  if orphan_count  else None, delta_color="inverse")
            m4.metric("Duplicate rows", dup_count,     delta=f"-{dup_count}"     if dup_count     else None, delta_color="inverse")
            m5.metric("Invalid cells",  invalid_count, delta=f"-{invalid_count}" if invalid_count else None, delta_color="inverse")

            # Build styler
            def _make_style_fn(preview, orphan_indices, orphan_cols, dup_indices, invalid_cells):
                def style_row(row):
                    styles = []
                    idx = row.name
                    for col in preview.columns:
                        val = row[col]
                        is_null = pd.isna(val) or (isinstance(val, str) and val.strip() == "")
                        is_orphan_col = col in orphan_cols
                        is_orphan_row = idx in orphan_indices
                        is_dup        = idx in dup_indices
                        is_invalid    = (idx, col) in invalid_cells

                        if is_null:
                            styles.append("background-color:#3d0f0f;color:#F85149;font-weight:700")
                        elif is_orphan_row and is_orphan_col:
                            styles.append("background-color:#2d1500;color:#F0883E;font-weight:700")
                        elif is_orphan_row:
                            styles.append("background-color:#1a0d00;color:#c97a50")
                        elif is_dup:
                            styles.append("background-color:#0d1a2d;color:#58A6FF")
                        elif is_invalid:
                            styles.append("background-color:#2a1d00;color:#E3B341;font-weight:700")
                        else:
                            styles.append("")
                    return styles
                return style_row

            styled = preview.style.apply(
                _make_style_fn(preview, orphan_indices, orphan_cols, dup_indices, invalid_cells),
                axis=1
            )

            st.dataframe(styled, use_container_width=True, height=420)

            if null_count == 0 and orphan_count == 0 and dup_count == 0 and invalid_count == 0:
                st.success("✅ No issues detected in the visible rows of this file.")


# ─────────────────────────────────────────────────────────────────────────────
# Column profiler
# ─────────────────────────────────────────────────────────────────────────────

COL_ICONS = {
    "id": "🔑", "monetary": "💰", "temporal": "📅",
    "status": "🏷", "personal": "👤", "geographic": "🌍",
    "quantity": "🔢", "other": "📊",
}

def profile_columns(df: pd.DataFrame) -> list:
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


def render_column_profiler(dfs: dict):
    """Render a column-level profiler for each file."""
    st.markdown('<hr class="dq-divider">', unsafe_allow_html=True)
    st.markdown("""
    <div class="section-header">Column Intelligence</div>
    <div class="section-title">Column-Level Profile</div>
    <div class="section-sub">
      Data type, fill rate, distribution, and quality score for every column in your data.
    </div>
    """, unsafe_allow_html=True)

    tabs = st.tabs([f"📄 {name}" for name in dfs.keys()])
    for tab, (fname, df) in zip(tabs, dfs.items()):
        with tab:
            profiles = profile_columns(df)
            # Build one HTML block with CSS grid
            cards = []
            for p in profiles:
                qc  = score_color(p["quality"])
                fc  = score_color(p["fill_pct"])
                dtype_label = p["dtype"].upper()

                # Stats section varies by type
                if p["dtype"] == "numeric" and "min" in p:
                    neg_warn = (f'<div style="color:#F85149;font-size:11px;margin-top:6px">'
                                f'⚠ {p["negatives"]} negative values</div>'
                                if p.get("negatives", 0) > 0 else "")
                    stats = f"""
                    <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:4px;margin-top:10px">
                      <div style="text-align:center;background:#0D1117;border-radius:4px;padding:5px">
                        <div style="color:#6E7681;font-size:10px">min</div>
                        <div style="color:#E6EDF3;font-weight:700;font-size:12px;font-family:monospace">{p['min']:,}</div>
                      </div>
                      <div style="text-align:center;background:#0D1117;border-radius:4px;padding:5px">
                        <div style="color:#6E7681;font-size:10px">avg</div>
                        <div style="color:#E6EDF3;font-weight:700;font-size:12px;font-family:monospace">{p['mean']:,}</div>
                      </div>
                      <div style="text-align:center;background:#0D1117;border-radius:4px;padding:5px">
                        <div style="color:#6E7681;font-size:10px">max</div>
                        <div style="color:#E6EDF3;font-weight:700;font-size:12px;font-family:monospace">{p['max']:,}</div>
                      </div>
                    </div>{neg_warn}"""
                elif p["dtype"] == "datetime" and "date_min" in p:
                    fut = (f'<div style="color:#F85149;font-size:11px;margin-top:4px">⚠ {p["future"]} future dates</div>'
                           if p.get("future", 0) > 0 else "")
                    stats = f"""
                    <div style="margin-top:10px;font-size:11px;color:#8B949E">
                      <div>{p['date_min']} → {p['date_max']}</div>
                      <div style="color:#6E7681">{p.get('range_days',0):,} day range</div>
                      {fut}
                    </div>"""
                else:
                    tops = p.get("top_values", [])
                    chips = " ".join(
                        f'<span style="background:#21262D;color:#79C0FF;font-size:10px;padding:1px 7px;border-radius:4px;font-family:monospace">{v[:20]}</span>'
                        for v in tops
                    )
                    stats = f'<div style="margin-top:8px;line-height:2">{chips}</div>'

                uniq_pct = round(p["unique"] / p["total"] * 100, 0) if p["total"] else 0

                cards.append(f"""
                <div style="background:#161B22;border:1px solid #21262D;border-radius:10px;padding:16px">
                  <div style="display:flex;align-items:flex-start;justify-content:space-between;margin-bottom:10px">
                    <div style="display:flex;align-items:center;gap:8px">
                      <span style="font-size:15px">{p['icon']}</span>
                      <div>
                        <div style="font-size:13px;font-weight:700;color:#E6EDF3;font-family:monospace">{p['name']}</div>
                        <div style="font-size:10px;color:#6E7681;margin-top:1px">{dtype_label} · {p['unique']:,} unique ({uniq_pct:.0f}%)</div>
                      </div>
                    </div>
                    <span style="font-size:13px;font-weight:800;color:{qc};font-family:monospace">{p['quality']}</span>
                  </div>
                  <div>
                    <div style="display:flex;justify-content:space-between;font-size:10px;color:#6E7681;margin-bottom:3px">
                      <span>Filled</span><span style="color:{fc}">{p['fill_pct']}%</span>
                    </div>
                    <div style="background:#21262D;border-radius:999px;height:5px;overflow:hidden">
                      <div style="width:{p['fill_pct']}%;background:{fc};height:5px;border-radius:999px"></div>
                    </div>
                    {f'<div style="font-size:10px;color:#F85149;margin-top:3px">⚠ {p["nulls"]:,} missing values</div>' if p["nulls"] > 0 else ""}
                  </div>
                  {stats}
                </div>""")

            # Render as CSS grid (3 columns)
            grid = f"""
            <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-bottom:8px">
              {''.join(cards)}
            </div>"""
            st.markdown(grid, unsafe_allow_html=True)


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


def run_analysis(uploaded_files) -> tuple:
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

    joins      = detect_join_keys(dfs)
    orphans    = check_orphan_records(dfs, joins)
    dupes      = check_entity_duplicates(dfs, joins)
    gaps       = check_process_gaps(dfs, joins)
    score_data = calculate_scores(dfs, orphans, dupes, gaps, len(dfs))
    recs       = generate_recommendations(orphans, dupes, gaps, score_data)

    # Semantic layer
    entities = {name: detect_entity(name, df) for name, df in dfs.items()}
    domain, conf  = detect_domain(dfs)
    impact    = estimate_monetary_impact(dfs, orphans, dupes, gaps)
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
# Main app
# ─────────────────────────────────────────────────────────────────────────────

def main():

    # ── HERO ─────────────────────────────────────────────────────────────────
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

    # ── UPLOAD ────────────────────────────────────────────────────────────────
    up_col, info_col = st.columns([3, 1], gap="medium")

    with up_col:
        uploaded_files = st.file_uploader(
            "Drop your CSV files here — up to 5 related files from the same business process",
            type=["csv"], accept_multiple_files=True,
            help="Related files: e.g. orders + customers + invoices from the same pipeline.",
        )

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

    # ── ANALYZE BUTTON ────────────────────────────────────────────────────────
    btn_col, _ = st.columns([2, 5])
    with btn_col:
        do_analyze = st.button("🔬  Run Diagnostic", type="primary", use_container_width=True)

    if do_analyze:
        for k in ["results","email_submitted","show_form","user_info"]:
            st.session_state.pop(k, None)

        steps = [
            ("📂", "Loading and profiling files..."),
            ("🧠", "Detecting entities and business domain..."),
            ("🗺️", "Mapping relationships and join paths..."),
            ("⚡", "Running 23 quality checks..."),
            ("💥", "Calculating business impact..."),
        ]
        with st.status("Running diagnostic...", expanded=True) as status:
            for emoji, text in steps:
                st.write(f"{emoji} {text}")
                time.sleep(0.55)
            results, errors = run_analysis(uploaded_files)
            for e in errors:
                st.error(e)
            if results:
                st.session_state.results = results
                status.update(label="Diagnostic complete.", state="complete", expanded=False)
            else:
                status.update(label="Analysis failed.", state="error")

    if "results" not in st.session_state:
        return

    R       = st.session_state.results
    scores  = R["score_data"]["scores"]
    weights = R["score_data"]["weights"]
    details = R["score_data"]["details"]
    overall = scores["overall"]
    grade, grade_color = overall_grade(overall)
    lbl, lbl_color     = score_label(overall)
    recs    = R["recs"]

    # ── EXECUTIVE SUMMARY ────────────────────────────────────────────────────
    render_executive_summary(R)

    # ── MISSING FILE SUGGESTIONS ─────────────────────────────────────────────
    render_missing_file_suggestions(R["dfs"], R["domain"])

    # ── EXPORT BUTTON ────────────────────────────────────────────────────────
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

    # ── SECTION 0: DATA PREVIEW ───────────────────────────────────────────────
    render_data_preview(R["dfs"], R["joins"])

    # ── COLUMN PROFILER ───────────────────────────────────────────────────────
    render_column_profiler(R["dfs"])

    st.markdown('<hr class="dq-divider">', unsafe_allow_html=True)

    # ── SECTION 1: SCORE OVERVIEW ─────────────────────────────────────────────
    st.markdown("""
    <div class="section-header">Diagnostic Result</div>
    <div class="section-title">Data Quality Score</div>
    """, unsafe_allow_html=True)

    g_col, score_col, dim_col = st.columns([1.5, 1, 2.2], gap="medium")

    with g_col:
        st.plotly_chart(make_gauge(overall), use_container_width=True,
                        config={"displayModeBar": False})

    with score_col:
        benchmark = details.get("benchmark", "")
        urgency   = details.get("urgency", "")
        files_txt = " · ".join(f"<b>{k}</b>" for k in R["dfs"].keys())
        rows_tot  = sum(len(df) for df in R["dfs"].values())
        domain_txt = f"{R['domain']} (detected)"
        st.markdown(f"""
        <div style="padding:10px 0">
          <div class="score-grade" style="color:{grade_color}">{grade}</div>
          <div class="score-label" style="color:{lbl_color}">{lbl}</div>
          <div class="benchmark-badge">{benchmark}</div>
          <div style="font-size:12px;color:#484F58;margin-top:16px;line-height:1.9">
            {len(R['dfs'])} file{'s' if len(R['dfs'])>1 else ''} · {rows_tot:,} rows<br>
            Domain: <span style="color:#8B949E">{domain_txt}</span><br>
            {files_txt}
          </div>
          <div style="margin-top:14px;font-size:13px;color:{lbl_color};font-weight:600">
            {urgency}
          </div>
        </div>
        """, unsafe_allow_html=True)

    with dim_col:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown('<div class="card-title">Score by dimension</div>', unsafe_allow_html=True)
        render_dim_bars(scores, weights)
        if details.get("integration_penalty", 0) > 0:
            st.markdown(
                f'<div style="font-size:11px;color:#484F58;margin-top:8px">'
                f'⚠ -{details["integration_penalty"]} integration complexity penalty applied</div>',
                unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

    # ── SECTION 2: DATA MAP ────────────────────────────────────────────────────
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
        st.plotly_chart(make_flow_map(R["dfs"], R["joins"], R["orphans"], R["gaps"]),
                        use_container_width=True, config={"displayModeBar": False})
        st.markdown('</div>', unsafe_allow_html=True)

    # ── SECTION 3: WHAT YOUR DATA IS TELLING YOU ──────────────────────────────
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

    # ── SECTION 4: BUSINESS IMPACT ────────────────────────────────────────────
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

    # ── SECTION 5: CRITICAL FINDINGS ──────────────────────────────────────────
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

    # ── SECTION 6: RECOMMENDATIONS ────────────────────────────────────────────
    st.markdown('<hr class="dq-divider">', unsafe_allow_html=True)
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
        st.markdown(
            '<p style="font-size:13px;color:#6E7681;margin-bottom:18px">'
            'Prioritized by severity. Includes root cause, SQL fix queries, and prevention strategies.</p>',
            unsafe_allow_html=True)
        for rec in recs:
            render_rec_full(rec)
        return

    # TEASER
    st.markdown(
        '<p style="font-size:14px;color:#8B949E;margin-bottom:18px">'
        'A preview of your top issues. Enter your details below to unlock the full step-by-step guide.</p>',
        unsafe_allow_html=True)

    for rec in recs[:3]:
        render_rec_teaser(rec)

    # Blurred extra
    if len(recs) > 3:
        extra_lines = "".join(
            f'<div style="font-size:13px;color:#C9D1D9;margin-bottom:8px">'
            f'{r["icon"]} {r["title"]} — {r["teaser_metric"]}</div>'
            for r in recs[3:]
        )
        st.markdown(f"""
        <div class="rec-blur">
          <div style="font-size:11px;font-weight:700;color:#6E7681;text-transform:uppercase;
                      letter-spacing:1px;margin-bottom:12px">
            + {len(recs)-3} more recommendations locked
          </div>
          {extra_lines}
        </div>""", unsafe_allow_html=True)

    # Lock CTA
    st.markdown(f"""
    <div class="locked-box">
      <div style="font-size:40px;margin-bottom:12px">🔒</div>
      <div style="font-size:22px;font-weight:900;color:#E6EDF3;margin-bottom:8px">
        Unlock Full Remediation Plan
      </div>
      <div style="font-size:14px;color:#8B949E;max-width:480px;margin:0 auto;line-height:1.7">
        Get specific SQL queries, root cause analysis, step-by-step fix guides,
        effort estimates, and prevention strategies — tailored to your data.
      </div>
      <div style="margin-top:20px;display:flex;justify-content:center;flex-wrap:wrap;gap:8px">
        <span style="background:#21262D;border:1px solid #30363D;border-radius:999px;
                     padding:4px 14px;font-size:12px;color:#8B949E">💻 SQL fix queries</span>
        <span style="background:#21262D;border:1px solid #30363D;border-radius:999px;
                     padding:4px 14px;font-size:12px;color:#8B949E">🔍 Root cause analysis</span>
        <span style="background:#21262D;border:1px solid #30363D;border-radius:999px;
                     padding:4px 14px;font-size:12px;color:#8B949E">⏱ Effort estimates</span>
        <span style="background:#21262D;border:1px solid #30363D;border-radius:999px;
                     padding:4px 14px;font-size:12px;color:#8B949E">🛡 Prevention strategies</span>
      </div>
    </div>""", unsafe_allow_html=True)

    if not st.session_state.get("show_form"):
        _, btn_c, _ = st.columns([1, 2, 1])
        with btn_c:
            if st.button("📨  Get Full Remediation Plan →",
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
