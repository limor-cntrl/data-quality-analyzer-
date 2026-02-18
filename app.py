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
from semantic import detect_entity, detect_domain, estimate_monetary_impact, generate_narrative, classify_columns

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
