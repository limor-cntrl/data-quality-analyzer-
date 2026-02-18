"""
Data Quality Analyzer — Streamlit Application
==============================================
Upload 1–5 related CSV files → get an industry-standard quality score,
critical integration findings, and (after email capture) full remediation steps.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import csv
import os
from datetime import datetime

st.set_page_config(
    page_title="Data Quality Analyzer",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

from data_quality_engine import (
    detect_join_keys,
    check_orphan_records,
    check_entity_duplicates,
    check_process_gaps,
)
from scoring import (
    calculate_scores,
    generate_recommendations,
    score_label,
    overall_grade,
)

# ─────────────────────────────────────────────────────────────────────────────
# CSS
# ─────────────────────────────────────────────────────────────────────────────

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
.stApp { background: #F0F2F6; }
#MainMenu, footer { visibility: hidden; }
.stDeployButton { display: none; }

/* ── Header ── */
.dq-header {
    background: linear-gradient(135deg, #1E1B4B 0%, #3730A3 55%, #6366F1 100%);
    padding: 44px 40px 36px;
    border-radius: 16px;
    margin-bottom: 28px;
    color: white;
}
.dq-header h1 { font-size: 36px; font-weight: 800; margin: 0; letter-spacing: -0.5px; }
.dq-header p  { font-size: 16px; opacity: 0.85; margin: 10px 0 0; font-weight: 400; line-height: 1.5; }

/* ── Cards ── */
.card {
    background: white;
    border-radius: 14px;
    padding: 26px;
    box-shadow: 0 2px 10px rgba(0,0,0,0.07);
}

/* ── Dimension bars ── */
.dim-row { margin-bottom: 16px; }
.dim-header { display: flex; justify-content: space-between; margin-bottom: 5px; }
.dim-name   { font-size: 13px; font-weight: 600; color: #374151; }
.dim-val    { font-size: 13px; font-weight: 700; }
.dim-track  { background: #F3F4F6; border-radius: 999px; height: 9px; overflow: hidden; }
.dim-fill   { border-radius: 999px; height: 9px; }

/* ── Finding cards ── */
.finding {
    background: white;
    border-radius: 12px;
    padding: 20px 22px;
    margin-bottom: 14px;
    border-left: 5px solid;
    box-shadow: 0 2px 8px rgba(0,0,0,0.06);
}
.finding-critical { border-left-color: #EF4444; }
.finding-high     { border-left-color: #F97316; }
.finding-medium   { border-left-color: #F59E0B; }

.sev-tag {
    display: inline-block;
    font-size: 11px;
    font-weight: 700;
    padding: 2px 10px;
    border-radius: 999px;
    text-transform: uppercase;
    letter-spacing: 0.4px;
}

/* ── Teaser recommendations ── */
.rec-teaser {
    background: #FAFAFA;
    border: 1px solid #E5E7EB;
    border-radius: 10px;
    padding: 18px 20px;
    margin-bottom: 10px;
}

/* ── Blurred preview ── */
.rec-blur {
    filter: blur(4px);
    pointer-events: none;
    user-select: none;
    background: #F9FAFB;
    border-radius: 10px;
    padding: 18px 20px;
    border: 1px solid #E5E7EB;
    margin-bottom: 10px;
    opacity: 0.55;
}

/* ── Locked CTA box ── */
.locked-box {
    background: linear-gradient(135deg, #3730A3, #7C3AED);
    border-radius: 14px;
    padding: 36px;
    text-align: center;
    color: white;
    margin: 20px 0;
}

/* ── Full recommendation ── */
.rec-full {
    border-radius: 10px;
    padding: 20px;
    margin-bottom: 14px;
    border: 1px solid;
}

/* ── Info chips ── */
.info-chip {
    display: inline-block;
    background: rgba(255,255,255,0.15);
    border-radius: 999px;
    padding: 4px 14px;
    font-size: 13px;
    margin: 4px 4px 0 0;
}
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# Color helpers
# ─────────────────────────────────────────────────────────────────────────────

def score_color(s):
    if s is None: return "#9CA3AF"
    if s >= 90:   return "#10B981"
    if s >= 80:   return "#34D399"
    if s >= 70:   return "#F59E0B"
    if s >= 60:   return "#F97316"
    return               "#EF4444"

SEV_STYLES = {
    "critical": {"bg": "#FEE2E2", "text": "#991B1B", "border": "#FCA5A5", "card_bg": "#FFF5F5"},
    "high":     {"bg": "#FFEDD5", "text": "#9A3412", "border": "#FDBA74", "card_bg": "#FFFAF5"},
    "medium":   {"bg": "#FEF9C3", "text": "#854D0E", "border": "#FDE68A", "card_bg": "#FEFEF5"},
}

# ─────────────────────────────────────────────────────────────────────────────
# Component renderers
# ─────────────────────────────────────────────────────────────────────────────

DIMENSIONS = [
    ("completeness", "Completeness",  "Non-null rate across all columns"),
    ("uniqueness",   "Uniqueness",    "Duplicate-free rate + entity duplicate penalty"),
    ("validity",     "Validity",      "Format, range, and type conformance"),
    ("consistency",  "Consistency",   "Cross-file referential integrity"),
    ("timeliness",   "Timeliness",    "Data freshness via date columns"),
]


def make_gauge(score: float) -> go.Figure:
    color = score_color(score)
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=score,
        number={"font": {"size": 54, "color": color, "family": "Inter"}, "suffix": ""},
        gauge={
            "axis": {
                "range": [0, 100], "tickwidth": 1,
                "tickcolor": "#D1D5DB",
                "tickfont": {"size": 11, "color": "#9CA3AF"},
            },
            "bar": {"color": color, "thickness": 0.22},
            "bgcolor": "white",
            "borderwidth": 0,
            "steps": [
                {"range": [0,  50], "color": "#FEF2F2"},
                {"range": [50, 70], "color": "#FFF7ED"},
                {"range": [70, 85], "color": "#FFFBEB"},
                {"range": [85,100], "color": "#F0FDF4"},
            ],
            "threshold": {
                "line": {"color": color, "width": 3},
                "thickness": 0.75,
                "value": score,
            },
        },
    ))
    fig.update_layout(
        height=270,
        margin=dict(t=20, b=10, l=10, r=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font={"family": "Inter"},
    )
    return fig


def render_dim_bars(scores: dict, weights: dict):
    bars = []
    for key, label, desc in DIMENSIONS:
        val = scores.get(key)
        w   = weights.get(key, 0)
        lbl_txt, _ = score_label(val)
        color  = score_color(val)
        pct    = val if val is not None else 0
        val_str = f"{val:.0f}" if val is not None else "N/A"
        w_pct   = int(w * 100)

        bars.append(f"""
        <div class="dim-row">
          <div class="dim-header">
            <span class="dim-name" title="{desc}">
              {label}
              <span style="color:#9CA3AF;font-size:11px;font-weight:400"> ({w_pct}% weight)</span>
            </span>
            <span class="dim-val" style="color:{color}">
              {val_str} &nbsp;<span style="font-size:12px;font-weight:500">{lbl_txt}</span>
            </span>
          </div>
          <div class="dim-track">
            <div class="dim-fill" style="width:{pct}%;background:{color}"></div>
          </div>
        </div>""")

    st.markdown("".join(bars), unsafe_allow_html=True)


def render_finding(title, metric, severity, details, examples=None):
    s = SEV_STYLES.get(severity, SEV_STYLES["medium"])
    ex_html = ""
    if examples:
        chips = " &nbsp;·&nbsp; ".join(f"<code style='font-size:11px'>{v}</code>" for v in examples[:5])
        ex_html = f'<div style="margin-top:10px;font-size:12px;color:#6B7280">Examples: {chips}</div>'

    st.markdown(f"""
    <div class="finding finding-{severity}">
      <div style="display:flex;align-items:center;gap:10px;margin-bottom:8px">
        <span class="sev-tag" style="background:{s['bg']};color:{s['text']}">{severity.upper()}</span>
        <span style="font-size:15px;font-weight:600;color:#1F2937">{title}</span>
      </div>
      <div style="font-size:22px;font-weight:700;color:{s['text']};margin-bottom:4px">{metric}</div>
      <div style="font-size:13px;color:#6B7280">{details}</div>
      {ex_html}
    </div>""", unsafe_allow_html=True)


def render_rec_teaser(rec: dict):
    st.markdown(f"""
    <div class="rec-teaser">
      <div style="display:flex;gap:12px;align-items:flex-start">
        <span style="font-size:22px;line-height:1.3">{rec['icon']}</span>
        <div style="flex:1">
          <div style="font-size:14px;font-weight:600;color:#1F2937">{rec['title']}</div>
          <div style="font-size:13px;color:#6B7280;margin-top:4px">{rec['teaser_metric']}</div>
          <div style="font-size:13px;color:#DC2626;margin-top:5px;font-weight:500">
            ⚠️&nbsp; {rec['teaser_impact']}
          </div>
          <div style="margin-top:10px;font-size:12px;color:#9CA3AF;font-style:italic">
            🔒 Step-by-step fix, SQL queries &amp; prevention strategy — unlock below
          </div>
        </div>
      </div>
    </div>""", unsafe_allow_html=True)


def render_rec_full(rec: dict):
    s     = SEV_STYLES.get(rec["severity"], SEV_STYLES["medium"])
    steps = "".join(f"<li style='margin-bottom:7px'>{step}</li>" for step in rec["full_steps"])
    st.markdown(f"""
    <div class="rec-full" style="background:{s['card_bg']};border-color:{s['border']}">
      <div style="display:flex;align-items:center;gap:10px;margin-bottom:12px">
        <span style="font-size:22px">{rec['icon']}</span>
        <span style="font-size:15px;font-weight:700;color:#1F2937">{rec['title']}</span>
        <span class="sev-tag" style="background:{s['bg']};color:{s['text']};margin-left:auto">
          {rec['severity'].upper()}
        </span>
      </div>
      <p style="font-size:13px;color:#374151;margin:0 0 12px">
        <strong>Root cause:</strong> {rec['full_root_cause']}
      </p>
      <p style="font-size:13px;font-weight:600;color:#1F2937;margin:0 0 6px">Step-by-step fix:</p>
      <ol style="font-size:13px;color:#374151;margin:0 0 14px;padding-left:18px;line-height:1.6">
        {steps}
      </ol>
      <div style="display:flex;flex-wrap:wrap;gap:16px;font-size:12px;color:#6B7280;border-top:1px solid {s['border']};padding-top:10px;margin-top:4px">
        <span>⏱️ <strong>Effort:</strong> {rec['effort']}</span>
        <span>🛡️ <strong>Prevention:</strong> {rec['prevention']}</span>
      </div>
    </div>""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# Lead capture (save to local CSV for CRM integration)
# ─────────────────────────────────────────────────────────────────────────────

LEADS_FILE = os.path.join(os.path.dirname(__file__), "leads.csv")


def save_lead(name: str, company: str, email: str, role: str):
    file_exists = os.path.isfile(LEADS_FILE)
    with open(LEADS_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["timestamp", "name", "company", "email", "role"])
        if not file_exists:
            writer.writeheader()
        writer.writerow({
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "name": name, "company": company, "email": email, "role": role,
        })


# ─────────────────────────────────────────────────────────────────────────────
# Core analysis
# ─────────────────────────────────────────────────────────────────────────────

def run_analysis(uploaded_files) -> tuple:
    dfs, errors = {}, []

    for f in uploaded_files:
        name = f.name.replace(".csv", "").replace(".CSV", "")
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

    return {
        "dfs": dfs, "joins": joins,
        "orphans": orphans, "dupes": dupes, "gaps": gaps,
        "score_data": score_data, "recs": recs,
    }, errors


# ─────────────────────────────────────────────────────────────────────────────
# App layout
# ─────────────────────────────────────────────────────────────────────────────

def main():

    # ── Header ───────────────────────────────────────────────────────────────
    st.markdown("""
    <div class="dq-header">
      <h1>📊 Data Quality Analyzer</h1>
      <p>
        Upload 1–5 related CSV files from the same business process.<br>
        Get an industry-standard quality score, critical cross-file findings,
        and a prioritized remediation plan.
      </p>
    </div>
    """, unsafe_allow_html=True)

    # ── File upload ───────────────────────────────────────────────────────────
    up_col, hint_col = st.columns([3, 1], gap="medium")

    with up_col:
        uploaded_files = st.file_uploader(
            "Drop your CSV files here (up to 5 related files — e.g. orders, customers, invoices)",
            type=["csv"],
            accept_multiple_files=True,
            help="Files should come from the same business process so cross-file checks make sense.",
        )

    with hint_col:
        st.markdown("""
        <div class="card" style="padding:18px">
          <div style="font-size:13px;font-weight:700;color:#1F2937;margin-bottom:10px">What we measure</div>
          <div style="font-size:12px;color:#6B7280;line-height:1.9">
            ✅ Completeness (nulls)<br>
            ✅ Uniqueness &amp; duplicates<br>
            ✅ Format &amp; range validity<br>
            ✅ Cross-file referential integrity<br>
            ✅ Data freshness / timeliness
          </div>
        </div>
        """, unsafe_allow_html=True)

    if not uploaded_files:
        st.markdown("""
        <div style="text-align:center;padding:48px 0;color:#9CA3AF">
          <div style="font-size:48px">☝️</div>
          <div style="font-size:15px;margin-top:8px">Upload your CSV files to get started</div>
        </div>
        """, unsafe_allow_html=True)
        return

    if len(uploaded_files) > 5:
        st.warning("Please upload up to 5 files at a time.")
        return

    # ── Analyze button ────────────────────────────────────────────────────────
    btn_col, _ = st.columns([2, 5])
    with btn_col:
        do_analyze = st.button("🔍  Analyze Data Quality", type="primary", use_container_width=True)

    if do_analyze:
        for key in ["results", "email_submitted", "show_form", "user_info"]:
            st.session_state.pop(key, None)

        progress = st.progress(0, text="Loading files…")
        results, errors = run_analysis(uploaded_files)
        progress.progress(100, text="Done!")
        progress.empty()

        for e in errors:
            st.error(e)
        if results:
            st.session_state.results = results

    if "results" not in st.session_state:
        return

    R      = st.session_state.results
    scores = R["score_data"]["scores"]
    weights = R["score_data"]["weights"]
    overall = scores["overall"]
    grade, grade_color = overall_grade(overall)
    lbl, lbl_color     = score_label(overall)
    recs = R["recs"]

    st.markdown("---")

    # ── SECTION 1 — Score Overview ────────────────────────────────────────────
    st.markdown("### Data Quality Score")

    gauge_col, grade_col, dim_col = st.columns([1.6, 1, 2.2], gap="medium")

    with gauge_col:
        st.plotly_chart(make_gauge(overall), use_container_width=True,
                        config={"displayModeBar": False})

    with grade_col:
        files_text = ", ".join(f"<strong>{k}</strong>" for k in R["dfs"].keys())
        num_files  = len(R["dfs"])
        rows_total = sum(len(df) for df in R["dfs"].values())
        joins_found = len(R["joins"])
        st.markdown(f"""
        <div style="padding:12px 0">
          <div style="font-size:88px;font-weight:800;color:{grade_color};line-height:1">{grade}</div>
          <div style="font-size:22px;font-weight:700;color:{lbl_color};margin-top:6px">{lbl}</div>
          <div style="font-size:12px;color:#9CA3AF;margin-top:16px;line-height:1.8">
            {num_files} file{'s' if num_files > 1 else ''} · {rows_total:,} rows<br>
            {joins_found} join key{'s' if joins_found != 1 else ''} detected<br>
            {files_text}
          </div>
        </div>
        """, unsafe_allow_html=True)

    with dim_col:
        st.markdown("""
        <div style="font-size:13px;font-weight:700;color:#374151;margin-bottom:14px">
          Score by dimension
        </div>
        """, unsafe_allow_html=True)
        render_dim_bars(scores, weights)

    # ── SECTION 2 — Critical Findings ─────────────────────────────────────────
    st.markdown("---")
    st.markdown("### 🚨 Critical Findings")
    st.markdown(
        '<p style="color:#6B7280;font-size:14px;margin-bottom:18px">'
        'Issues discovered by analyzing <em>relationships between files</em> — not just individual columns. '
        'Data that looks clean in isolation often breaks at the joins.'
        '</p>',
        unsafe_allow_html=True,
    )

    shown = 0

    for f in R["orphans"].get("findings", [])[:2]:
        pct = f["pct_of_source"]
        sev = "critical" if pct > 30 else ("high" if pct > 10 else "medium")
        render_finding(
            title=f"Orphan records — {f['direction']}",
            metric=f"{f['orphan_count']:,} records ({pct}%) have no matching counterpart",
            severity=sev,
            details=f"Key: '{f['key']}'. These records are invisible in joined reports and revenue roll-ups.",
            examples=f["example_values"],
        )
        shown += 1

    for f in R["dupes"].get("findings", [])[:1]:
        sev = "critical" if f["duplicate_count"] > 10 else "high"
        names = [e.get("name") or e.get("value_a", "") for e in f["examples"][:3]]
        render_finding(
            title=f"Entity duplicates — '{f['file']}' ({f['type']})",
            metric=f"{f['duplicate_count']} duplicate entities",
            severity=sev,
            details="Same real-world entity under multiple IDs — inflates counts, risks double billing.",
            examples=names,
        )
        shown += 1

    for f in R["gaps"].get("findings", [])[:1]:
        pct = f["pct_of_upstream"]
        sev = "critical" if pct > 20 else ("high" if pct > 5 else "medium")
        render_finding(
            title=f"Process gap — {f['stage_from']} → {f['stage_to']}",
            metric=f"{f['missing_count']:,} records ({pct}%) stalled between stages",
            severity=sev,
            details="Records exist in the upstream stage but never appear in the downstream stage.",
            examples=f["example_ids"],
        )
        shown += 1

    if shown == 0:
        st.success("✅ No critical integration issues detected. Your data looks clean across all checked dimensions.")

    # ── SECTION 3 — Recommendations ──────────────────────────────────────────
    st.markdown("---")
    st.markdown("### 💡 Improvement Recommendations")

    if not recs:
        st.success("No specific recommendations — data quality is solid!")
        return

    # ── Unlocked (after email) ────────────────────────────────────────────────
    if st.session_state.get("email_submitted"):
        name_val = st.session_state.get("user_info", {}).get("name", "")
        st.success(f"✅ Full recommendations unlocked — welcome, {name_val}!")
        st.markdown(
            '<p style="color:#6B7280;font-size:14px;margin-bottom:18px">'
            'Prioritized by severity. Includes root cause, step-by-step SQL fixes, and prevention strategy.'
            '</p>',
            unsafe_allow_html=True,
        )
        for rec in recs:
            render_rec_full(rec)
        return

    # ── Teaser (free, always visible) ────────────────────────────────────────
    st.markdown(
        '<p style="color:#6B7280;font-size:14px;margin-bottom:16px">'
        'Here\'s a preview of what needs to be fixed. '
        'Enter your details below to unlock the full step-by-step remediation plan.</p>',
        unsafe_allow_html=True,
    )

    for rec in recs[:3]:
        render_rec_teaser(rec)

    # Blurred preview of remaining recommendations
    extra = len(recs) - 3
    if extra > 0:
        extra_lines = "".join(
            f'<div style="font-size:13px;color:#374151;margin-bottom:6px">'
            f'{r["icon"]} {r["title"]} — {r["teaser_metric"]}</div>'
            for r in recs[3:]
        )
        st.markdown(f"""
        <div class="rec-blur">
          <div style="font-size:12px;font-weight:700;color:#6B7280;margin-bottom:10px;text-transform:uppercase;letter-spacing:0.5px">
            + {extra} more recommendation{'s' if extra > 1 else ''} (locked)
          </div>
          {extra_lines}
        </div>""", unsafe_allow_html=True)

    # Lock CTA
    st.markdown("""
    <div class="locked-box">
      <div style="font-size:36px;margin-bottom:10px">🔒</div>
      <div style="font-size:22px;font-weight:700;margin-bottom:8px">Unlock Full Recommendations</div>
      <div style="font-size:14px;opacity:0.85;max-width:500px;margin:0 auto;line-height:1.6">
        Get specific SQL queries, step-by-step fix guides, root cause analysis,
        effort estimates, and prevention strategies — tailored to your data.
      </div>
      <div style="margin-top:16px">
        <span class="info-chip">📋 Root cause analysis</span>
        <span class="info-chip">💻 SQL fix queries</span>
        <span class="info-chip">⏱️ Effort estimates</span>
        <span class="info-chip">🛡️ Prevention strategies</span>
      </div>
    </div>
    """, unsafe_allow_html=True)

    # Toggle form button
    if not st.session_state.get("show_form"):
        _, btn_center, _ = st.columns([1, 2, 1])
        with btn_center:
            if st.button("📨  Get My Full Recommendations →",
                         use_container_width=True, type="primary"):
                st.session_state.show_form = True
                st.rerun()

    # ── Lead capture form ─────────────────────────────────────────────────────
    if st.session_state.get("show_form"):
        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
        with st.container():
            st.markdown("""
            <div class="card" style="margin-top:4px">
              <div style="font-size:17px;font-weight:700;color:#1F2937;margin-bottom:4px">
                Get Your Full Data Quality Report
              </div>
              <div style="font-size:13px;color:#6B7280;margin-bottom:20px">
                We'll send the complete remediation guide to your email.
                No spam — unsubscribe anytime.
              </div>
            </div>
            """, unsafe_allow_html=True)

            with st.form("lead_capture", clear_on_submit=False):
                row1_a, row1_b = st.columns(2)
                with row1_a:
                    name    = st.text_input("Full Name *", placeholder="Jane Smith")
                    company = st.text_input("Company *",   placeholder="Acme Corp")
                with row1_b:
                    email = st.text_input("Work Email *", placeholder="jane@company.com")
                    role  = st.selectbox("Your Role *", [
                        "Select…",
                        "Data Engineer",
                        "Data Analyst",
                        "Analytics / BI Manager",
                        "Data Governance Lead",
                        "CTO / VP Engineering",
                        "Other",
                    ])

                consent = st.checkbox(
                    "I agree to receive the full report and occasional data quality insights. "
                    "No spam — unsubscribe anytime."
                )

                submitted = st.form_submit_button(
                    "📊  Send My Full Report →",
                    type="primary",
                    use_container_width=True,
                )

                if submitted:
                    errors_form = []
                    if not name.strip():    errors_form.append("Full Name is required.")
                    if not company.strip(): errors_form.append("Company is required.")
                    if not email.strip() or "@" not in email or "." not in email:
                        errors_form.append("A valid work email is required.")
                    if role == "Select…":   errors_form.append("Please select your role.")
                    if not consent:         errors_form.append("Please accept the terms to continue.")

                    if errors_form:
                        for err in errors_form:
                            st.error(err)
                    else:
                        save_lead(name.strip(), company.strip(), email.strip(), role)
                        st.session_state.user_info = {
                            "name": name.strip(), "company": company.strip(),
                            "email": email.strip(), "role": role,
                        }
                        st.session_state.email_submitted = True
                        st.session_state.show_form       = False
                        st.rerun()


if __name__ == "__main__":
    main()
