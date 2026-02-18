"""
Scoring Module — Real-World Calibrated
=======================================
Industry-standard dimensions with harsher, more realistic calibration.
Most datasets score 40–65. Only well-engineered data exceeds 80.
"""

import pandas as pd
import numpy as np
import re


# ─────────────────────────────────────────────────────────────────────────────
# Score calibration helpers
# ─────────────────────────────────────────────────────────────────────────────

def _cap(v, lo=0, hi=100):
    return max(lo, min(hi, v))


def _benchmark_text(score: float) -> tuple:
    """Return (percentile_text, urgency_text)."""
    if score >= 85:
        return "Top 8% of datasets analyzed", "Your data is in excellent shape."
    if score >= 75:
        return "Top 22% of datasets analyzed", "Above average, but gaps remain."
    if score >= 65:
        return "Bottom 45% — below industry average", "Significant risks to your reports."
    if score >= 50:
        return "Bottom 25% — high risk territory", "Decision-making on this data is unreliable."
    return "Bottom 10% — critical condition", "This data should not drive business decisions."


# ─────────────────────────────────────────────────────────────────────────────
# Main scoring function
# ─────────────────────────────────────────────────────────────────────────────

def calculate_scores(dfs: dict, orphan_result: dict, duplicate_result: dict,
                     gap_result: dict, num_files: int) -> dict:
    scores  = {}
    details = {}

    # ── 1. COMPLETENESS (cap at 92 — no real dataset is 100% complete by design) ──
    total_cells    = sum(df.size for df in dfs.values())
    non_null_cells = sum(df.notna().sum().sum() for df in dfs.values())
    raw_completeness = (non_null_cells / total_cells * 100) if total_cells else 100
    scores["completeness"] = _cap(round(raw_completeness * 0.93, 1), hi=92)

    per_file = {}
    for name, df in dfs.items():
        per_file[name] = round(df.notna().sum().sum() / df.size * 100, 1) if df.size else 100
    details["completeness_per_file"] = per_file

    # ── 2. UNIQUENESS (penalise harder for entity-level duplicates) ──
    total_rows  = sum(len(df) for df in dfs.values())
    unique_rows = sum(len(df.drop_duplicates()) for df in dfs.values())
    base_uniq   = (unique_rows / total_rows * 100) if total_rows else 100

    dup_count = sum(f.get("duplicate_count", 0) for f in duplicate_result.get("findings", []))
    penalty   = min(30, dup_count * 4)
    scores["uniqueness"] = _cap(round(base_uniq - penalty, 1))
    details["uniqueness_penalty"] = penalty

    # ── 3. VALIDITY ──
    col_scores     = []
    validity_issues = []

    for fname, df in dfs.items():
        for col in df.columns:
            non_null = df[col].dropna()
            if len(non_null) == 0:
                col_scores.append(0)
                validity_issues.append(f"{fname}.{col}: entirely null")
                continue
            if non_null.nunique() == 1 and len(df) > 5:
                col_scores.append(50)
                validity_issues.append(f"{fname}.{col}: single unique value")
                continue

            if pd.api.types.is_numeric_dtype(df[col]):
                is_money = bool(re.search(
                    r"(amount|price|cost|revenue|salary|fee|total|value)", col, re.IGNORECASE))
                if is_money:
                    neg = (df[col] < 0).sum()
                    col_scores.append(_cap(100 - neg / len(df) * 200))
                    if neg:
                        validity_issues.append(f"{fname}.{col}: {neg} negative monetary values")
                elif len(non_null) > 10:
                    q25, q75 = non_null.quantile(0.25), non_null.quantile(0.75)
                    iqr = q75 - q25
                    if iqr > 0:
                        outliers = ((non_null < q25 - 3*iqr) | (non_null > q75 + 3*iqr)).sum()
                        col_scores.append(_cap(100 - outliers / len(non_null) * 120))
                    else:
                        col_scores.append(85)
                else:
                    col_scores.append(88)

            elif re.search(r"(date|time|created|updated|timestamp)", col, re.IGNORECASE):
                try:
                    parsed    = pd.to_datetime(df[col], errors="coerce")
                    fail_rate = parsed.isna().sum() / len(df)
                    future    = (parsed > pd.Timestamp.now()).sum()
                    score     = 100 - fail_rate * 60 - (future / len(df)) * 30
                    col_scores.append(_cap(round(score, 1)))
                    if future:
                        validity_issues.append(f"{fname}.{col}: {future} future dates")
                except Exception:
                    col_scores.append(70)
            else:
                col_scores.append(90)

    scores["validity"] = _cap(round(sum(col_scores) / len(col_scores), 1)) if col_scores else 85
    details["validity_issues"] = validity_issues[:8]

    # ── 4. CONSISTENCY — most punitive dimension ──
    if num_files < 2:
        scores["consistency"] = None
        details["consistency_note"] = "Single file — cross-file check not applicable"
    else:
        orphan_pcts = [f["pct_of_source"]   for f in orphan_result.get("findings", [])]
        gap_pcts    = [f["pct_of_upstream"]  for f in gap_result.get("findings", [])]
        all_issues  = orphan_pcts + gap_pcts

        if all_issues:
            worst = max(all_issues)
            avg   = sum(all_issues) / len(all_issues)
            # Harsher formula: worst issue dominates
            raw = 100 - worst * 0.65 - avg * 0.35
            scores["consistency"] = _cap(round(raw, 1))
        else:
            scores["consistency"] = 95   # never give 100 — hidden issues may exist

        details["consistency_orphan_issues"] = len(orphan_pcts)
        details["consistency_gap_issues"]    = len(gap_pcts)

    # ── 5. TIMELINESS ──
    date_series = []
    for df in dfs.values():
        for col in df.columns:
            if re.search(r"(date|time|created|updated)", col, re.IGNORECASE):
                try:
                    parsed = pd.to_datetime(df[col], errors="coerce").dropna()
                    if len(parsed):
                        date_series.append(parsed)
                except Exception:
                    pass

    if date_series:
        all_dates = pd.concat(date_series)
        latest    = all_dates.max()
        days_old  = (pd.Timestamp.now() - latest).days
        future    = int((all_dates > pd.Timestamp.now()).sum())

        if   days_old <  7:  t = 95
        elif days_old < 30:  t = 82
        elif days_old < 90:  t = 65
        elif days_old < 365: t = 45
        else:                t = 25

        scores["timeliness"] = _cap(round(t - future * 2, 1))
        details["latest_date"] = str(latest.date())
        details["days_old"]    = days_old
    else:
        # No date columns — bigger penalty (can't verify freshness)
        scores["timeliness"] = 40
        details["latest_date"] = None

    # ── STRUCTURAL INTEGRATION PENALTY ──
    # More files = more join surface = more risk
    integration_penalty = max(0, (num_files - 1) * 2)

    # ── WEIGHTED OVERALL ──
    if num_files >= 2:
        weights = {
            "completeness": 0.18,
            "uniqueness":   0.18,
            "validity":     0.14,
            "consistency":  0.35,   # dominant — this is our key differentiator
            "timeliness":   0.15,
        }
    else:
        weights = {
            "completeness": 0.30,
            "uniqueness":   0.28,
            "validity":     0.27,
            "consistency":  0.00,
            "timeliness":   0.15,
        }

    w_sum, w_total = 0.0, 0.0
    for dim, w in weights.items():
        if w > 0 and scores.get(dim) is not None:
            w_sum   += scores[dim] * w
            w_total += w

    raw_overall = (w_sum / w_total - integration_penalty) if w_total else 0
    scores["overall"] = _cap(round(raw_overall, 1), hi=92)

    # Benchmark
    bench_text, urgency = _benchmark_text(scores["overall"])
    details["benchmark"]      = bench_text
    details["urgency"]        = urgency
    details["integration_penalty"] = integration_penalty

    return {"scores": scores, "details": details, "weights": weights}


# ─────────────────────────────────────────────────────────────────────────────
# Label helpers
# ─────────────────────────────────────────────────────────────────────────────

def score_label(s) -> tuple:
    if s is None:  return "N/A",       "#6E7681"
    if s >= 85:    return "Excellent",  "#3FB950"
    if s >= 75:    return "Good",       "#56D364"
    if s >= 65:    return "Fair",       "#E3B341"
    if s >= 50:    return "Poor",       "#F0883E"
    return             "Critical",  "#F85149"


def overall_grade(s) -> tuple:
    if s >= 85:  return "A", "#3FB950"
    if s >= 75:  return "B", "#56D364"
    if s >= 65:  return "C", "#E3B341"
    if s >= 50:  return "D", "#F0883E"
    return           "F", "#F85149"


# ─────────────────────────────────────────────────────────────────────────────
# Recommendations
# ─────────────────────────────────────────────────────────────────────────────

def generate_recommendations(orphan_result, duplicate_result,
                              gap_result, score_data) -> list:
    recs   = []
    scores = score_data["scores"]

    for f in orphan_result.get("findings", [])[:2]:
        pct = f["pct_of_source"]
        sev = "critical" if pct > 25 else ("high" if pct > 8 else "medium")
        src = f["direction"].split("→")[0].strip()
        tgt = f["direction"].split("→")[1].strip() if "→" in f["direction"] else "target"
        recs.append({
            "severity": sev, "icon": "🔗",
            "title": f"Fix referential integrity — {f['direction']}",
            "teaser_metric": f"{f['orphan_count']:,} records ({pct}%) have no match",
            "teaser_impact": "These records are invisible in reports and revenue calculations",
            "full_root_cause": (
                f"Records keyed on '{f['key']}' exist in '{src}' but not in '{tgt}'. "
                "Typical causes: partial ETL loads, out-of-order inserts, or siloed systems."
            ),
            "full_steps": [
                f"SELECT a.* FROM {src} a LEFT JOIN {tgt} b ON a.{f['key']} = b.{f['key']} WHERE b.{f['key']} IS NULL",
                "Classify: cancelled/voided records vs. genuine missing records",
                "For genuine gaps: re-extract from source system or trigger downstream creation",
                f"Add NOT NULL / FK constraint on '{f['key']}' in your pipeline schema",
                "Schedule daily reconciliation: alert if orphan rate > 0.5%",
            ],
            "effort": "Medium — 4–8 hours", "prevention": f"FK constraint on '{f['key']}' + daily reconciliation",
        })

    for f in duplicate_result.get("findings", [])[:2]:
        sev = "critical" if f["duplicate_count"] > 10 else "high"
        recs.append({
            "severity": sev, "icon": "👥",
            "title": f"Resolve entity duplicates — '{f['file']}'",
            "teaser_metric": f"{f['duplicate_count']} duplicate entities ({f['type']})",
            "teaser_impact": "Every metric built on this table is currently incorrect",
            "full_root_cause": (
                f"Type: {f['type']}. Duplicates arise from multiple source systems, "
                "absent unique constraints, or manual entry without deduplication."
            ),
            "full_steps": [
                "SELECT name, COUNT(DISTINCT id) cnt FROM table GROUP BY name HAVING cnt > 1",
                "Designate a 'golden record' per group (most complete + most recent)",
                "UPDATE child tables to point all FKs to the golden record ID",
                "Soft-delete duplicates: set merged_into_id, is_deleted = true",
                "Add UNIQUE constraint on natural key (name + email or name + region)",
                "Implement fuzzy-match check at entry: flag similarity > 85%",
            ],
            "effort": "High — 1–2 days", "prevention": "UNIQUE constraint + fuzzy-match at ingestion",
        })

    for f in gap_result.get("findings", [])[:2]:
        pct = f["pct_of_upstream"]
        sev = "critical" if pct > 20 else ("high" if pct > 5 else "medium")
        recs.append({
            "severity": sev, "icon": "⚡",
            "title": f"Close process gap — {f['stage_from']} → {f['stage_to']}",
            "teaser_metric": f"{f['missing_count']:,} records ({pct}%) stall between stages",
            "teaser_impact": "Broken workflow — SLA violations and audit trail gaps",
            "full_root_cause": (
                f"Records reach '{f['stage_from']}' but never appear in '{f['stage_to']}'. "
                "Caused by failed automation, silent rejections, or manual handoffs."
            ),
            "full_steps": [
                f"SELECT * FROM {f['stage_from']} WHERE id NOT IN (SELECT id FROM {f['stage_to']})",
                "Filter: are these cancelled/failed, or genuinely missing?",
                "Inspect system logs at the handoff point for silent errors",
                "For stuck records: replay automation or manually advance",
                f"Alert: if record in {f['stage_from']} > 48h without moving → trigger notification",
            ],
            "effort": "Medium — 2–6 hours",
            "prevention": f"SLA monitoring {f['stage_from']} → {f['stage_to']}, target < 1% gap",
        })

    if (scores.get("completeness") or 100) < 85:
        recs.append({
            "severity": "medium", "icon": "📋",
            "title": "Improve data completeness",
            "teaser_metric": f"Completeness: {scores['completeness']}% — null values detected",
            "teaser_impact": "Null values cause silent errors in aggregations and KPIs",
            "full_root_cause": "Nulls from optional fields, failed ETL steps, or schema mismatches.",
            "full_steps": [
                "SELECT col, COUNT(*)-COUNT(col) nulls, ROUND(100.*(COUNT(*)-COUNT(col))/COUNT(*),1) pct FROM table",
                "Classify: valid optional nulls vs. missing required values",
                "Add NOT NULL constraint on required fields and backfill from source",
                "Document expected null rate per column as a quality baseline",
                "Alert when null rate exceeds baseline by +2% in any pipeline run",
            ],
            "effort": "Low–Medium", "prevention": "Schema validation + null-rate monitoring at ingestion",
        })

    order = {"critical": 0, "high": 1, "medium": 2}
    recs.sort(key=lambda r: order.get(r["severity"], 9))
    return recs
