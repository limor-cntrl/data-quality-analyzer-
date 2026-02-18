"""
Scoring Module — Industry-standard data quality dimensions
===========================================================
Based on DAMA / DCAM / ISO 8000 framework:
  1. Completeness  — non-null rate
  2. Uniqueness    — duplicate-free rate + entity-duplicate penalty
  3. Validity      — format, range, type conformance
  4. Consistency   — cross-file referential integrity  (multi-file only)
  5. Timeliness    — data freshness via date columns
"""

import pandas as pd
import numpy as np


# ─────────────────────────────────────────────────────────────────────────────
# Main scoring function
# ─────────────────────────────────────────────────────────────────────────────

def calculate_scores(dfs: dict, orphan_result: dict, duplicate_result: dict,
                     gap_result: dict, num_files: int) -> dict:
    scores = {}
    details = {}

    # ── 1. COMPLETENESS ──────────────────────────────────────────────────────
    total_cells   = sum(df.size for df in dfs.values())
    non_null_cells = sum(df.notna().sum().sum() for df in dfs.values())
    scores["completeness"] = round(non_null_cells / total_cells * 100, 1) if total_cells else 100

    per_file = {}
    for name, df in dfs.items():
        per_file[name] = round(df.notna().sum().sum() / df.size * 100, 1) if df.size else 100
    details["completeness_per_file"] = per_file

    # ── 2. UNIQUENESS ────────────────────────────────────────────────────────
    total_rows  = sum(len(df) for df in dfs.values())
    unique_rows = sum(len(df.drop_duplicates()) for df in dfs.values())
    base_uniq   = round(unique_rows / total_rows * 100, 1) if total_rows else 100

    dup_count = sum(f.get("duplicate_count", 0) for f in duplicate_result.get("findings", []))
    penalty   = min(25, dup_count * 3)
    scores["uniqueness"] = max(0, round(base_uniq - penalty, 1))
    details["uniqueness_base"]    = base_uniq
    details["uniqueness_penalty"] = penalty

    # ── 3. VALIDITY ──────────────────────────────────────────────────────────
    col_scores = []
    validity_issues = []

    for fname, df in dfs.items():
        for col in df.columns:
            non_null = df[col].dropna()
            if len(non_null) == 0:
                col_scores.append(0)
                validity_issues.append(f"{fname}.{col}: entirely null")
                continue

            # Single-value column (suspicious)
            if non_null.nunique() == 1 and len(df) > 5:
                col_scores.append(55)
                validity_issues.append(f"{fname}.{col}: only one unique value")
                continue

            if pd.api.types.is_numeric_dtype(df[col]):
                money_col = any(k in col for k in ["amount", "price", "cost", "revenue", "salary", "fee"])
                if money_col:
                    neg = (df[col] < 0).sum()
                    col_scores.append(max(40, 100 - neg / len(df) * 200))
                    if neg:
                        validity_issues.append(f"{fname}.{col}: {neg} negative values")
                elif len(non_null) > 10:
                    q25, q75 = non_null.quantile(0.25), non_null.quantile(0.75)
                    iqr = q75 - q25
                    if iqr > 0:
                        outliers = ((non_null < q25 - 3 * iqr) | (non_null > q75 + 3 * iqr)).sum()
                        col_scores.append(max(60, 100 - outliers / len(non_null) * 100))
                    else:
                        col_scores.append(88)
                else:
                    col_scores.append(90)

            elif any(k in col for k in ["date", "time", "created", "updated", "timestamp"]):
                try:
                    parsed     = pd.to_datetime(df[col], errors="coerce")
                    fail_rate  = parsed.isna().sum() / len(df)
                    future_cnt = (parsed > pd.Timestamp.now()).sum()
                    score      = 100 - fail_rate * 50 - (future_cnt / len(df)) * 25
                    col_scores.append(max(40, round(score, 1)))
                    if future_cnt:
                        validity_issues.append(f"{fname}.{col}: {future_cnt} future dates")
                except Exception:
                    col_scores.append(72)
            else:
                col_scores.append(92)

    scores["validity"] = round(sum(col_scores) / len(col_scores), 1) if col_scores else 90
    details["validity_issues"] = validity_issues[:8]

    # ── 4. CONSISTENCY (cross-file only) ─────────────────────────────────────
    if num_files < 2:
        scores["consistency"] = None
        details["consistency_note"] = "Single file — cross-file check not applicable"
    else:
        issues = (
            [f["pct_of_source"]   for f in orphan_result.get("findings", [])] +
            [f["pct_of_upstream"] for f in gap_result.get("findings", [])]
        )
        if issues:
            worst = max(issues)
            avg   = sum(issues) / len(issues)
            scores["consistency"] = max(0, round(100 - worst * 0.55 - avg * 0.45, 1))
        else:
            scores["consistency"] = 100
        details["consistency_issues_count"] = len(issues)

    # ── 5. TIMELINESS ────────────────────────────────────────────────────────
    date_series = []
    for df in dfs.values():
        for col in df.columns:
            if any(k in col for k in ["date", "time", "created", "updated"]):
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

        if   days_old <  7:  t = 100
        elif days_old < 30:  t = 90
        elif days_old < 90:  t = 75
        elif days_old < 365: t = 55
        else:                t = 30

        scores["timeliness"] = max(0, round(t - future * 1.5, 1))
        details["latest_date"] = str(latest.date())
        details["days_old"]    = days_old
        details["future_dates"] = future
    else:
        scores["timeliness"] = 70
        details["latest_date"] = None

    # ── WEIGHTED OVERALL ─────────────────────────────────────────────────────
    if num_files >= 2:
        weights = {
            "completeness": 0.20, "uniqueness": 0.20, "validity": 0.15,
            "consistency":  0.30, "timeliness": 0.15,
        }
    else:
        weights = {
            "completeness": 0.32, "uniqueness": 0.28, "validity": 0.25,
            "consistency":  0.00, "timeliness": 0.15,
        }

    w_sum, w_total = 0.0, 0.0
    for dim, w in weights.items():
        if w > 0 and scores.get(dim) is not None:
            w_sum   += scores[dim] * w
            w_total += w

    scores["overall"] = round(w_sum / w_total, 1) if w_total else 0

    return {"scores": scores, "details": details, "weights": weights}


# ─────────────────────────────────────────────────────────────────────────────
# Label helpers
# ─────────────────────────────────────────────────────────────────────────────

def score_label(s) -> tuple:
    """Return (label_text, hex_color) for a numeric score."""
    if s is None:  return "N/A",       "#9CA3AF"
    if s >= 90:    return "Excellent",  "#10B981"
    if s >= 80:    return "Good",       "#34D399"
    if s >= 70:    return "Fair",       "#F59E0B"
    if s >= 60:    return "Poor",       "#F97316"
    return             "Critical",  "#EF4444"


def overall_grade(s) -> tuple:
    """Return (letter_grade, hex_color)."""
    if s >= 90:  return "A", "#10B981"
    if s >= 80:  return "B", "#34D399"
    if s >= 70:  return "C", "#F59E0B"
    if s >= 60:  return "D", "#F97316"
    return           "F", "#EF4444"


# ─────────────────────────────────────────────────────────────────────────────
# Recommendations generator
# ─────────────────────────────────────────────────────────────────────────────

def generate_recommendations(orphan_result, duplicate_result,
                              gap_result, score_data) -> list:
    recs = []
    scores = score_data["scores"]

    # ── From orphan records ───────────────────────────────────────────────────
    for f in orphan_result.get("findings", [])[:2]:
        pct = f["pct_of_source"]
        sev = "critical" if pct > 30 else ("high" if pct > 10 else "medium")
        src = f["direction"].split("→")[0].strip()
        tgt = f["direction"].split("→")[1].strip() if "→" in f["direction"] else ""
        recs.append({
            "severity": sev,
            "icon": "🔗",
            "title": f"Fix referential integrity — {f['direction']}",
            "teaser_metric": f"{f['orphan_count']:,} records ({pct}%) have no matching counterpart",
            "teaser_impact": "These records vanish from joined reports and revenue calculations",
            "full_root_cause": (
                f"Records keyed on '{f['key']}' exist in '{src}' but not in '{tgt}'. "
                "Typical causes: partial ETL loads, out-of-order inserts, or siloed source systems."
            ),
            "full_steps": [
                f"Audit orphans: SELECT a.* FROM {src} a LEFT JOIN {tgt} b "
                f"ON a.{f['key']} = b.{f['key']} WHERE b.{f['key']} IS NULL",
                "Classify results: distinguish cancelled/voided records from genuine gaps",
                "For genuine gaps: re-extract from source system or trigger downstream creation",
                f"Add validation rule or FK constraint on '{f['key']}' in your pipeline",
                "Set up daily reconciliation alert if orphan rate exceeds 0.5%",
            ],
            "effort": "Medium — 4–8 hours",
            "prevention": f"Referential constraint on '{f['key']}' + daily reconciliation job",
        })

    # ── From entity duplicates ────────────────────────────────────────────────
    for f in duplicate_result.get("findings", [])[:2]:
        sev = "critical" if f["duplicate_count"] > 10 else "high"
        recs.append({
            "severity": sev,
            "icon": "👥",
            "title": f"Resolve entity duplicates — '{f['file']}'",
            "teaser_metric": f"{f['duplicate_count']} duplicate entities ({f['type']})",
            "teaser_impact": "Inflated counts, double billing risk, broken customer segmentation",
            "full_root_cause": (
                f"Type: {f['type']}. Duplicates arise from multiple source systems, "
                "absent unique constraints, or manual entry without deduplication at intake."
            ),
            "full_steps": [
                "Run: SELECT name, COUNT(DISTINCT id) cnt FROM table GROUP BY name HAVING cnt > 1",
                "For each group: designate a 'golden record' (most complete + most recent)",
                "Remap all child records (orders, invoices) to the golden record ID",
                "Archive duplicates with a 'merged_into_id' column for audit trail",
                "Add UNIQUE constraint on the natural key (e.g. name + email, or name + region)",
                "Implement fuzzy-match check at data entry: flag similarity > 85% before save",
            ],
            "effort": "High — 1–2 days",
            "prevention": "Unique constraint + real-time fuzzy-match check at ingestion layer",
        })

    # ── From process gaps ─────────────────────────────────────────────────────
    for f in gap_result.get("findings", [])[:2]:
        pct = f["pct_of_upstream"]
        sev = "critical" if pct > 20 else ("high" if pct > 5 else "medium")
        recs.append({
            "severity": sev,
            "icon": "⚡",
            "title": f"Close process gap — {f['stage_from']} → {f['stage_to']}",
            "teaser_metric": f"{f['missing_count']:,} records ({pct}%) stall between stages",
            "teaser_impact": "Broken workflows, SLA violations, incomplete audit trails",
            "full_root_cause": (
                f"Records reach '{f['stage_from']}' but never appear in '{f['stage_to']}'. "
                "Caused by failed automation, silent rejections, or manual handoffs that get skipped."
            ),
            "full_steps": [
                f"List stuck IDs: SELECT * FROM {f['stage_from']} WHERE id NOT IN (SELECT id FROM {f['stage_to']})",
                "Check their status: cancelled/failed vs. genuinely missing",
                "Inspect system logs at the handoff point for silent errors",
                "For stuck-but-valid records: replay the automation or manually advance them",
                f"Alert rule: if any record stays in {f['stage_from']} > 48h without moving, trigger Slack/email alert",
            ],
            "effort": "Medium — 2–6 hours",
            "prevention": f"SLA monitoring between {f['stage_from']} and {f['stage_to']}, target < 1% gap",
        })

    # ── Completeness-based ───────────────────────────────────────────────────
    if (scores.get("completeness") or 100) < 85:
        recs.append({
            "severity": "medium",
            "icon": "📋",
            "title": "Improve data completeness",
            "teaser_metric": f"Completeness score: {scores['completeness']}% — significant nulls detected",
            "teaser_impact": "Null values cause incorrect aggregations and unreliable KPIs",
            "full_root_cause": "Nulls result from optional fields, failed ETL steps, or schema mismatches between source and target.",
            "full_steps": [
                "Profile nulls: SELECT col, COUNT(*) - COUNT(col) AS nulls, ROUND(100.0*(COUNT(*)-COUNT(col))/COUNT(*),1) pct FROM table",
                "Classify: valid optional nulls vs. missing required values",
                "For required fields: add NOT NULL constraint and backfill from source system",
                "Document expected null rate per column as a quality baseline",
                "Monitor: alert when null rate exceeds baseline by more than +2% in any pipeline run",
            ],
            "effort": "Low–Medium — depends on source system access",
            "prevention": "Schema validation + null-rate baseline monitoring at ingestion",
        })

    # Sort by severity
    order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    recs.sort(key=lambda r: order.get(r["severity"], 9))
    return recs
