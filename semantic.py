"""
Semantic Intelligence Layer
============================
Understands WHAT the data represents, not just whether it's valid.
- Detects column types (monetary, temporal, ID, status, personal...)
- Identifies entity types (customer, order, invoice, product...)
- Infers business domain (e-commerce, CRM, finance, HR...)
- Calculates business impact in $ terms
- Generates plain-English diagnostic narrative
"""

import pandas as pd
import numpy as np
import re

# ─────────────────────────────────────────────────────────────────────────────
# Column semantic type detection
# ─────────────────────────────────────────────────────────────────────────────

COL_SEMANTIC = {
    "id":        r"(^id$|_id$|_key$|_code$|_ref$|_no$|_num$|_number$|reference)",
    "monetary":  r"(amount|price|cost|revenue|salary|fee|total|value|budget|income|spend|charge|payment)",
    "temporal":  r"(date|time|created|updated|modified|timestamp|_at$|_on$)",
    "status":    r"(status|state|stage|flag|type|category|kind|mode|phase)",
    "personal":  r"(name|email|phone|mobile|address|contact|first_name|last_name|full_name)",
    "geographic":r"(city|country|region|state|zip|postal|province|lat|lon|location|territory)",
    "quantity":  r"(qty|quantity|count|stock|inventory|units|volume)",
}

def classify_columns(df: pd.DataFrame) -> dict:
    """Return {col: semantic_type} for each column."""
    result = {}
    for col in df.columns:
        matched = "other"
        for sem_type, pattern in COL_SEMANTIC.items():
            if re.search(pattern, col, re.IGNORECASE):
                matched = sem_type
                break
        result[col] = matched
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Entity type detection
# ─────────────────────────────────────────────────────────────────────────────

ENTITY_SIGNALS = {
    "customer":    ["customer", "client", "user", "contact", "account", "person", "buyer"],
    "order":       ["order", "purchase", "sale", "cart", "booking", "reservation"],
    "invoice":     ["invoice", "bill", "receipt", "statement"],
    "payment":     ["payment", "charge", "refund", "transaction", "transfer"],
    "product":     ["product", "item", "sku", "catalog", "listing", "merchandise"],
    "employee":    ["employee", "staff", "worker", "personnel", "hr", "payroll"],
    "lead":        ["lead", "prospect", "opportunity", "pipeline"],
    "campaign":    ["campaign", "marketing", "promo", "ad", "impression"],
    "event":       ["event", "log", "activity", "action", "audit", "track"],
    "shipment":    ["shipment", "delivery", "shipping", "dispatch", "fulfillment"],
}

def detect_entity(name: str, df: pd.DataFrame) -> str:
    """Detect what real-world entity a dataframe represents."""
    name_lower  = name.lower()
    cols_lower  = " ".join(df.columns).lower()

    scores = {}
    for entity, signals in ENTITY_SIGNALS.items():
        score = 0
        for s in signals:
            # File name match weights 3× more than column matches
            if s in name_lower:
                score += 3
            score += cols_lower.count(s)   # count occurrences in column names
        scores[entity] = score

    best = max(scores, key=scores.get)
    return best if scores[best] > 0 else "dataset"


# ─────────────────────────────────────────────────────────────────────────────
# Business domain detection
# ─────────────────────────────────────────────────────────────────────────────

DOMAIN_SIGNALS = {
    "E-commerce":  ["order", "product", "cart", "shipment", "customer", "invoice", "sku"],
    "CRM":         ["lead", "opportunity", "deal", "contact", "account", "pipeline", "prospect"],
    "Finance":     ["invoice", "payment", "ledger", "account", "transaction", "budget", "revenue"],
    "HR":          ["employee", "department", "payroll", "salary", "hire", "staff", "leave"],
    "Marketing":   ["campaign", "lead", "conversion", "impression", "click", "ad", "promo"],
    "Operations":  ["shipment", "delivery", "inventory", "warehouse", "dispatch", "supplier"],
}

def detect_domain(dfs: dict) -> tuple:
    """Return (domain_name, confidence_0_to_1)."""
    all_text = " ".join(
        name + " " + " ".join(df.columns)
        for name, df in dfs.items()
    ).lower()

    scores = {}
    for domain, signals in DOMAIN_SIGNALS.items():
        scores[domain] = sum(1 for s in signals if s in all_text)

    total = sum(scores.values())
    best  = max(scores, key=scores.get)
    conf  = scores[best] / total if total > 0 else 0
    return (best, round(conf, 2)) if scores[best] > 0 else ("General Business", 0.3)


# ─────────────────────────────────────────────────────────────────────────────
# Business impact calculation
# ─────────────────────────────────────────────────────────────────────────────

def estimate_monetary_impact(dfs: dict, orphan_result: dict,
                              duplicate_result: dict, gap_result: dict,
                              monetary_override=None) -> dict:
    """Estimate $ impact of data quality issues."""
    # Find average transaction value across all files
    avg_values = []

    if monetary_override:
        fname, col = monetary_override
        if fname in dfs:
            col_norm = col.strip().lower().replace(" ", "_")
            if col_norm in dfs[fname].columns:
                numeric = pd.to_numeric(dfs[fname][col_norm], errors="coerce").dropna()
                if len(numeric) > 0 and numeric.mean() > 0:
                    avg_values = [numeric.mean()]

    if not avg_values:
        for df in dfs.values():
            for col in df.columns:
                if re.search(COL_SEMANTIC["monetary"], col, re.IGNORECASE):
                    numeric = pd.to_numeric(df[col], errors="coerce").dropna()
                    if len(numeric) > 0 and numeric.mean() > 0:
                        avg_values.append(numeric.mean())

    avg_val = sum(avg_values) / len(avg_values) if avg_values else None
    has_monetary = avg_val is not None

    impact = {"has_monetary": has_monetary, "avg_value": avg_val, "items": []}

    if not has_monetary:
        # Use record count as proxy
        for f in orphan_result.get("findings", [])[:2]:
            impact["items"].append({
                "label": f"Orphan records — {f['direction']}",
                "count": f["orphan_count"],
                "value": None,
                "risk": f"{f['orphan_count']:,} records unaccounted for",
            })
        return impact

    # Orphans → revenue leakage
    for f in orphan_result.get("findings", [])[:2]:
        val = f["orphan_count"] * avg_val
        impact["items"].append({
            "label": f"Unlinked records — {f['direction']}",
            "count": f["orphan_count"],
            "value": val,
            "risk": "Revenue leakage / reporting gap",
        })

    # Duplicates → inflated pipeline / double billing risk
    for f in duplicate_result.get("findings", [])[:1]:
        val = f["duplicate_count"] * avg_val * 0.4  # 40% risk factor
        impact["items"].append({
            "label": f"Duplicate entities — '{f['file']}'",
            "count": f["duplicate_count"],
            "value": val,
            "risk": "Double billing / inflated pipeline risk",
        })

    # Process gaps → stalled value
    for f in gap_result.get("findings", [])[:1]:
        val = f["missing_count"] * avg_val
        impact["items"].append({
            "label": f"Process gap — {f['stage_from']} → {f['stage_to']}",
            "count": f["missing_count"],
            "value": val,
            "risk": "Stalled workflow / SLA breach",
        })

    impact["total"] = sum(i["value"] for i in impact["items"] if i.get("value"))
    return impact


# ─────────────────────────────────────────────────────────────────────────────
# Narrative generation
# ─────────────────────────────────────────────────────────────────────────────

DOMAIN_PROCESS = {
    "E-commerce":  "order → fulfillment → invoice → payment",
    "CRM":         "lead → opportunity → deal → closed-won",
    "Finance":     "transaction → reconciliation → ledger → report",
    "HR":          "hire → onboard → payroll → review",
    "Marketing":   "impression → click → lead → conversion",
    "Operations":  "request → dispatch → delivery → confirmation",
    "General Business": "upstream → processing → downstream",
}

def generate_narrative(dfs: dict, domain: str, entities: dict,
                       orphan_result: dict, duplicate_result: dict,
                       gap_result: dict, score: float) -> list:
    """Generate 3-5 plain-English insights about the data."""
    insights = []

    # Domain context
    process = DOMAIN_PROCESS.get(domain, "upstream → downstream")
    file_names = list(dfs.keys())
    entity_list = ", ".join(f"**{e}** ({n})" for n, e in entities.items())

    insights.append({
        "icon": "🧠",
        "title": f"Detected domain: {domain}",
        "text": (
            f"We identified {len(dfs)} related dataset{'s' if len(dfs)>1 else ''}: {entity_list}. "
            f"This maps to a typical {domain} pipeline: *{process}*."
        ),
    })

    # Orphan pattern interpretation
    orphan_findings = orphan_result.get("findings", [])
    if orphan_findings:
        worst = max(orphan_findings, key=lambda f: f["pct_of_source"])
        pct   = worst["pct_of_source"]
        cause = (
            "a manual process that missed a batch" if pct > 40
            else "an ETL timing issue or out-of-order load"
            if pct > 15
            else "occasional sync failures between systems"
        )
        insights.append({
            "icon": "🔗",
            "title": "Referential integrity is broken",
            "text": (
                f"{pct}% of records in **{worst['direction']}** have no matching counterpart. "
                f"This pattern is consistent with {cause}. "
                f"These records are invisible in any joined report or aggregation."
            ),
        })

    # Duplicate pattern interpretation
    dup_findings = duplicate_result.get("findings", [])
    if dup_findings:
        f = dup_findings[0]
        cause = (
            "a post-acquisition CRM merge" if "customer" in f["file"].lower()
            else "multiple source systems feeding the same table"
        )
        insights.append({
            "icon": "👥",
            "title": "Entity identity problem detected",
            "text": (
                f"**'{f['file']}'** contains {f['duplicate_count']} entities "
                f"that appear under multiple identities ({f['type']}). "
                f"This is a classic symptom of {cause}. "
                f"Every metric built on this table is currently incorrect."
            ),
        })

    # Process gap interpretation
    gap_findings = gap_result.get("findings", [])
    if gap_findings:
        f = gap_findings[0]
        pct = f["pct_of_upstream"]
        cause = (
            "a failed automation step" if pct > 30
            else "a manual handoff that gets skipped under load"
            if pct > 10
            else "edge cases not handled by the pipeline"
        )
        insights.append({
            "icon": "⚡",
            "title": "Process pipeline has a leak",
            "text": (
                f"{pct}% of records start in **{f['stage_from']}** "
                f"but never reach **{f['stage_to']}**. "
                f"This indicates {cause}. "
                f"Without monitoring, this gap grows silently over time."
            ),
        })

    # Score context
    if score < 55:
        insights.append({
            "icon": "💀",
            "title": "This data should not be used for decision-making",
            "text": (
                f"With a score of {score}, the risk of incorrect conclusions from this data is high. "
                f"Analyses built on it may reach wrong results up to 30-40% of the time. "
                f"Remediation should happen before the next reporting cycle."
            ),
        })
    elif score < 70:
        insights.append({
            "icon": "⚠️",
            "title": "Some reports built on this data are unreliable",
            "text": (
                f"A score of {score} means your data has material issues that affect specific analyses. "
                f"Revenue reports, customer counts, and pipeline metrics are likely affected. "
                f"Use findings below to identify which reports to trust."
            ),
        })

    return insights
