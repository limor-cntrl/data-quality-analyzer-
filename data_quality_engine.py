"""
Data Quality Engine - Integration-Based Checks
================================================
Reads 1-5 related CSV files and runs 3 critical cross-file quality checks:
  1. Orphan Records     - records with no match across files (referential integrity)
  2. Entity Duplicates  - same entity appearing with different IDs or slight variations
  3. Process Flow Gaps  - records missing from expected next steps in a workflow
"""

import pandas as pd
import numpy as np
import re
import os
import sys
from itertools import combinations
from difflib import SequenceMatcher


# ─────────────────────────────────────────────
# STEP 1: Load files
# ─────────────────────────────────────────────

def load_files(file_paths: list[str]) -> dict[str, pd.DataFrame]:
    """Load CSV files. Returns {filename: dataframe}."""
    dfs = {}
    for path in file_paths:
        name = os.path.basename(path).replace(".csv", "")
        try:
            df = pd.read_csv(path)
            df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
            dfs[name] = df
            print(f"  [OK] {name}: {len(df):,} rows × {len(df.columns)} columns")
        except Exception as e:
            print(f"  [ERR] {path}: {e}")
    return dfs


# ─────────────────────────────────────────────
# STEP 2: Auto-detect join keys
# ─────────────────────────────────────────────

ID_PATTERNS = re.compile(r"(id|_id|key|code|num|number|ref)$", re.IGNORECASE)


def detect_join_keys(dfs: dict[str, pd.DataFrame]) -> list[dict]:
    """
    Find columns that appear in multiple files and look like identifiers.
    Returns a list of potential join pairs:
      [{"key": col_name, "files": [file_a, file_b]}, ...]
    """
    # Map: column_name -> list of files that have it
    col_to_files: dict[str, list[str]] = {}
    for name, df in dfs.items():
        for col in df.columns:
            col_to_files.setdefault(col, []).append(name)

    joins = []
    for col, files in col_to_files.items():
        if len(files) < 2:
            continue
        # Prefer columns that look like IDs, or have high cardinality
        looks_like_id = bool(ID_PATTERNS.search(col))
        # Check cardinality across at least one file
        high_cardinality = any(
            dfs[f][col].nunique() > 0.3 * len(dfs[f]) for f in files
        )
        if looks_like_id or high_cardinality:
            for i, j in combinations(files, 2):
                joins.append({"key": col, "file_a": i, "file_b": j})

    # Also try fuzzy column name matching (e.g. "customer_id" ↔ "cust_id")
    all_cols = {name: list(df.columns) for name, df in dfs.items()}
    for (fa, cols_a), (fb, cols_b) in combinations(all_cols.items(), 2):
        for ca in cols_a:
            for cb in cols_b:
                if ca == cb:
                    continue  # already caught above
                ratio = SequenceMatcher(None, ca, cb).ratio()
                if ratio > 0.82 and (ID_PATTERNS.search(ca) or ID_PATTERNS.search(cb)):
                    joins.append({"key": f"{ca} ↔ {cb}", "file_a": fa, "file_b": fb,
                                  "col_a": ca, "col_b": cb})

    return joins


# ─────────────────────────────────────────────
# CHECK 1: Orphan Records
# ─────────────────────────────────────────────

def check_orphan_records(dfs: dict, joins: list[dict]) -> dict:
    """
    For each join pair, find records in file_a whose key value
    does NOT appear in file_b (and vice versa).

    Business impact: unmatched invoices, revenue with no customer,
                     orders with no shipment record, etc.
    """
    findings = []

    for join in joins:
        fa, fb = join["file_a"], join["file_b"]
        col_a = join.get("col_a", join["key"])
        col_b = join.get("col_b", join["key"])

        if col_a not in dfs[fa].columns or col_b not in dfs[fb].columns:
            continue

        vals_a = set(dfs[fa][col_a].dropna().astype(str))
        vals_b = set(dfs[fb][col_b].dropna().astype(str))

        orphans_in_a = vals_a - vals_b   # in A but not in B
        orphans_in_b = vals_b - vals_a   # in B but not in A

        for direction, orphans, source, target in [
            (f"{fa} → {fb}", orphans_in_a, fa, fb),
            (f"{fb} → {fa}", orphans_in_b, fb, fa),
        ]:
            if not orphans:
                continue

            count = len(orphans)
            pct = count / len(dfs[source]) * 100
            examples = sorted(list(orphans))[:5]

            # Pull sample rows for context
            col_src = col_a if source == fa else col_b
            sample_rows = dfs[source][dfs[source][col_src].astype(str).isin(examples)].head(3)

            findings.append({
                "direction": direction,
                "key": join["key"],
                "orphan_count": count,
                "pct_of_source": round(pct, 1),
                "example_values": examples,
                "sample_rows": sample_rows,
            })

    # Sort by impact (highest % first)
    findings.sort(key=lambda x: x["pct_of_source"], reverse=True)
    return {"check": "Orphan Records", "findings": findings}


# ─────────────────────────────────────────────
# CHECK 2: Entity Duplicates
# ─────────────────────────────────────────────

NAME_PATTERNS = re.compile(r"(name|customer|client|company|vendor|supplier|product)", re.IGNORECASE)


def check_entity_duplicates(dfs: dict, joins: list[dict]) -> dict:
    """
    Detect entities that appear multiple times with different IDs
    but similar names — classic sign of duplicate records that inflate counts.

    Strategy:
      a) Within each file: same name-like column, different ID column
      b) Across files: same entity appears with slightly different spelling

    Business impact: duplicate customer billing, inflated product catalog,
                     double-counted revenue.
    """
    findings = []

    for name, df in dfs.items():
        id_cols = [c for c in df.columns if ID_PATTERNS.search(c)]
        # Name columns must NOT be ID columns (avoid customer_id matching both patterns)
        name_cols = [c for c in df.columns
                     if NAME_PATTERNS.search(c) and not ID_PATTERNS.search(c)]

        if not id_cols or not name_cols:
            continue

        id_col = id_cols[0]
        name_col = name_cols[0]

        # --- 2a: exact name duplication with different IDs ---
        dup_names = (
            df.groupby(name_col)[id_col]
            .nunique()
            .reset_index()
            .rename(columns={id_col: "unique_ids"})
        )
        dup_names = dup_names[dup_names["unique_ids"] > 1].sort_values("unique_ids", ascending=False)

        if not dup_names.empty:
            top = dup_names.head(5)
            example_detail = []
            for _, row in top.iterrows():
                ids = df[df[name_col] == row[name_col]][id_col].unique().tolist()
                example_detail.append({
                    "name": row[name_col],
                    "appears_with_ids": ids[:6],
                    "id_count": int(row["unique_ids"]),
                })
            findings.append({
                "file": name,
                "type": "Same name, multiple IDs",
                "duplicate_count": int(len(dup_names)),
                "examples": example_detail,
            })

        # --- 2b: fuzzy name duplicates (typos / spacing / abbreviations) ---
        unique_names = df[name_col].dropna().astype(str).unique()
        if len(unique_names) > 5000:
            unique_names = unique_names[:5000]  # cap for performance

        fuzzy_pairs = []
        names_list = sorted(unique_names)
        for i in range(len(names_list)):
            for j in range(i + 1, min(i + 30, len(names_list))):  # sliding window
                a, b = names_list[i], names_list[j]
                if abs(len(a) - len(b)) > 8:
                    continue
                ratio = SequenceMatcher(None, a.lower(), b.lower()).ratio()
                if 0.85 < ratio < 1.0:
                    fuzzy_pairs.append((a, b, round(ratio, 2)))

        if fuzzy_pairs:
            findings.append({
                "file": name,
                "type": "Fuzzy name duplicates (likely same entity)",
                "duplicate_count": len(fuzzy_pairs),
                "examples": [
                    {"value_a": p[0], "value_b": p[1], "similarity": p[2]}
                    for p in fuzzy_pairs[:5]
                ],
            })

    return {"check": "Entity Duplicates", "findings": findings}


# ─────────────────────────────────────────────
# CHECK 3: Process Flow Gaps
# ─────────────────────────────────────────────

def check_process_gaps(dfs: dict, joins: list[dict]) -> dict:
    """
    Detect records that exist in an early-stage file but are absent
    from a later-stage file — indicating broken process flow.

    Heuristic: if files can be ordered by name (order→invoice→payment)
    or by row-count descending, check that IDs flow forward.

    Business impact: invoices without payment, orders never fulfilled,
                     leads never converted, entries missing from reports.
    """
    findings = []

    if len(dfs) < 2:
        return {"check": "Process Flow Gaps", "findings": []}

    # Order files by decreasing row count (upstream usually has more rows)
    ordered = sorted(dfs.keys(), key=lambda k: len(dfs[k]), reverse=True)

    STAGE_KEYWORDS = ["order", "lead", "request", "invoice", "claim",
                      "payment", "shipment", "delivery", "report", "event"]

    def stage_rank(fname):
        for i, kw in enumerate(STAGE_KEYWORDS):
            if kw in fname.lower():
                return i
        return 999

    ranked = sorted(dfs.keys(), key=stage_rank)
    pipeline = ranked if ranked[0] != ranked[-1] else ordered

    # Walk consecutive stages in the detected pipeline order
    for i in range(len(pipeline) - 1):
        fa, fb = pipeline[i], pipeline[i + 1]

        # Find shared key between these two specific files
        shared = [j for j in joins if {j["file_a"], j["file_b"]} == {fa, fb}]
        if not shared:
            shared = [j for j in joins if fa in (j["file_a"], j["file_b"])
                      and fb in (j["file_a"], j["file_b"])]
        if not shared:
            continue

        join = shared[0]
        col_a = join.get("col_a", join["key"])
        col_b = join.get("col_b", join["key"])

        if col_a not in dfs[fa].columns or col_b not in dfs[fb].columns:
            continue

        ids_upstream = set(dfs[fa][col_a].dropna().astype(str))
        ids_downstream = set(dfs[fb][col_b].dropna().astype(str))

        missing_downstream = ids_upstream - ids_downstream
        if not missing_downstream:
            continue

        count = len(missing_downstream)
        pct = count / len(ids_upstream) * 100
        examples = sorted(list(missing_downstream))[:5]

        sample_rows = dfs[fa][dfs[fa][col_a].astype(str).isin(examples)].head(3)

        findings.append({
            "stage_from": fa,
            "stage_to": fb,
            "key": join["key"],
            "missing_count": count,
            "pct_of_upstream": round(pct, 1),
            "example_ids": examples,
            "sample_rows": sample_rows,
        })

    findings.sort(key=lambda x: x["pct_of_upstream"], reverse=True)
    return {"check": "Process Flow Gaps", "findings": findings}


# ─────────────────────────────────────────────
# REPORT PRINTER
# ─────────────────────────────────────────────

SEP = "=" * 65
SEP2 = "-" * 65


def print_report(orphans: dict, duplicates: dict, gaps: dict):
    print(f"\n{SEP}")
    print("  DATA QUALITY REPORT — 3 CRITICAL INTEGRATION CHECKS")
    print(SEP)

    # ── CHECK 1 ──
    print(f"\n{'CHECK 1 ▶ ORPHAN RECORDS':}")
    print("  Records that have no matching counterpart in a related file.")
    print("  Business risk: unassigned revenue, unlinked transactions,")
    print("  missing entries in consolidated reports.")
    print(SEP2)
    findings = orphans["findings"]
    if not findings:
        print("  No orphan records detected across detected join keys.\n")
    else:
        for f in findings[:3]:
            print(f"\n  {f['direction']}  |  key: {f['key']}")
            print(f"  Orphans: {f['orphan_count']:,}  ({f['pct_of_source']}% of source file)")
            print(f"  Example values: {f['example_values']}")
            if not f["sample_rows"].empty:
                print("  Sample rows from source:")
                print(f["sample_rows"].to_string(index=False, max_cols=6))

    # ── CHECK 2 ──
    print(f"\n{'CHECK 2 ▶ ENTITY DUPLICATES':}")
    print("  Same real-world entity registered under multiple IDs or")
    print("  with slightly different spellings.")
    print("  Business risk: duplicate billing, inflated customer count,")
    print("  double-counted revenue.")
    print(SEP2)
    findings = duplicates["findings"]
    if not findings:
        print("  No entity duplicates detected.\n")
    else:
        for f in findings[:4]:
            print(f"\n  File: {f['file']}  |  Type: {f['type']}")
            print(f"  Affected entities: {f['duplicate_count']:,}")
            for ex in f["examples"][:3]:
                if "similarity" in ex:
                    print(f"    '{ex['value_a']}' ≈ '{ex['value_b']}'  (similarity {ex['similarity']})")
                else:
                    print(f"    '{ex['name']}' → IDs: {ex['appears_with_ids']}  ({ex['id_count']} IDs)")

    # ── CHECK 3 ──
    print(f"\n{'CHECK 3 ▶ PROCESS FLOW GAPS':}")
    print("  Records present in an upstream stage but absent from the")
    print("  expected downstream stage.")
    print("  Business risk: orders never fulfilled, invoices not paid,")
    print("  leads lost mid-funnel, gaps in audit trail.")
    print(SEP2)
    findings = gaps["findings"]
    if not findings:
        print("  No process flow gaps detected (or insufficient file structure).\n")
    else:
        for f in findings[:3]:
            print(f"\n  {f['stage_from']}  →→  {f['stage_to']}  |  key: {f['key']}")
            print(f"  Missing downstream: {f['missing_count']:,}  ({f['pct_of_upstream']}% of upstream)")
            print(f"  Example IDs not found in {f['stage_to']}: {f['example_ids']}")
            if not f["sample_rows"].empty:
                print("  Sample rows from upstream that have no continuation:")
                print(f["sample_rows"].to_string(index=False, max_cols=6))

    print(f"\n{SEP}\n")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def run(file_paths: list[str]):
    print(f"\nLoading {len(file_paths)} file(s)...")
    dfs = load_files(file_paths)

    if len(dfs) == 0:
        print("No files loaded. Exiting.")
        return

    print("\nDetecting join keys...")
    joins = detect_join_keys(dfs)
    if joins:
        for j in joins:
            print(f"  Found key '{j['key']}' between {j['file_a']} and {j['file_b']}")
    else:
        print("  No shared keys detected — some cross-file checks may be limited.")

    print("\nRunning checks...")
    orphans    = check_orphan_records(dfs, joins)
    duplicates = check_entity_duplicates(dfs, joins)
    gaps       = check_process_gaps(dfs, joins)

    print_report(orphans, duplicates, gaps)

    return {
        "join_keys": joins,
        "orphan_records": orphans,
        "entity_duplicates": duplicates,
        "process_gaps": gaps,
    }


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python data_quality_engine.py file1.csv file2.csv ...")
        sys.exit(1)
    run(sys.argv[1:])
