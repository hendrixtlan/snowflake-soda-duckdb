"""Ingestion gate: enforce the data contract before anything reaches RAW.

This module executes `contract.yml` and holds no rules of its own. It produces
three artefacts:

  * <input>.clean.parquet       rows that satisfy the contract
  * <input>.quarantine.parquet  rejected rows, each with the reason
  * <input>.validation.json     machine-readable result, consumed by the
                                detection-matrix harness

Exit codes: 0 accepted, 2 batch rejected (schema), 3 batch rejected (too many
bad rows), 4 usage error.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import great_expectations as ge
import numpy as np
import pandas as pd
import yaml

HERE = Path(__file__).resolve().parent
CONTRACT = HERE / "contract.yml"

PANDAS_KIND = {"string": "O", "integer": "iu", "float": "fc", "datetime": "M"}


def _read(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path, parse_dates=["event_ts", "_ingested_at"])
    return pd.read_parquet(path)


def _bad_mask(df: pd.DataFrame, exp: dict) -> pd.Series | None:
    """True where the row violates the expectation. None if not row-scoped."""
    kind = exp["kind"]
    n = len(df)

    def col(name):
        return df[name] if name in df.columns else None

    if kind == "not_null":
        c = col(exp["column"])
        return pd.Series(True, index=df.index) if c is None else c.isna()

    if kind == "unique":
        c = col(exp["column"])
        return pd.Series(True, index=df.index) if c is None else c.duplicated(keep="first")

    if kind == "in_set":
        c = col(exp["column"])
        if c is None:
            return pd.Series(False, index=df.index)
        return c.notna() & ~c.isin(exp["values"])

    if kind == "regex":
        c = col(exp["column"])
        if c is None:
            return pd.Series(False, index=df.index)
        return c.notna() & ~c.astype(str).str.match(exp["pattern"])

    if kind == "between":
        c = col(exp["column"])
        if c is None:
            return pd.Series(True, index=df.index)
        v = pd.to_numeric(c, errors="coerce")
        return v.isna() | (v < exp["min"]) | (v > exp["max"])

    if kind == "between_dates":
        c = col(exp["column"])
        if c is None:
            return pd.Series(True, index=df.index)
        v = pd.to_datetime(c, errors="coerce")
        lo = pd.Timestamp(exp["min"])
        hi = pd.Timestamp(datetime.now(timezone.utc).replace(tzinfo=None)) \
            if exp["max"] == "now" else pd.Timestamp(exp["max"])
        return v.isna() | (v < lo) | (v > hi)

    if kind == "pair_a_le_b":
        a, b = col(exp["column_a"]), col(exp["column_b"])
        if a is None or b is None:
            return pd.Series(True, index=df.index)
        av, bv = pd.to_numeric(a, errors="coerce"), pd.to_numeric(b, errors="coerce")
        return av.isna() | bv.isna() | (av > bv)

    if kind == "conditional_ratio_lt":
        w, num, den = col(exp["when_column"]), col(exp["numerator"]), col(exp["denominator"])
        if w is None or num is None or den is None:
            return pd.Series(False, index=df.index)
        nv, dv = pd.to_numeric(num, errors="coerce"), pd.to_numeric(den, errors="coerce")
        ratio = nv / dv.replace(0, np.nan)
        return (w == exp["when_value"]) & (ratio >= exp["threshold"])

    return None


def _table_check(df: pd.DataFrame, exp: dict, contract: dict) -> tuple[bool, dict]:
    v = ge.from_pandas(df)
    if exp["kind"] == "columns_exist":
        # Every declared column must be present. `required` governs nullability,
        # not presence: a nullable column that vanishes is still a broken schema.
        missing = [c["name"] for c in contract["columns"]
                   if not v.expect_column_to_exist(c["name"])["success"]]
        return not missing, {"missing_columns": missing}

    if exp["kind"] == "column_types":
        wrong = []
        for c in contract["columns"]:
            if c["name"] not in df.columns:
                continue
            want = PANDAS_KIND.get(c["dtype"])
            if want and df[c["name"]].dtype.kind not in want:
                wrong.append({"column": c["name"], "expected": c["dtype"],
                              "found": str(df[c["name"]].dtype)})
        return not wrong, {"type_mismatches": wrong}

    return True, {}


def validate(path: Path, contract_path: Path = CONTRACT) -> dict:
    contract = yaml.safe_load(contract_path.read_text())
    df = _read(path)
    results, reasons = [], {}
    quarantine = pd.Series(False, index=df.index)

    for exp in contract["expectations"]:
        if exp["scope"] == "table":
            ok, detail = _table_check(df, exp, contract)
            results.append({"id": exp["id"], "scope": "table", "severity": exp["severity"],
                            "success": ok, "detail": detail})
            continue

        bad = _bad_mask(df, exp)
        if bad is None:
            continue
        count = int(bad.sum())
        results.append({"id": exp["id"], "scope": "row", "severity": exp["severity"],
                        "success": count == 0, "unexpected_count": count})
        if count and exp["severity"] in ("critical", "high"):
            quarantine |= bad
            for i in df.index[bad]:
                reasons.setdefault(i, []).append(exp["id"])

    table_failed = [r["id"] for r in results
                    if r["scope"] == "table" and not r["success"] and r["severity"] == "critical"]
    q_count = int(quarantine.sum())
    q_ratio = q_count / len(df) if len(df) else 0.0
    over_budget = q_ratio > contract["max_quarantine_ratio"]

    if table_failed:
        verdict, code = "rejected_schema", 2
    elif over_budget:
        verdict, code = "rejected_volume", 3
    else:
        verdict, code = "accepted", 0

    return {
        "contract_version": contract["contract_version"],
        "input": str(path),
        "validated_at": datetime.now(timezone.utc).isoformat(),
        "rows_in": len(df),
        "rows_clean": len(df) - q_count,
        "rows_quarantined": q_count,
        "quarantine_ratio": round(q_ratio, 6),
        "verdict": verdict,
        "exit_code": code,
        "failed_expectations": [r["id"] for r in results if not r["success"]],
        "checks": results,
        "_frame": df,
        "_quarantine_mask": quarantine,
        "_reasons": reasons,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--contract", default=str(CONTRACT))
    parser.add_argument("--out-dir", default=None,
                        help="Where to write clean/quarantine/validation files. Defaults beside the input.")
    args = parser.parse_args()

    src = Path(args.input)
    if not src.exists():
        print(f"input not found: {src}", file=sys.stderr)
        return 4

    res = validate(src, Path(args.contract))
    df, mask, reasons = res.pop("_frame"), res.pop("_quarantine_mask"), res.pop("_reasons")
    out_dir = Path(args.out_dir) if args.out_dir else src.parent
    out_dir.mkdir(parents=True, exist_ok=True)
    stem = src.stem

    clean = df[~mask]
    bad = df[mask].copy()
    if len(bad):
        bad["_quarantine_reason"] = [";".join(reasons.get(i, [])) for i in bad.index]
        bad["_quarantined_at"] = pd.Timestamp(datetime.now(timezone.utc).replace(tzinfo=None))

    clean_path = out_dir / f"{stem}.clean.parquet"
    quarantine_path = out_dir / f"{stem}.quarantine.parquet"
    report_path = out_dir / f"{stem}.validation.json"

    if res["verdict"] == "accepted":
        clean.to_parquet(clean_path, index=False)
    bad.to_parquet(quarantine_path, index=False)
    res["clean_path"] = str(clean_path) if res["verdict"] == "accepted" else None
    res["quarantine_path"] = str(quarantine_path)
    report_path.write_text(json.dumps(res, indent=2, default=str))

    for c in res["checks"]:
        if not c["success"]:
            print(f"FAIL [{c['severity']:>8}] {c['id']}  {c.get('unexpected_count', c.get('detail'))}")
    print(f"\ncontract {res['contract_version']} -> {res['verdict'].upper()}  "
          f"({res['rows_clean']:,} clean / {res['rows_quarantined']:,} quarantined "
          f"= {res['quarantine_ratio']:.2%})")
    print(f"report: {report_path}")
    return res["exit_code"]


if __name__ == "__main__":
    sys.exit(main())
