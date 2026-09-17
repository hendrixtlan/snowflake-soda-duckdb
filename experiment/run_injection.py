"""Run one anomaly end to end and record which controls actually caught it.

This is the measurement instrument of the project. It injects a single anomaly
into a clean baseline, runs the full pipeline against it, and writes one row per
(anomaly x control) with what was observed -- never with what was expected.

A control that is not configured is recorded as `unavailable`, not as a miss:
an absent tool and a tool that failed to notice are different findings.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "ingestion" / "great_expectations"))

from generator.anomalies import inject  # noqa: E402

CONTROLS = ["great_expectations", "dbt", "soda", "monte_carlo"]
RESULTS = ROOT / "experiment" / "results"


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _run(cmd: list[str], cwd: Path | None = None, env: dict | None = None) -> tuple[int, str]:
    proc = subprocess.run(cmd, cwd=cwd or ROOT, capture_output=True, text=True,
                          env={**os.environ, **(env or {})})
    return proc.returncode, (proc.stdout + proc.stderr)


# --------------------------------------------------------------------------- #
# Controls
# --------------------------------------------------------------------------- #

def check_great_expectations(batch: Path) -> dict:
    from validate_batch import validate  # noqa: PLC0415
    res = validate(batch)
    for k in ("_frame", "_quarantine_mask", "_reasons"):
        res.pop(k, None)
    detected = res["verdict"] != "accepted" or bool(res["failed_expectations"])
    return {
        "detected": detected,
        "detail": json.dumps({
            "verdict": res["verdict"],
            "failed": res["failed_expectations"],
            "quarantined": res["rows_quarantined"],
        }),
        "artifact": res,
    }


def check_dbt() -> dict:
    code, out = _run(["dbt", "build", "--profiles-dir", "."], cwd=ROOT / "dbt_music")
    failures = []
    rr = ROOT / "dbt_music" / "target" / "run_results.json"
    if rr.exists():
        data = json.loads(rr.read_text())
        failures = [r["unique_id"] for r in data.get("results", [])
                    if r.get("status") in ("fail", "error")]
    # `dbt build` runs source freshness separately; it is the only thing that
    # can see a stale load, so it has to be part of this control.
    fcode, fout = _run(["dbt", "source", "freshness", "--profiles-dir", "."],
                       cwd=ROOT / "dbt_music")
    stale = fcode != 0 or "ERROR STALE" in fout or "WARN" in fout
    return {
        "detected": bool(failures) or code != 0 or stale,
        "detail": json.dumps({"failed_nodes": failures[:20], "exit": code, "stale_source": stale}),
        "artifact": {"stdout_tail": out[-2000:]},
    }


def check_soda() -> dict:
    scan = RESULTS / "soda_scan.json"
    scan.parent.mkdir(parents=True, exist_ok=True)
    code, out = _run([
        "soda", "scan", "-d", "snowflake", "-c", "soda/configuration.yml",
        "-srf", str(scan),
        "soda/checks/raw.yml", "soda/checks/core.yml", "soda/checks/marts.yml",
    ])
    failed, warned, evaluated = [], [], 0
    if scan.exists():
        data = json.loads(scan.read_text())
        checks = data.get("checks", [])
        evaluated = len(checks)
        failed = [c["name"] for c in checks if c.get("outcome") == "fail"]
        warned = [c["name"] for c in checks if c.get("outcome") == "warn"]
    return {
        "detected": bool(failed or warned),
        "detail": json.dumps({"failed": failed, "warned": warned,
                              "evaluated": evaluated, "exit": code}),
        "artifact": {"stdout_tail": out[-2000:]},
    }


def check_monte_carlo() -> dict:
    if not os.getenv("MONTECARLO_API_KEY"):
        return {"detected": None, "detail": json.dumps({"reason": "not configured"}),
                "artifact": {}}
    code, out = _run(["montecarlo", "incidents", "list", "--last-hours", "2", "--output", "json"])
    incidents = []
    if code == 0:
        try:
            incidents = json.loads(out)
        except json.JSONDecodeError:
            incidents = []
    return {
        "detected": bool(incidents),
        "detail": json.dumps({"incident_count": len(incidents)}),
        "artifact": {"stdout_tail": out[-2000:]},
    }


# --------------------------------------------------------------------------- #

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--anomaly", required=True, choices=[f"A{i}" for i in range(1, 15)])
    parser.add_argument("--baseline", default="data/generated/baseline.parquet")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--skip-warehouse", action="store_true",
                        help="Only run the controls that need no Snowflake connection.")
    args = parser.parse_args()

    baseline = Path(args.baseline)
    if not baseline.exists():
        print(f"baseline not found: {baseline} -- run `make baseline` first", file=sys.stderr)
        return 4

    run_id = args.run_id or f"{args.anomaly}-{int(time.time())}"
    RESULTS.mkdir(parents=True, exist_ok=True)

    injected_at = _now()
    batch = Path("data/generated") / f"experiment_{args.anomaly}.parquet"
    inject(pd.read_parquet(baseline), args.anomaly).to_parquet(batch, index=False)
    print(f"[{run_id}] injected {args.anomaly} -> {batch}")

    rows, artifacts = [], {}

    def record(control: str, result: dict) -> None:
        detected_at = _now()
        rows.append({
            "run_id": run_id,
            "anomaly": args.anomaly,
            "control": control,
            "detected": result["detected"],
            "detail": result["detail"],
            "injected_at": injected_at.isoformat(),
            "detected_at": detected_at.isoformat(),
            "latency_seconds": round((detected_at - injected_at).total_seconds(), 1),
        })
        artifacts[control] = result.get("artifact", {})
        flag = {True: "DETECTED", False: "missed", None: "unavailable"}[result["detected"]]
        print(f"  {control:<20} {flag}")

    ge_result = check_great_expectations(batch)
    record("great_expectations", ge_result)

    if args.skip_warehouse:
        print("  (warehouse controls skipped)")
    else:
        # The gate is a gate: if it rejected the batch, nothing downstream ever
        # sees the anomaly. Recording dbt/Soda as "missed" would be a lie.
        if ge_result["artifact"]["verdict"] != "accepted":
            for c in ("dbt", "soda"):
                record(c, {"detected": None,
                           "detail": json.dumps({"reason": "batch blocked at ingestion gate"}),
                           "artifact": {}})
        else:
            clean = batch.with_name(f"{batch.stem}.clean.parquet")
            quarantine = batch.with_name(f"{batch.stem}.quarantine.parquet")
            report = batch.with_name(f"{batch.stem}.validation.json")
            code, out = _run([sys.executable, "ingestion/load_to_snowflake.py",
                              "--events", str(clean),
                              "--quarantine", str(quarantine),
                              "--validation-report", str(report),
                              "--songs", "data/catalog/songs.csv",
                              "--artists", "data/catalog/artists.csv"])
            if code != 0:
                print(f"  load failed:\n{out[-1500:]}", file=sys.stderr)
                return 5
            record("dbt", check_dbt())
            record("soda", check_soda())
        record("monte_carlo", check_monte_carlo())

    df = pd.DataFrame(rows)
    out_csv = RESULTS / f"run_{run_id}.csv"
    df.to_csv(out_csv, index=False)
    (RESULTS / f"run_{run_id}.artifacts.json").write_text(json.dumps(artifacts, indent=2, default=str))
    print(f"[{run_id}] -> {out_csv}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
