"""Turn a Soda scan result into the dataset's certification flag.

Certification is a property of the DATASET decided by the quality gates, not a
column computed alongside the data it is meant to judge. dbt marks a user as
`feature_eligible` (enough signal to profile them); this script is what says the
feature set as a whole can be trusted, and it can only say yes if Soda agreed.
"""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()

DATASETS = {
    "MARTS.USER_LISTENING_FEATURES": "USER_LISTENING_FEATURES",
    "MARTS.USER_SONG_AFFINITY": "USER_SONG_AFFINITY",
}


def summarise(scan: dict, dataset: str) -> dict:
    checks = [c for c in scan.get("checks", [])
              if c.get("table", "").upper().endswith(dataset.split(".")[-1])]
    failed = [c["name"] for c in checks if c.get("outcome") == "fail"]
    warned = [c["name"] for c in checks if c.get("outcome") == "warn"]
    passed = [c for c in checks if c.get("outcome") == "pass"]
    return {
        "DATASET": dataset,
        "CERTIFIED": not failed,
        "CHECKS_EVALUATED": len(checks),
        "CHECKS_PASSED": len(passed),
        "CHECKS_WARNED": len(warned),
        "CHECKS_FAILED": len(failed),
        "FAILED_CHECK_NAMES": ",".join(failed),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scan-result", required=True, help="Soda -srf JSON output")
    parser.add_argument("--batch-id", default="unknown")
    args = parser.parse_args()

    scan = json.loads(Path(args.scan_result).read_text())
    now = pd.Timestamp(datetime.now(timezone.utc).replace(tzinfo=None))
    rows = []
    for dataset in DATASETS:
        row = summarise(scan, dataset)
        row |= {"BATCH_ID": args.batch_id, "EVALUATED_AT": now}
        rows.append(row)

    df = pd.DataFrame(rows)[[
        "DATASET", "BATCH_ID", "EVALUATED_AT", "CERTIFIED", "CHECKS_EVALUATED",
        "CHECKS_PASSED", "CHECKS_WARNED", "CHECKS_FAILED", "FAILED_CHECK_NAMES",
    ]]

    database = os.getenv("SNOWFLAKE_DATABASE", "MUSIC_PREFS")
    ctx = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "WH_TRANSFORM"),
        database=database,
        role=os.getenv("SNOWFLAKE_ROLE", "TRANSFORMER"),
        schema="MARTS",
    )
    try:
        ctx.cursor().execute(
            f"DELETE FROM {database}.MARTS.DATA_QUALITY_STATUS WHERE BATCH_ID = %s",
            (args.batch_id,))
        write_pandas(ctx, df, "DATA_QUALITY_STATUS", database=database, schema="MARTS",
                     auto_create_table=False)
    finally:
        ctx.close()

    for r in rows:
        state = "CERTIFIED" if r["CERTIFIED"] else f"NOT CERTIFIED ({r['FAILED_CHECK_NAMES']})"
        print(f"{r['DATASET']}: {state}  "
              f"[{r['CHECKS_PASSED']}/{r['CHECKS_EVALUATED']} passed, {r['CHECKS_WARNED']} warned]")
    return 0 if all(r["CERTIFIED"] for r in rows) else 1


if __name__ == "__main__":
    raise SystemExit(main())
