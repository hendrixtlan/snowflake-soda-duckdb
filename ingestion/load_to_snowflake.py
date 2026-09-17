"""Idempotent load of a validated batch into RAW.

Every load carries a `_batch_id`. Re-running the same batch replaces it instead
of appending, because an append-only loader run twice doubles RAW and corrupts
exactly the volume monitors that are supposed to detect a doubling.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()


def batch_id_for(path: Path) -> str:
    """Stable id derived from the file contents, so the same batch is the same id."""
    h = hashlib.sha256(path.read_bytes()).hexdigest()[:16]
    return f"{path.stem}-{h}"


def connect():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "WH_TRANSFORM"),
        database=os.getenv("SNOWFLAKE_DATABASE", "MUSIC_PREFS"),
        role=os.getenv("SNOWFLAKE_ROLE", "TRANSFORMER"),
        schema="RAW",
    )


def upper(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [c.upper() for c in out.columns]
    return out


def replace_batch(ctx, table: str, df: pd.DataFrame, database: str, batch_id: str) -> int:
    cur = ctx.cursor()
    cur.execute(f"DELETE FROM {database}.RAW.{table} WHERE _BATCH_ID = %s", (batch_id,))
    deleted = cur.rowcount or 0
    if deleted:
        print(f"  {table}: replaced {deleted:,} rows from a previous run of this batch")
    ok, _, nrows, _ = write_pandas(ctx, df, table, database=database, schema="RAW",
                                   auto_create_table=False)
    if not ok:
        raise RuntimeError(f"write_pandas failed for {table}")
    return nrows


def reload_reference(ctx, table: str, df: pd.DataFrame, database: str) -> int:
    """Catalogue tables are a snapshot, not a stream: truncate and reload."""
    ctx.cursor().execute(f"TRUNCATE TABLE {database}.RAW.{table}")
    ok, _, nrows, _ = write_pandas(ctx, df, table, database=database, schema="RAW",
                                   auto_create_table=False)
    if not ok:
        raise RuntimeError(f"write_pandas failed for {table}")
    return nrows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", required=True, help="Clean parquet emitted by the ingestion gate")
    parser.add_argument("--quarantine", default=None)
    parser.add_argument("--songs", required=True)
    parser.add_argument("--artists", required=True)
    parser.add_argument("--validation-report", default=None)
    args = parser.parse_args()

    database = os.getenv("SNOWFLAKE_DATABASE", "MUSIC_PREFS")
    events_path = Path(args.events)
    batch_id = batch_id_for(events_path)
    loaded_at = pd.Timestamp(datetime.now(timezone.utc).replace(tzinfo=None))

    events = upper(pd.read_parquet(events_path))
    events["_BATCH_ID"] = batch_id
    events["_LOADED_AT"] = loaded_at

    ctx = connect()
    try:
        print(f"batch {batch_id}")
        n = replace_batch(ctx, "LISTENING_EVENTS", events, database, batch_id)
        print(f"  LISTENING_EVENTS: {n:,} rows")

        if args.quarantine and Path(args.quarantine).exists():
            q = upper(pd.read_parquet(args.quarantine))
            if len(q):
                q["_BATCH_ID"] = batch_id
                q["_LOADED_AT"] = loaded_at
                nq = replace_batch(ctx, "LISTENING_EVENTS_QUARANTINE", q, database, batch_id)
                print(f"  LISTENING_EVENTS_QUARANTINE: {nq:,} rows")

        print(f"  SONGS: {reload_reference(ctx, 'SONGS', upper(pd.read_csv(args.songs)), database):,} rows")
        print(f"  ARTISTS: {reload_reference(ctx, 'ARTISTS', upper(pd.read_csv(args.artists)), database):,} rows")

        if args.validation_report and Path(args.validation_report).exists():
            report = json.loads(Path(args.validation_report).read_text())
            rows = pd.DataFrame([{
                "BATCH_ID": batch_id,
                "CONTRACT_VERSION": report["contract_version"],
                "VALIDATED_AT": report["validated_at"],
                "VERDICT": report["verdict"],
                "ROWS_IN": report["rows_in"],
                "ROWS_CLEAN": report["rows_clean"],
                "ROWS_QUARANTINED": report["rows_quarantined"],
                "FAILED_EXPECTATIONS": ",".join(report["failed_expectations"]),
                "CHECKS_JSON": json.dumps(report["checks"]),
            }])
            replace_batch_ctx = ctx.cursor()
            replace_batch_ctx.execute(
                f"DELETE FROM {database}.RAW.GE_VALIDATION_RESULTS WHERE BATCH_ID = %s", (batch_id,))
            write_pandas(ctx, rows, "GE_VALIDATION_RESULTS", database=database, schema="RAW",
                         auto_create_table=False)
            print("  GE_VALIDATION_RESULTS: 1 row")
    finally:
        ctx.close()


if __name__ == "__main__":
    main()
