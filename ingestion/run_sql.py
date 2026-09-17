from __future__ import annotations
import os, sys
from pathlib import Path
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()

if len(sys.argv) != 2:
    raise SystemExit("usage: python ingestion/run_sql.py <sql-file>")

sql = Path(sys.argv[1]).read_text()
ctx = snowflake.connector.connect(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    role=os.getenv("SNOWFLAKE_ROLE"),
)
try:
    for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
        ctx.cursor().execute(stmt)
        print("OK:", stmt.splitlines()[0][:100])
finally:
    ctx.close()
