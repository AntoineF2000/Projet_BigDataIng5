"""Independent verification of the gold hub ranking with DuckDB.

DuckDB re-computes the top hubs *directly from the raw CSV on S3*, with plain
SQL on the IATA codes (no Spark code, no id repair), and compares the result
with the Parquet produced by Spark. Exit code 1 if the top-10 differs.

Usage: python src/verify/verify_duckdb.py --ingest-date 2026-11-16
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone

import duckdb

NULL_MARKER = r"\N"  # OpenFlights null marker


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    endpoint = os.environ["AWS_S3_ENDPOINT"].removeprefix("https://").removeprefix("http://")
    con.execute(
        """CREATE SECRET onyxia (TYPE s3, KEY_ID ?, SECRET ?, SESSION_TOKEN ?,
           ENDPOINT ?, REGION ?, URL_STYLE 'path')""",
        [os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"],
         os.environ.get("AWS_SESSION_TOKEN", ""), endpoint, os.environ.get("AWS_DEFAULT_REGION", "us-east-1")],
    )
    return con


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--bucket", default=os.getenv("BUCKET", "user-a-ferrer-ece"))
    p.add_argument("--prefix", default=os.getenv("PREFIX", "bigdata_project"))
    p.add_argument("--ingest-date", required=True)
    p.add_argument("--root", default=None, help="override storage root (local dry run)")
    a = p.parse_args()
    root = a.root or f"s3://{a.bucket}/{a.prefix}"
    routes_csv = f"{root}/raw/openflights/ingest_date={a.ingest_date}/routes.dat"
    airports_csv = f"{root}/raw/openflights/ingest_date={a.ingest_date}/airports.dat"
    print(f"[verify] user=a-ferrer-ece utc={datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S} duckdb={duckdb.__version__}")

    con = connect() if root.startswith("s3://") else duckdb.connect()
    # Re-implementation of the business rule in SQL, on the raw files only:
    # airport = known id, else IATA code, else ICAO code; drop unresolved and self loops.
    raw_sql = f"""
        WITH ap AS (
            SELECT TRY_CAST(column00 AS INT) AS id, column04 AS iata, column05 AS icao
            FROM read_csv('{airports_csv}', header=false, all_varchar=true, nullstr='{NULL_MARKER}',
                          quote='"', escape='"')
            WHERE TRY_CAST(column06 AS DOUBLE) BETWEEN -90 AND 90
              AND TRY_CAST(column07 AS DOUBLE) BETWEEN -180 AND 180
        ), r AS (
            SELECT upper(column2) AS sc, TRY_CAST(column3 AS INT) AS sid,
                   upper(column4) AS dc, TRY_CAST(column5 AS INT) AS did
            FROM read_csv('{routes_csv}', header=false, all_varchar=true, nullstr='{NULL_MARKER}')
        ), res AS (
            SELECT coalesce(a1.id, a2.id, a3.id) AS s, coalesce(b1.id, b2.id, b3.id) AS d
            FROM r
            LEFT JOIN ap a1 ON a1.id = r.sid  LEFT JOIN ap a2 ON a2.iata = r.sc  LEFT JOIN ap a3 ON a3.icao = r.sc
            LEFT JOIN ap b1 ON b1.id = r.did  LEFT JOIN ap b2 ON b2.iata = r.dc  LEFT JOIN ap b3 ON b3.icao = r.dc
        )
        SELECT ap.iata, count(DISTINCT res.d) AS n_destinations
        FROM res JOIN ap ON ap.id = res.s
        WHERE res.d IS NOT NULL AND res.s <> res.d
        GROUP BY ap.iata ORDER BY n_destinations DESC, ap.iata LIMIT 10
    """
    # Naive count on the route codes only, to show the effect of the id repair.
    naive = dict(con.execute(
        f"SELECT column2, count(DISTINCT column4) FROM read_csv('{routes_csv}', header=false, "
        "all_varchar=true) GROUP BY 1").fetchall())
    expected = con.execute(raw_sql).fetchall()
    gold = con.execute(
        f"SELECT iata, n_destinations FROM read_parquet('{root}/processed/gold/hub_ranking/*.parquet') "
        "ORDER BY rank LIMIT 10").fetchall()

    print(f"{'rank':>4} {'DuckDB(raw)':>16} {'Spark(gold)':>16}  status")
    ok = True
    for i, (e, g) in enumerate(zip(expected, gold), 1):
        same = e == g
        ok &= same
        print(f"{i:>4} {e[0]:>6} {e[1]:>9} {g[0]:>6} {g[1]:>9}  {'OK' if same else 'MISMATCH'}")
    print(f"[verify] naive code-only count: FRA={naive.get('FRA')} IST={naive.get('IST')} ISL={naive.get('ISL')}")
    print("[verify] RESULT:", "PASS - top-10 identical" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
