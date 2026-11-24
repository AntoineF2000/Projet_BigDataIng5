"""Measure the alternatives for the core aggregation (distinct destinations per
airport) with pandas, DuckDB and Spark, at scale x1, x10 and x100.

The x10/x100 inputs are produced by replicating routes.dat with a distinct
airline code per copy, so the number of rows (and groups work) really grows.

Usage: python src/benchmark/benchmark.py --raw-dir /tmp/raw --scales 1 10 100
Output: results/benchmark.csv
"""
from __future__ import annotations

import argparse
import csv
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

COLS = ["airline", "airline_id", "src", "src_id", "dst", "dst_id", "codeshare", "stops", "equipment"]


def make_scaled(raw: Path, factor: int, out_dir: Path) -> Path:
    out = out_dir / f"routes_x{factor}.csv"
    if not out.exists():
        lines = raw.read_text(encoding="utf-8").splitlines()
        with out.open("w", encoding="utf-8") as f:
            for k in range(factor):
                for line in lines:
                    f.write(f"{k:03d}{line}\n")  # unique airline code per replica
    return out


def timed(fn, repeat: int) -> tuple[float, object]:
    times, result = [], None
    for _ in range(repeat):
        t = time.perf_counter()
        result = fn()
        times.append(time.perf_counter() - t)
    return statistics.median(times), result


def run_pandas(path: Path):
    # only 2 columns + arrow-backed strings: ~3x less RAM than object dtype (pod limit)
    df = pd.read_csv(path, header=None, usecols=[2, 4], names=["src", "dst"],
                     dtype="string[pyarrow]")
    return df.groupby("src")["dst"].nunique().nlargest(1).to_dict()


def run_duckdb(path: Path):
    return duckdb.sql(
        f"SELECT column2, count(DISTINCT column4) n FROM read_csv('{path}', header=false, all_varchar=true) "
        "GROUP BY 1 ORDER BY n DESC LIMIT 1").fetchall()


def run_spark(spark: SparkSession, path: Path):
    df = spark.read.csv(f"file://{path}", header=False).toDF(*COLS)
    return (df.groupBy("src").agg(F.countDistinct("dst").alias("n"))
            .orderBy(F.desc("n")).limit(1).collect())


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--raw-dir", type=Path, required=True)
    p.add_argument("--scales", type=int, nargs="+", default=[1, 10, 100])
    p.add_argument("--repeat", type=int, default=3)
    p.add_argument("--out", type=Path, default=Path("results/benchmark.csv"))
    a = p.parse_args()
    work = a.raw_dir / "bench"
    work.mkdir(parents=True, exist_ok=True)
    print(f"[bench] user=a-ferrer-ece utc={datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S}")

    spark = (SparkSession.builder.master("local[2]").appName("a-ferrer-ece-benchmark")
             .config("spark.driver.memory", "1g").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    rows = []
    for factor in a.scales:
        path = make_scaled(a.raw_dir / "routes.dat", factor, work)
        size_mb = path.stat().st_size / 1e6
        engines = {"pandas": lambda: run_pandas(path), "duckdb": lambda: run_duckdb(path)}
        for parts in (200, 8):
            def spark_job(parts=parts):
                spark.conf.set("spark.sql.shuffle.partitions", parts)
                return run_spark(spark, path)
            engines[f"spark_shuffle{parts}"] = spark_job
        for name, fn in engines.items():
            fn()  # warm-up (JIT, Spark JVM, file cache)
            sec, res = timed(fn, a.repeat)
            rows.append({"scale": factor, "size_mb": round(size_mb, 1), "engine": name,
                         "median_s": round(sec, 3), "check": str(res)[:60]})
            print(f"[bench] x{factor:<4} {size_mb:7.1f} MB  {name:<16} {sec:7.3f} s  {str(res)[:50]}")
    spark.stop()
    a.out.parent.mkdir(parents=True, exist_ok=True)
    with a.out.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        w.writeheader()
        w.writerows(rows)
    print(f"[bench] written {a.out}")


if __name__ == "__main__":
    main()
