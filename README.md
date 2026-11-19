# OpenFlights hub analysis — Big Data Ecosystem II

**Authors:** FERRER Antoine - AIFOUTE Othmane

**Prerequisites:** Onyxia account (namespace `user-a-ferrer-ece`) and a `vscode-pyspark` service; it ships Python 3, Spark, `mc` with the `s3` alias, and S3 credentials as environment variables. Run `pip install -r requirements.txt` to add DuckDB and pytest.

**Dataset:** OpenFlights Airports/Airlines/Routes/Countries. Source: <https://github.com/jpatokal/openflights> (data from <https://openflights.org/data.php>), pinned to commit `cd163fa`. License: ODbL 1.0. Downloaded on 2026-11-16. Size: 3.91 MB in 4 files (`routes.dat` 67,663 rows, `airports.dat` 7,698, `airlines.dat` 6,162, `countries.dat` 261).

**Problem statement:** Which airports are the most connected hubs of the world air network, measured by the number of distinct direct destinations? How dependent is each country on international links? The pipeline outputs a full hub ranking and a per-country connectivity table. The top-10 ranking is independently recomputed with DuckDB from the raw files.

**S3 paths** (bucket `user-a-ferrer-ece`, prefix `bigdata_project/`):

| Layer | Path |
|---|---|
| raw (unchanged CSV + SHA-256 manifest) | `raw/openflights/ingest_date=YYYY-MM-DD/` |
| silver (typed, cleaned Parquet) | `processed/silver/{airports,airlines,routes,routes_quarantine}/` |
| gold (answers, Parquet + CSV) | `processed/gold/{hub_ranking,country_connectivity}[_csv]/` |
| quality report (JSON) | `processed/quality_report/` |

**Reproduction** (in the Onyxia VS Code terminal):

```bash
git clone <repo-url> && cd bigdata_project
pip install -r requirements.txt
bash src/run_all.sh                      # tests -> ingest -> pipeline -> verify
STEPS=bench bash src/run_all.sh          # optional: pandas/DuckDB/Spark benchmark x1/x10/x100
```

- **Restart:** every step writes with overwrite to deterministic paths, so a failed run is re-launched with the same command. To run a single step: `STEPS="pipeline verify" INGEST_DATE=2026-11-16 bash src/run_all.sh`.
- **Local dry run without S3:** `python src/pipeline/run_pipeline.py --ingest-date D --root file:///tmp/bdp`.
- **Outputs:** `results/quality_report.json`, `results/*.log`, `results/benchmark.csv`. Screenshots are in `evidence/`.
