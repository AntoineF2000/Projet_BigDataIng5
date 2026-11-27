# OpenFlights hub analysis - Big Data Ecosystem II

**Authors:** FERRER Antoine - AIFOUTE Othmane

**Prerequisites:** Onyxia account (namespace `user-a-ferrer-ece`) and a `vscode-pyspark` service (Spark 4.1.1, `mc`, S3 credentials injected as environment variables). The namespace quota only allows `requests.memory=200Mi`, so launch the service from the Onyxia YAML editor with the values of [`k8s/vscode-pyspark-values.yaml`](k8s/vscode-pyspark-values.yaml): requests 10m CPU / 200Mi, memory limit 3600Mi, persistence off. Spark therefore runs `local[2]` inside the pod (executor pods are rejected by the quota, see `STEPS=k8sprobe`). Run `pip install -r requirements.txt` to add DuckDB and pytest.

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
git clone https://github.com/AntoineF2000/Projet_BigDataIng5.git && cd Projet_BigDataIng5
source src/prompt.sh                     # author, Onyxia id and UTC time in the prompt (evidence)
pip install -r requirements.txt
bash src/run_all.sh                      # tests -> ingest -> pipeline -> verify
STEPS=bench bash src/run_all.sh          # optional: pandas/DuckDB/Spark benchmark x1/x10/x100
```

- **Restart:** every step writes with overwrite to deterministic paths, so a failed run is re-launched with the same command. To run a single step: `STEPS="pipeline verify" INGEST_DATE=2026-11-19 bash src/run_all.sh`.
- **Expired S3 token** (STS, 12 h): Onyxia > Stockage de données > Paramètres du profil > "MinIO Client (bash)", paste the `export MC_HOST_default=…` line in the terminal, then `source src/s3_env.sh`.
- **Local dry run without S3:** `python src/pipeline/run_pipeline.py --ingest-date D --root file:///tmp/bdp`.
- **Outputs:** `results/quality_report.json`, `results/*.log`, `results/benchmark.csv`. Screenshots are in `evidence/`.
