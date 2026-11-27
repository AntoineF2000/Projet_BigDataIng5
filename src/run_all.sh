#!/usr/bin/env bash
# End-to-end run on an Onyxia vscode-pyspark service:
#   tests -> ingest (raw to S3) -> Spark pipeline (silver/gold) -> DuckDB verification.
# Every step is idempotent; on failure, fix and re-run the same command,
# or run a single step: STEPS="pipeline verify" bash src/run_all.sh
set -euo pipefail
cd "$(dirname "$0")/.."

export BUCKET="${BUCKET:-user-a-ferrer-ece}"
export PREFIX="${PREFIX:-bigdata_project}"
export INGEST_DATE="${INGEST_DATE:-$(date -u +%Y-%m-%d)}"
STEPS="${STEPS:-tests ingest pipeline verify}"
mkdir -p results

OWNER="${OWNER:-Antoine FERRER (a-ferrer-ece)}"
banner() { printf '\n===== %s | %s | ns=%s | %s UTC =====\n' "$1" "$OWNER" "${KUBERNETES_NAMESPACE:-?}" "$(date -u '+%F %T')"; }

for step in $STEPS; do
  case "$step" in
    tests)    banner "unit tests";  python -m pytest -q tests ;;
    ingest)   banner "ingest";      bash src/ingest/ingest_raw.sh ;;
    pipeline) banner "pipeline";    python src/pipeline/run_pipeline.py --ingest-date "$INGEST_DATE" 2>&1 | tee results/pipeline.log ;;
    verify)   banner "verify";      python src/verify/verify_duckdb.py --ingest-date "$INGEST_DATE" | tee results/verify.log ;;
    bench)    banner "benchmark"
              raw=/tmp/openflights_raw; mkdir -p "$raw"
              mc cp --quiet --recursive "s3/${BUCKET}/${PREFIX}/raw/openflights/ingest_date=${INGEST_DATE}/" "$raw/"
              python src/benchmark/benchmark.py --raw-dir "$raw" | tee results/benchmark.log ;;
    k8sprobe) banner "Spark on Kubernetes probe (rejected alternative)"
              # One executor of 512m: shows whether the namespace quota admits executor pods.
              timeout 150 python - <<'PY' || echo "[k8sprobe] no executor obtained within 150 s"
from pyspark.sql import SparkSession
s = (SparkSession.builder.master("k8s://https://kubernetes.default.svc:443")
     .appName("a-ferrer-ece-k8s-probe").config("spark.dynamicAllocation.enabled", "false")
     .config("spark.executor.instances", "1").config("spark.executor.memory", "512m")
     .config("spark.scheduler.minRegisteredResourcesRatio", "1")
     .config("spark.scheduler.maxRegisteredResourcesWaitingTime", "120s").getOrCreate())
print("[k8sprobe] executors:", s.sparkContext._jsc.sc().getExecutorMemoryStatus().size() - 1)
s.stop()
PY
              kubectl get events --sort-by=.lastTimestamp 2>/dev/null | grep -iE 'quota|exec' | tail -4 | cut -c1-220 ;;
    *) echo "unknown step $step" >&2; exit 2 ;;
  esac
done
banner "done"
mc ls --recursive "s3/${BUCKET}/${PREFIX}/" | grep -v '_SUCCESS' | head -60
