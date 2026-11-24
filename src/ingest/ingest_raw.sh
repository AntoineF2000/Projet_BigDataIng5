#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Ingestion (raw / bronze layer)
# Downloads the OpenFlights files from a pinned commit, checks integrity and
# uploads them unchanged to the Onyxia S3 bucket.
#
# Idempotent: re-running overwrites the same objects with identical content.
# Requires: curl, sha256sum, mc (MinIO client, preconfigured as alias "s3"
# on Onyxia services).
# -----------------------------------------------------------------------------
set -euo pipefail

BUCKET="${BUCKET:-user-a-ferrer-ece}"
PREFIX="${PREFIX:-bigdata_project}"
# Pin the upstream commit so every run downloads exactly the same bytes.
OPENFLIGHTS_REF="${OPENFLIGHTS_REF:-cd163fa17710f3135540651626ad719dd5322a05}"
BASE_URL="https://raw.githubusercontent.com/jpatokal/openflights/${OPENFLIGHTS_REF}/data"
FILES=(airports.dat airlines.dat routes.dat countries.dat)

INGEST_DATE="$(date -u +%Y-%m-%d)"
WORKDIR="$(mktemp -d)"
trap 'rm -rf "$WORKDIR"' EXIT

log() { printf '[%s] [ingest] %s\n' "$(date -u +%FT%TZ)" "$*"; }

log "user=${USER:-unknown} bucket=${BUCKET} prefix=${PREFIX} ref=${OPENFLIGHTS_REF}"
for f in "${FILES[@]}"; do
  curl --fail --silent --show-error --location --retry 3 -o "${WORKDIR}/${f}" "${BASE_URL}/${f}"
  log "downloaded ${f} ($(wc -c < "${WORKDIR}/${f}") bytes, $(wc -l < "${WORKDIR}/${f}") lines)"
done

( cd "$WORKDIR" && sha256sum "${FILES[@]}" > MANIFEST.sha256 )
total=$(cat "${WORKDIR}"/*.dat | wc -c)
log "total raw size: ${total} bytes"
# Guard rail from the project brief: raw data must stay between 1 MB and 5 MB.
if (( total < 1000000 || total > 5000000 )); then
  log "ERROR raw size outside [1 MB, 5 MB]"; exit 1
fi

DEST="s3/${BUCKET}/${PREFIX}/raw/openflights/ingest_date=${INGEST_DATE}"
mc cp --quiet "${WORKDIR}"/*.dat "${WORKDIR}/MANIFEST.sha256" "${DEST}/"
log "uploaded to ${DEST}"
mc ls --recursive "s3/${BUCKET}/${PREFIX}/raw/"
