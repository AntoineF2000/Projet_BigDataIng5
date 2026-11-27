# Source me after renewing the Onyxia S3 token (STS tokens expire after 12 h).
#   1. Onyxia > Stockage de données > Paramètres du profil > "MinIO Client (bash)" > copy
#   2. paste the `export MC_HOST_s3=...` line in the terminal
#   3. source src/s3_env.sh   -> derives AWS_* for Spark (S3A) and DuckDB from MC_HOST_s3
# Secrets are never echoed nor written to disk.
# Onyxia names the alias after the S3 profile ("default"); the pipeline uses alias "s3".
if [ -n "${MC_HOST_default:-}" ]; then export MC_HOST_s3="$MC_HOST_default"; fi
if [ -z "${MC_HOST_s3:-}" ]; then
  echo "[s3_env] MC_HOST_s3 is not set" >&2
  return 1 2>/dev/null || exit 1
fi
eval "$(python3 - <<'PY'
import os, shlex
from urllib.parse import urlsplit, unquote
u = urlsplit(os.environ["MC_HOST_s3"])
key, secret, token = (unquote(x) for x in (u.netloc.rsplit("@", 1)[0].split(":", 2) + [""])[:3])
for k, v in {"AWS_ACCESS_KEY_ID": key, "AWS_SECRET_ACCESS_KEY": secret,
             "AWS_SESSION_TOKEN": token, "AWS_S3_ENDPOINT": u.hostname}.items():
    print(f"export {k}={shlex.quote(v)}")
PY
)"
python3 - <<'PY'
import base64, datetime as dt, json, os
exp = json.loads(base64.urlsafe_b64decode(os.environ["AWS_SESSION_TOKEN"].split(".")[1] + "=="))["exp"]
print("[s3_env] S3 credentials loaded, valid until",
      dt.datetime.fromtimestamp(exp, dt.timezone.utc).strftime("%Y-%m-%d %H:%M UTC"))
PY
