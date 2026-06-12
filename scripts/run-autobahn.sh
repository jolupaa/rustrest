#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

mkdir -p target/autobahn

endpoint_url="${AUTOBAHN_ENDPOINT_URL:-http://127.0.0.1:3000/autobahn}"
config_dir="${PWD}/autobahn"

if [[ -n "${AUTOBAHN_SERVER_URL:-}" ]]; then
  config_dir="${PWD}/target/autobahn/config"
  mkdir -p "${config_dir}"
  python3 - "${AUTOBAHN_SERVER_URL}" <<'PY'
import json
import sys
from pathlib import Path

source = Path("autobahn/fuzzingclient.json")
target = Path("target/autobahn/config/fuzzingclient.json")
config = json.loads(source.read_text(encoding="utf-8"))
config["servers"][0]["url"] = sys.argv[1]
target.write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")
PY
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker no esta instalado o no esta disponible en PATH." >&2
  exit 2
fi

if ! docker info >/dev/null 2>&1; then
  echo "El daemon de Docker no esta disponible." >&2
  exit 2
fi

status="$(curl --silent --show-error --max-time 3 --output /dev/null \
  --write-out '%{http_code}' "${endpoint_url}")"
case "${status}" in
  400|426) ;;
  *)
    echo "El endpoint /autobahn no esta listo (HTTP ${status})." >&2
    exit 2
    ;;
esac

rm -rf target/autobahn/server

docker run --rm --network host \
  -v "${config_dir}:/config:ro" \
  -v "${PWD}/target/autobahn:/reports" \
  crossbario/autobahn-testsuite:25.10.1 \
  wstest -m fuzzingclient -s /config/fuzzingclient.json

python3 scripts/check-autobahn-report.py target/autobahn/server
