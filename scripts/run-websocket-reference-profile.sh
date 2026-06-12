#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "El perfil de referencia requiere Linux para /proc y /usr/bin/time -v." >&2
  exit 2
fi

fd_limit="$(ulimit -n)"
if [[ "${fd_limit}" != "unlimited" ]] && (( fd_limit < 65536 )); then
  echo "ulimit -n debe ser al menos 65536; valor actual: ${fd_limit}" >&2
  exit 2
fi

for command in cargo curl lscpu free ps grep pgrep python3; do
  command -v "${command}" >/dev/null 2>&1 || {
    echo "Falta el comando requerido: ${command}" >&2
    exit 2
  }
done
[[ -x /usr/bin/time ]] || {
  echo "Falta /usr/bin/time." >&2
  exit 2
}

output_dir="target/ws-reference"
rm -rf "${output_dir}"
mkdir -p "${output_dir}"

{
  date --iso-8601=seconds
  uname -a
  rustc -Vv
  echo "ulimit -n: ${fd_limit}"
  lscpu
  free -h
} | tee "${output_dir}/environment.txt"

cargo build --release --example websocket --example websocket_load \
  >"${output_dir}/build.log" 2>&1

server_time_pid=""
server_pid=""
sampler_pid=""
cleanup() {
  [[ -n "${sampler_pid}" ]] && kill "${sampler_pid}" 2>/dev/null || true
  [[ -n "${server_pid}" ]] && kill -INT "${server_pid}" 2>/dev/null || true
  [[ -n "${server_time_pid}" ]] && kill "${server_time_pid}" 2>/dev/null || true
}
trap cleanup EXIT

RUSTREST_ADDR=127.0.0.1:3000 RUSTREST_WS_OBSERVER=off /usr/bin/time -v \
  -o "${output_dir}/server-time.txt" \
  target/release/examples/websocket \
  >"${output_dir}/server.log" 2>&1 &
server_time_pid=$!

for _ in $(seq 1 100); do
  server_pid="$(pgrep -P "${server_time_pid}" | head -1 || true)"
  [[ -n "${server_pid}" ]] && break
  sleep 0.1
done
[[ -n "${server_pid}" ]] || {
  echo "No se pudo identificar el proceso del servidor." >&2
  exit 1
}

for _ in $(seq 1 100); do
  status="$(curl --silent --max-time 1 --output /dev/null --write-out '%{http_code}' \
    http://127.0.0.1:3000/load || true)"
  [[ "${status}" == "400" || "${status}" == "426" ]] && break
  sleep 0.1
done
[[ "${status:-000}" == "400" || "${status:-000}" == "426" ]] || {
  echo "El servidor no estuvo listo antes del plazo." >&2
  exit 1
}

echo "elapsed_seconds,rss_kib,cpu_percent,open_fds" >"${output_dir}/server-samples.csv"
(
  started="$(date +%s)"
  while kill -0 "${server_pid}" 2>/dev/null; do
    now="$(date +%s)"
    read -r rss cpu < <(ps -p "${server_pid}" -o rss=,%cpu=)
    open_fds="$(find "/proc/${server_pid}/fd" -mindepth 1 -maxdepth 1 2>/dev/null | wc -l)"
    printf '%s,%s,%s,%s\n' "$((now - started))" "${rss}" "${cpu}" "${open_fds}"
    sleep 10
  done
) >>"${output_dir}/server-samples.csv" &
sampler_pid=$!

set +e
target/release/examples/websocket_load \
  --idle 10000 \
  --active 1000 \
  --duration-secs 900 \
  --message-bytes 256 \
  --connect-concurrency 256 \
  --json-out "${output_dir}/load-report.json" \
  >"${output_dir}/load.log" 2>&1
load_status=$?
set -e

kill "${sampler_pid}" 2>/dev/null || true
wait "${sampler_pid}" 2>/dev/null || true
sampler_pid=""

shutdown_started="$(date +%s%3N)"
kill -INT "${server_pid}"
for _ in $(seq 1 150); do
  ! kill -0 "${server_pid}" 2>/dev/null && break
  sleep 0.1
done
if kill -0 "${server_pid}" 2>/dev/null; then
  echo "El servidor no completo el cierre en 15 segundos." >&2
  exit 1
fi
shutdown_finished="$(date +%s%3N)"
printf '%s\n' "$((shutdown_finished - shutdown_started))" \
  >"${output_dir}/shutdown-millis.txt"
wait "${server_time_pid}"
server_time_pid=""
server_pid=""

if (( load_status != 0 )); then
  echo "El cliente de carga termino con estado ${load_status}." >&2
  exit "${load_status}"
fi
if grep -Eiq 'panic|panicked at' "${output_dir}/server.log"; then
  echo "Se encontro un panic en el log del servidor." >&2
  exit 1
fi

python3 - "${output_dir}/load-report.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as report_file:
    report = json.load(report_file)

expected = {
    "connected_idle": report["requested_idle"],
    "connected_active": report["requested_active"],
    "connect_failures": 0,
    "send_failures": 0,
    "receive_failures": 0,
    "unexpected_closes": 0,
}
for field, value in expected.items():
    if report[field] != value:
        raise SystemExit(f"{field}: esperado {value}, obtenido {report[field]}")
if report["sent_messages"] != report["received_messages"]:
    raise SystemExit("Se detectaron mensajes enviados sin eco")
PY

python3 - "${output_dir}/server-samples.csv" <<'PY'
import csv
import sys

with open(sys.argv[1], newline="", encoding="utf-8") as samples_file:
    samples = list(csv.DictReader(samples_file))

stable = [int(row["rss_kib"]) for row in samples if int(row["elapsed_seconds"]) >= 300]
if len(stable) < 2:
    raise SystemExit("No hay suficientes muestras RSS despues de 300 segundos")
first, final = stable[0], stable[-1]
minimum, maximum = min(stable), max(stable)
if final > first * 1.05:
    raise SystemExit("RSS final supera en mas de 5% la primera muestra estable")
if maximum > minimum * 1.10:
    raise SystemExit("La variacion RSS estable supera el 10%")
PY

trap - EXIT
echo "Perfil WebSocket completado en ${output_dir}."
