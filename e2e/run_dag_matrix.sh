#!/usr/bin/env bash
# Drive the Airflow DAG-generation matrix.
#
# Each config carries both a `selector:` and an `airflow:` block, so this runs the
# real two-step flow: generate selectors, then generate DAGs from them. Every
# generated file is byte-compiled before Airflow ever sees it.
#
# Prereqs: ./e2e/render_configs.sh, and a manifest (dbt parse).
#
# Env: same as run_selector_matrix.sh
set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT="${DBT_PROJECT_DIR:?set DBT_PROJECT_DIR}"
WORKDIR="${MAESTRO_E2E_WORKDIR:-${MAESTRO_E2E_SCRATCH:-$PWD/.maestro-e2e}}"
RENDERED="${MAESTRO_E2E_RENDERED:-$WORKDIR/configs}"
ART="${MAESTRO_E2E_ARTIFACTS:-$REPO/e2e/artifacts}"
MAESTRO="${MAESTRO_BIN:-$(command -v maestro || echo "$REPO/.venv/bin/maestro")}"
PY="${PYTHON_BIN:-$(command -v python3)}"

mkdir -p "$ART" "$WORKDIR"
[[ -x "$MAESTRO" ]] || { echo "maestro not found (set MAESTRO_BIN)"; exit 1; }
[[ -d "$RENDERED" ]] || { echo "no rendered configs at $RENDERED - run e2e/render_configs.sh"; exit 1; }

BASELINE="${BASELINE_SELECTORS:-$WORKDIR/baseline-selectors.yml}"
if [[ ! -f "$BASELINE" ]]; then
  if [[ -f "$PROJECT/selectors.yml" ]]; then cp "$PROJECT/selectors.yml" "$BASELINE"
  else printf 'selectors: []\n' > "$BASELINE"; fi
fi

CONFIGS=(10-af-simple 11-af-staggered 12-af-none 13-af-combined-failfast
         14-af-combined-continue 15-af-commands 16-af-fullrefresh 17-af-legacy 18-af-v3)

printf "%-28s %-10s %-8s %s\n" CONFIG SELECTORS DAGS COMPILE
printf '%.0s-' {1..60}; echo

for cfg in "${CONFIGS[@]}"; do
  [[ -f "$RENDERED/$cfg.yml" ]] || { printf "%-28s %s\n" "$cfg" "(missing)"; continue; }
  cp "$BASELINE" "$PROJECT/selectors.yml"

  "$MAESTRO" generate --config "$RENDERED/$cfg.yml" > "$ART/$cfg.gen.log" 2>&1
  nsel=$(grep -c "^  - name:" "$PROJECT/selectors.yml" 2>/dev/null || echo 0)

  "$MAESTRO" generate-dags --config "$RENDERED/$cfg.yml" > "$ART/$cfg.dags.log" 2>&1
  dagdir=$("$PY" -c "
from dbt_job_maestro.config import Config
print(Config.from_yaml('$RENDERED/$cfg.yml').airflow.dags_dir)" 2>/dev/null)
  ndag=$(find "$dagdir" -maxdepth 1 -name '*.py' 2>/dev/null | wc -l | tr -d ' ')

  if "$PY" -m compileall -q "$dagdir" >/dev/null 2>&1; then compile=OK; else compile=SYNTAX_ERROR; fi
  printf "%-28s %-10s %-8s %s\n" "$cfg" "$nsel" "$ndag" "$compile"
done

cp "$BASELINE" "$PROJECT/selectors.yml"
echo
echo "project selectors.yml restored from $BASELINE"
