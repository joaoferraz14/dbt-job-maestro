#!/usr/bin/env bash
# Load every generated DAG variant into a real Airflow DagBag.
#
# Variants targeting Airflow 2 (airflow_version: 2) are loaded with AIRFLOW2_PYTHON;
# the airflow_version: 3 variant is loaded with AIRFLOW3_PYTHON. A variant passes
# only with ZERO import errors and a DAG count matching the generated file count.
#
# The quickest way to get an Airflow 2 interpreter is the launcher, which builds one:
#   ./scripts/maestro_airflow_up.sh up --project-dir <p> --airflow-version 2.10.5
#   export AIRFLOW2_PYTHON=<workdir>/airflow-venv/bin/python
#
# Env:
#   MAESTRO_E2E_RENDERED  rendered configs dir      [$MAESTRO_E2E_WORKDIR/configs]
#   AIRFLOW2_PYTHON       python with Airflow 2.x   [required for variants 10-17]
#   AIRFLOW3_PYTHON       python with Airflow 3.x   [optional, for variant 18]
set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKDIR="${MAESTRO_E2E_WORKDIR:-${MAESTRO_E2E_SCRATCH:-$PWD/.maestro-e2e}}"
RENDERED="${MAESTRO_E2E_RENDERED:-$WORKDIR/configs}"
AF2_PY="${AIRFLOW2_PYTHON:-}"
AF3_PY="${AIRFLOW3_PYTHON:-}"
CFG_PY="${PYTHON_BIN:-$(command -v python3)}"

export AIRFLOW_HOME="${AIRFLOW_HOME:-$WORKDIR/airflow_home}"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
mkdir -p "$AIRFLOW_HOME"

[[ -d "$RENDERED" ]] || { echo "no rendered configs at $RENDERED - run e2e/render_configs.sh"; exit 1; }
[[ -n "$AF2_PY" || -n "$AF3_PY" ]] || { echo "set AIRFLOW2_PYTHON and/or AIRFLOW3_PYTHON"; exit 1; }

probe() {  # $1=python  $2=dags_dir
  "$1" - "$2" <<'PY'
import sys, warnings
warnings.filterwarnings("ignore")
import airflow
from airflow.models import DagBag
bag = DagBag(dag_folder=sys.argv[1], include_examples=False)
print(f"AIRFLOW={airflow.__version__}")
print(f"DAGS={len(bag.dags)}")
print(f"ERRORS={len(bag.import_errors)}")
for path, err in list(bag.import_errors.items())[:3]:
    print(f"ERR {path.split('/')[-1]}: {err.strip().splitlines()[-1]}")
PY
}

dags_dir_of() {
  "$CFG_PY" -c "
from dbt_job_maestro.config import Config
print(Config.from_yaml('$RENDERED/$1.yml').airflow.dags_dir)" 2>/dev/null
}
version_of() {
  "$CFG_PY" -c "
from dbt_job_maestro.config import Config
print(Config.from_yaml('$RENDERED/$1.yml').airflow.airflow_version)" 2>/dev/null
}

printf "%-28s %-9s %-7s %-7s %-7s %s\n" VARIANT AIRFLOW FILES DAGS ERRORS STATUS
printf '%.0s-' {1..80}; echo

fails=0
for cfg in 10-af-simple 11-af-staggered 12-af-none 13-af-combined-failfast \
           14-af-combined-continue 15-af-commands 16-af-fullrefresh 17-af-legacy 18-af-v3; do
  [[ -f "$RENDERED/$cfg.yml" ]] || continue
  dagdir="$(dags_dir_of "$cfg")"
  [[ -d "$dagdir" ]] || { printf "%-28s %s\n" "$cfg" "(no DAGs - run run_dag_matrix.sh)"; continue; }
  files=$(find "$dagdir" -maxdepth 1 -name '*.py' | wc -l | tr -d ' ')

  want="$(version_of "$cfg")"
  if [[ "$want" == "3" ]]; then py="$AF3_PY"; else py="$AF2_PY"; fi
  if [[ -z "$py" ]]; then
    printf "%-28s %s\n" "$cfg" "(skipped - no Airflow $want interpreter configured)"; continue
  fi

  out=$(probe "$py" "$dagdir" 2>/dev/null)
  av=$(sed -n 's/^AIRFLOW=//p' <<<"$out")
  dags=$(sed -n 's/^DAGS=//p' <<<"$out")
  errs=$(sed -n 's/^ERRORS=//p' <<<"$out")

  if [[ "$errs" == "0" && "$dags" == "$files" ]]; then status=OK
  else status=FAIL; fails=$((fails+1)); fi
  printf "%-28s %-9s %-7s %-7s %-7s %s\n" "$cfg" "$av" "$files" "$dags" "$errs" "$status"
  grep '^ERR ' <<<"$out" | sed 's/^/      /'
done

# Demonstrate the defect the airflow_version option exists to fix: Airflow-2 output
# loaded by Airflow 3 produces an import error for every single DAG.
if [[ -n "$AF3_PY" && -f "$RENDERED/10-af-simple.yml" ]]; then
  d10="$(dags_dir_of 10-af-simple)"
  if [[ -d "$d10" ]]; then
    echo
    echo "--- cross-check: airflow_version:2 output loaded by Airflow 3 ---"
    probe "$AF3_PY" "$d10" 2>/dev/null | grep -E "^AIRFLOW|^DAGS|^ERRORS|^ERR " | head -5
  fi
fi

echo
if [[ $fails -eq 0 ]]; then echo "import validation: all variants loaded cleanly"
else echo "import validation: $fails variant(s) FAILED"; fi
exit $fails
