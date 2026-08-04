#!/usr/bin/env bash
#
# maestro_airflow_up.sh - spin up a local Airflow serving maestro-generated DAGs.
#
# Point it at any dbt project and it will:
#   1. produce a manifest (dbt parse) if one is missing
#   2. generate selectors.yml and one DAG file per selector (maestro)
#   3. install an isolated Airflow into a throwaway venv
#   4. start Airflow standalone and wait until the DAGs are actually registered
#
# Nothing is scheduled: DAGs are generated with orchestration_mode=none and are
# created paused, so you trigger what you want from the UI. Only your dbt
# project's selectors.yml is written to (dbt requires it at the project root) and
# any existing one is backed up first.
#
# ---------------------------------------------------------------------------
# QUICK START
#   ./scripts/maestro_airflow_up.sh up --project-dir /path/to/your/dbt/project
#   ./scripts/maestro_airflow_up.sh status
#   ./scripts/maestro_airflow_up.sh down
# ---------------------------------------------------------------------------

set -uo pipefail

# ===========================================================================
# VARIABLES YOU NEED TO SET
# Every one can be given as an environment variable or a --flag (see --help).
# ===========================================================================

# --- REQUIRED --------------------------------------------------------------
# Absolute path to the dbt project you want to orchestrate. Must contain
# dbt_project.yml. There is no default: this is the one thing only you know.
DBT_PROJECT_DIR="${DBT_PROJECT_DIR:-}"

# --- USUALLY WORTH SETTING -------------------------------------------------
# Directory holding profiles.yml (the dir, not the file).
DBT_PROFILES_DIR="${DBT_PROFILES_DIR:-$HOME/.dbt}"
# dbt target to run as. IMPORTANT: some projects switch schema naming on
# target.name (a custom_schema macro keyed to 'dev'/'ci' is a common pattern),
# so a made-up target name can silently redirect writes to the wrong schemas.
# Use a target your project actually expects.
DBT_TARGET="${DBT_TARGET:-dev}"
# Threads passed to dbt via --threads.
DBT_THREADS="${DBT_THREADS:-4}"
# Port for the Airflow web UI.
PORT="${PORT:-8080}"

# --- USUALLY FINE AS-IS ----------------------------------------------------
# dbt executable. Set this if you have more than one dbt on PATH (e.g. dbt-core
# in a venv alongside a global dbt); the generated DAGs shell out to a bare
# `dbt`, so whatever resolves first is what actually runs.
DBT_BIN="${DBT_BIN:-dbt}"
# maestro executable. Defaults to `maestro` on PATH, then this repo's venv.
MAESTRO_BIN="${MAESTRO_BIN:-}"
# Airflow version to install into the throwaway venv. 2.x and 3.x both work -
# the script detects the major version and generates matching DAG syntax.
AIRFLOW_VERSION="${AIRFLOW_VERSION:-2.10.5}"
# Python for the Airflow venv. Airflow 2.10 supports <=3.12; Airflow 3 supports 3.12+.
PYTHON_VERSION="${PYTHON_VERSION:-3.12}"
# Where all generated state lives (venv, DAGs, AIRFLOW_HOME, logs, config).
WORKDIR="${WORKDIR:-$PWD/.maestro-airflow}"
# Where the generated DAG files land. Left empty here so we can tell "the user
# chose this" from "nobody said" - see resolve_dags_dir below for the precedence.
DAGS_DIR="${DAGS_DIR:-}"
# Fallback when neither a flag/env var nor a config file says otherwise.
# Deliberately OUTSIDE the workdir: the DAGs are the artefact you want to open,
# edit and point a real Airflow at, not throwaway state in a dot-directory.
DAGS_DIR_FALLBACK="$HOME/Desktop/maestro-dags"
# Use your own maestro config instead of the one this script generates.
# When set, the script does not override its selector/airflow settings.
MAESTRO_CONFIG="${MAESTRO_CONFIG:-}"

# --- GENERATED-CONFIG TUNING (ignored when MAESTRO_CONFIG is set) ----------
SELECTOR_PREFIX="${SELECTOR_PREFIX:-maestro}"
DEFAULT_COMMAND="${DEFAULT_COMMAND:-build}"      # build | run | test | run_test
MIN_MODELS_PER_DAG="${MIN_MODELS_PER_DAG:-1}"    # >1 combines tiny selectors into one DAG
COMBINE_ORPHANS="${COMBINE_ORPHANS:-true}"       # bundle single-model components
INCLUDE_SEEDS="${INCLUDE_SEEDS:-true}"
INCLUDE_SNAPSHOTS="${INCLUDE_SNAPSHOTS:-true}"
CONTINUE_ON_FAILURE="${CONTINUE_ON_FAILURE:-false}"
# Visual-only lineage nodes inside each DAG. On by default locally: seeing which
# models a selector builds is the main reason to run Airflow on your laptop.
LINEAGE_TASKS="${LINEAGE_TASKS:-true}"
LINEAGE_MAX_MODELS="${LINEAGE_MAX_MODELS:-50}"
# 'none' keeps every DAG manual-trigger only. Use simple/staggered to see crons.
ORCHESTRATION_MODE="${ORCHESTRATION_MODE:-none}"

# ===========================================================================

RED=$'\033[31m'; GRN=$'\033[32m'; YLW=$'\033[33m'; BLD=$'\033[1m'; RST=$'\033[0m'
say()  { printf "%s\n" "$*"; }
info() { printf "%s==>%s %s\n" "$BLD" "$RST" "$*"; }
ok()   { printf "%s  ok%s   %s\n" "$GRN" "$RST" "$*"; }
warn() { printf "%s  warn%s %s\n" "$YLW" "$RST" "$*"; }
die()  { printf "%s  fail%s %s\n" "$RED" "$RST" "$*" >&2; exit 1; }

usage() {
  cat <<EOF
${BLD}maestro_airflow_up.sh${RST} - local Airflow serving maestro-generated DAGs

USAGE
  $(basename "$0") <command> [options]

COMMANDS
  up        generate selectors + DAGs, install Airflow, start it (default)
  prepare   everything except starting Airflow (for use with a supervisor)
  serve     run Airflow in the FOREGROUND (pair with prepare; for Tilt/systemd)
  regen     regenerate selectors + DAGs only; a running Airflow picks them up
  status    show whether Airflow is up, DAG counts, import errors, URL
  logs      tail the Airflow log
  down      stop Airflow (leaves generated files in place)
  clean     stop Airflow and delete WORKDIR entirely (DAGS_DIR is left alone)

OPTIONS (each maps to the env var of the same name)
  --project-dir PATH      DBT_PROJECT_DIR   (required)
  --profiles-dir PATH     DBT_PROFILES_DIR  [$DBT_PROFILES_DIR]
  --target NAME           DBT_TARGET        [$DBT_TARGET]
  --threads N             DBT_THREADS       [$DBT_THREADS]
  --port N                PORT              [$PORT]
  --dbt-bin PATH          DBT_BIN           [$DBT_BIN]
  --maestro-bin PATH      MAESTRO_BIN       [auto-detect]
  --airflow-version V     AIRFLOW_VERSION   [$AIRFLOW_VERSION]
  --python-version V      PYTHON_VERSION    [$PYTHON_VERSION]
  --workdir PATH          WORKDIR           [$WORKDIR]
  --dags-dir PATH         DAGS_DIR          [$DAGS_DIR]
  --config PATH           MAESTRO_CONFIG    [generated]
  --default-command CMD   DEFAULT_COMMAND   [$DEFAULT_COMMAND]
  --min-models-per-dag N  MIN_MODELS_PER_DAG[$MIN_MODELS_PER_DAG]
  --orchestration MODE    ORCHESTRATION_MODE[$ORCHESTRATION_MODE]
  --selector-prefix P     SELECTOR_PREFIX   [$SELECTOR_PREFIX]
  --continue-on-failure   CONTINUE_ON_FAILURE=true
  --no-lineage            LINEAGE_TASKS=false (skip visual lineage nodes)
  --lineage-max N         LINEAGE_MAX_MODELS[$LINEAGE_MAX_MODELS] (0 = no cap)
  --parse                 force a dbt parse even if a manifest exists
  -h, --help              this help

EXAMPLES
  # simplest
  $(basename "$0") up --project-dir ~/code/my_dbt_project

  # a dbt-core in a venv, custom profiles dir, Airflow 3, port 8081
  $(basename "$0") up \\
      --project-dir ~/code/my_dbt_project \\
      --dbt-bin ~/code/my_dbt_project/.venv/bin/dbt \\
      --profiles-dir ~/code/my_dbt_project/.dbt \\
      --airflow-version 3.0.2 --python-version 3.12 --port 8081

  # see the combined-small-selectors DAG and cron staggering
  $(basename "$0") up --project-dir ~/p --min-models-per-dag 5 --orchestration staggered

NOTES
  * Writes <project>/selectors.yml (dbt only reads it from the project root).
    Any existing file is backed up to selectors.yml.bak-<timestamp>.
  * Everything else lands in WORKDIR and is safe to delete.
  * Generated DAGs invoke a bare \`dbt\`; the script puts DBT_BIN's directory
    first on PATH for the Airflow processes so the right dbt is used.
EOF
}

# ---------------------------------------------------------------- arg parsing
CMD="up"; FORCE_PARSE=0
[[ $# -gt 0 && "$1" != -* ]] && { CMD="$1"; shift; }
while [[ $# -gt 0 ]]; do
  case "$1" in
    --project-dir)        DBT_PROJECT_DIR="$2"; shift 2;;
    --profiles-dir)       DBT_PROFILES_DIR="$2"; shift 2;;
    --target)             DBT_TARGET="$2"; shift 2;;
    --threads)            DBT_THREADS="$2"; shift 2;;
    --port)               PORT="$2"; shift 2;;
    --dbt-bin)            DBT_BIN="$2"; shift 2;;
    --maestro-bin)        MAESTRO_BIN="$2"; shift 2;;
    --airflow-version)    AIRFLOW_VERSION="$2"; shift 2;;
    --python-version)     PYTHON_VERSION="$2"; shift 2;;
    --workdir)            WORKDIR="$2"; shift 2;;
    --dags-dir)           DAGS_DIR="$2"; shift 2;;
    --config)             MAESTRO_CONFIG="$2"; shift 2;;
    --default-command)    DEFAULT_COMMAND="$2"; shift 2;;
    --min-models-per-dag) MIN_MODELS_PER_DAG="$2"; shift 2;;
    --orchestration)      ORCHESTRATION_MODE="$2"; shift 2;;
    --selector-prefix)    SELECTOR_PREFIX="$2"; shift 2;;
    --continue-on-failure) CONTINUE_ON_FAILURE=true; shift;;
    --no-lineage)         LINEAGE_TASKS=false; shift;;
    --lineage-max)        LINEAGE_MAX_MODELS="$2"; shift 2;;
    --parse)              FORCE_PARSE=1; shift;;
    -h|--help)            usage; exit 0;;
    *) die "unknown option: $1  (try --help)";;
  esac
done

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mkdir -p "$WORKDIR" 2>/dev/null || die "cannot create WORKDIR: $WORKDIR"
WORKDIR="$(cd "$WORKDIR" && pwd)"
AF_HOME="$WORKDIR/airflow_home"
AF_VENV="$WORKDIR/airflow-venv"
GEN_CONFIG="$WORKDIR/maestro-config.generated.yml"

# Read airflow.dags_dir out of a maestro config file. Prints nothing when the
# file, the airflow: block, or the key is absent.
config_dags_dir() {
  [[ -f "$1" ]] || return 0
  # Slice out the airflow: block (up to the next top-level key), then take the
  # first indented dags_dir:, minus any trailing comment and surrounding quotes.
  sed -n '/^airflow:/,/^[^[:space:]#]/p' "$1" \
    | sed -n 's/^[[:space:]][[:space:]]*dags_dir:[[:space:]]*//p' \
    | head -1 \
    | sed -e 's/[[:space:]]*#.*$//' -e 's/^"\(.*\)"$/\1/' -e "s/^'\(.*\)'\$/\1/"
}

# Decide where DAGs go, highest priority first:
#   1. --dags-dir / DAGS_DIR        - an explicit instruction always wins
#   2. airflow.dags_dir in a config - so a repo that declares a path keeps it
#   3. DAGS_DIR_FALLBACK            - ~/Desktop/maestro-dags
#
# Step 2 matters for `tilt up` inside a project that already has a maestro
# config: without it the built-in default would silently relocate that project's
# DAGs. Only dags_dir is taken from a *discovered* config - adopting the whole
# file would also swap dbt_target and friends, which is not something to do
# behind the user's back. Pass --config to use a config file in full.
resolve_dags_dir() {
  local src
  if [[ -n "$DAGS_DIR" ]]; then
    src="--dags-dir/DAGS_DIR"
  else
    local cfg="$MAESTRO_CONFIG" found=""
    if [[ -z "$cfg" ]]; then
      for cand in \
        "${DBT_PROJECT_DIR:+$DBT_PROJECT_DIR/maestro-config.yml}" \
        "${DBT_PROJECT_DIR:+$DBT_PROJECT_DIR/maestro-config.yaml}" \
        "$PWD/maestro-config.yml" "$PWD/maestro-config.yaml"; do
        [[ -n "$cand" && -f "$cand" ]] && { cfg="$cand"; found=1; break; }
      done
    fi
    if [[ -n "$cfg" ]]; then
      local from_cfg; from_cfg="$(config_dags_dir "$cfg")"
      if [[ -n "$from_cfg" ]]; then
        # A relative dags_dir is relative to the config file, not to $PWD.
        [[ "$from_cfg" == /* ]] || from_cfg="$(cd "$(dirname "$cfg")" && pwd)/$from_cfg"
        DAGS_DIR="$from_cfg"
        src="airflow.dags_dir in ${found:+discovered }$cfg"
      fi
    fi
  fi
  if [[ -z "$DAGS_DIR" ]]; then
    DAGS_DIR="$DAGS_DIR_FALLBACK"
    src="default"
  fi
  mkdir -p "$DAGS_DIR" 2>/dev/null || die "cannot create DAGS_DIR: $DAGS_DIR"
  DAGS_DIR="$(cd "$DAGS_DIR" && pwd)"
  DAGS_DIR_SOURCE="$src"
}
resolve_dags_dir
PIDFILE="$WORKDIR/airflow.pid"
LOGFILE="$WORKDIR/airflow.log"

af_env() {
  export AIRFLOW_HOME="$AF_HOME"
  export AIRFLOW__CORE__DAGS_FOLDER="$DAGS_DIR"
  export AIRFLOW__CORE__LOAD_EXAMPLES=False
  export AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True
  export AIRFLOW__WEBSERVER__WEB_SERVER_PORT="$PORT"
  # `airflow standalone` re-invokes a bare `airflow` for its subprocesses, and
  # the generated DAGs shell out to a bare `dbt`. Both must resolve.
  local dbt_dir; dbt_dir="$(dirname "$(command -v "$DBT_BIN" 2>/dev/null || echo /nonexistent)")"
  export PATH="$AF_VENV/bin:$dbt_dir:$PATH"
}

af_python() { echo "$AF_VENV/bin/python"; }
af_bin()    { echo "$AF_VENV/bin/airflow"; }

airflow_major() {
  "$(af_python)" -c "import airflow,sys;sys.stdout.write(airflow.__version__.split('.')[0])" 2>/dev/null || echo ""
}

http_code() {
  # curl already prints 000 on a failed connection and exits non-zero, so an
  # `|| echo 000` fallback would concatenate and produce "000000".
  local code
  code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "http://localhost:$PORT$1" 2>/dev/null)"
  printf "%s" "${code:-000}"
}

# ------------------------------------------------------------------ preflight
resolve_maestro() {
  if [[ -n "$MAESTRO_BIN" ]]; then echo "$MAESTRO_BIN"; return; fi
  if command -v maestro >/dev/null 2>&1; then command -v maestro; return; fi
  if [[ -x "$REPO_ROOT/.venv/bin/maestro" ]]; then echo "$REPO_ROOT/.venv/bin/maestro"; return; fi
  echo ""
}

preflight() {
  info "Preflight"
  [[ -n "$DBT_PROJECT_DIR" ]] || die "DBT_PROJECT_DIR is required (--project-dir /path/to/dbt/project)"
  [[ -d "$DBT_PROJECT_DIR" ]] || die "project dir does not exist: $DBT_PROJECT_DIR"
  DBT_PROJECT_DIR="$(cd "$DBT_PROJECT_DIR" && pwd)"
  [[ -f "$DBT_PROJECT_DIR/dbt_project.yml" ]] || die "no dbt_project.yml in $DBT_PROJECT_DIR"
  ok "dbt project: $DBT_PROJECT_DIR"

  [[ -d "$DBT_PROFILES_DIR" ]] || die "profiles dir does not exist: $DBT_PROFILES_DIR"
  DBT_PROFILES_DIR="$(cd "$DBT_PROFILES_DIR" && pwd)"
  [[ -f "$DBT_PROFILES_DIR/profiles.yml" ]] || warn "no profiles.yml in $DBT_PROFILES_DIR"
  ok "profiles dir: $DBT_PROFILES_DIR"

  command -v "$DBT_BIN" >/dev/null 2>&1 || die "dbt not found: $DBT_BIN (set --dbt-bin)"
  ok "dbt: $(command -v "$DBT_BIN")  [$("$DBT_BIN" --version 2>&1 | sed -n 's/.*installed: *//p' | head -1)]"

  MAESTRO="$(resolve_maestro)"
  [[ -n "$MAESTRO" ]] || die "maestro not found. pip install -e . in this repo, or set --maestro-bin"
  ok "maestro: $MAESTRO"

  if [[ "${preflight_skip_port:-0}" != "1" ]]; then
    if [[ "$(http_code /health)" != "000" ]]; then
      die "something is already listening on port $PORT - use --port, or run '$(basename "$0") down'"
    fi
    ok "port $PORT is free"
  fi
}

# ------------------------------------------------------------------- manifest
ensure_manifest() {
  local manifest="$WORKDIR/target/manifest.json"
  if [[ -f "$manifest" && $FORCE_PARSE -eq 0 ]]; then
    ok "reusing manifest: $manifest"; MANIFEST="$manifest"; return
  fi
  info "Producing a manifest (dbt parse - no warehouse writes)"
  mkdir -p "$WORKDIR/target" "$WORKDIR/logs"
  if ! "$DBT_BIN" parse \
        --project-dir "$DBT_PROJECT_DIR" --profiles-dir "$DBT_PROFILES_DIR" \
        --target "$DBT_TARGET" --target-path "$WORKDIR/target" \
        --log-path "$WORKDIR/logs" > "$WORKDIR/dbt-parse.log" 2>&1; then
    tail -25 "$WORKDIR/dbt-parse.log" >&2
    die "dbt parse failed - see $WORKDIR/dbt-parse.log"
  fi
  [[ -f "$manifest" ]] || die "dbt parse produced no manifest at $manifest"
  MANIFEST="$manifest"
  ok "manifest: $(du -h "$manifest" | cut -f1)"
}

# --------------------------------------------------------------------- config
write_config() {
  if [[ -n "$MAESTRO_CONFIG" ]]; then
    [[ -f "$MAESTRO_CONFIG" ]] || die "config not found: $MAESTRO_CONFIG"
    CONFIG="$MAESTRO_CONFIG"; ok "using your config: $CONFIG"; return
  fi

  # Match generated DAG syntax to the Airflow that will actually load it.
  local major; major="$(airflow_major)"; [[ -n "$major" ]] || major=2
  info "Writing maestro config (airflow_version: $major)"

  cat > "$GEN_CONFIG" <<EOF
# Generated by scripts/maestro_airflow_up.sh - safe to edit and re-run with
#   $(basename "$0") regen --config $GEN_CONFIG
manifest_path: $MANIFEST
output_dir: $DBT_PROJECT_DIR
selectors_output_file: selectors.yml
jobs_output_file: $WORKDIR/jobs.yml

selector:
  selector_prefix: $SELECTOR_PREFIX
  combine_single_model_selectors: $COMBINE_ORPHANS
  include_seeds_selectors: $INCLUDE_SEEDS
  include_snapshots_selectors: $INCLUDE_SNAPSHOTS
  seeds_selector_method: path
  snapshots_selector_method: path

airflow:
  airflow_version: $major
  dags_dir: $DAGS_DIR
  dbt_project_dir: $DBT_PROJECT_DIR
  dbt_profiles_dir: $DBT_PROFILES_DIR
  dbt_target: $DBT_TARGET
  dbt_threads: $DBT_THREADS
  orchestration_mode: $ORCHESTRATION_MODE
  default_command: $DEFAULT_COMMAND
  min_models_per_dag: $MIN_MODELS_PER_DAG
  continue_on_failure: $CONTINUE_ON_FAILURE
  lineage_tasks: $LINEAGE_TASKS
  lineage_max_models: $LINEAGE_MAX_MODELS
EOF
  CONFIG="$GEN_CONFIG"
  ok "config: $CONFIG"
}

# ------------------------------------------------------------------- generate
generate() {
  local sel="$DBT_PROJECT_DIR/selectors.yml"
  if [[ -f "$sel" ]]; then
    local bak="$sel.bak-$(date +%Y%m%d-%H%M%S)"
    cp "$sel" "$bak"
    warn "backed up existing selectors.yml -> $(basename "$bak")"
  fi

  info "maestro generate  (selectors.yml)"
  "$MAESTRO" generate --config "$CONFIG" > "$WORKDIR/maestro-generate.log" 2>&1 \
    || { tail -25 "$WORKDIR/maestro-generate.log" >&2; die "maestro generate failed"; }
  ok "$(grep -c '^  - name:' "$sel" 2>/dev/null || echo '?') selectors in $sel"

  info "maestro generate-dags"
  mkdir -p "$DAGS_DIR"
  "$MAESTRO" generate-dags --config "$CONFIG" > "$WORKDIR/maestro-dags.log" 2>&1 \
    || { tail -25 "$WORKDIR/maestro-dags.log" >&2; die "maestro generate-dags failed"; }
  local n; n=$(find "$DAGS_DIR" -maxdepth 1 -name '*.py' | wc -l | tr -d ' ')
  [[ "$n" -gt 0 ]] || die "no DAG files were written to $DAGS_DIR"
  ok "$n DAG files in $DAGS_DIR"

  # Fail early on syntax before Airflow ever sees them.
  if command -v python3 >/dev/null 2>&1; then
    python3 -m compileall -q "$DAGS_DIR" >/dev/null 2>&1 \
      && ok "all DAG files compile" || warn "some DAG files failed to byte-compile"
  fi
}

# -------------------------------------------------------------------- install
install_airflow() {
  if [[ -x "$(af_bin)" ]]; then
    ok "airflow venv exists: $(af_bin) ($("$(af_bin)" version 2>/dev/null | tail -1))"
    return
  fi
  info "Creating Airflow venv (apache-airflow==$AIRFLOW_VERSION on Python $PYTHON_VERSION)"
  if command -v uv >/dev/null 2>&1; then
    UV_PYTHON_INSTALL_DIR="${UV_PYTHON_INSTALL_DIR:-$WORKDIR/uv-python}" \
      uv venv --python "$PYTHON_VERSION" "$AF_VENV" >/dev/null 2>&1 \
      || die "uv venv failed for Python $PYTHON_VERSION"
    UV_PYTHON_INSTALL_DIR="${UV_PYTHON_INSTALL_DIR:-$WORKDIR/uv-python}" \
      uv pip install --python "$(af_python)" "apache-airflow==$AIRFLOW_VERSION" \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-$AIRFLOW_VERSION/constraints-$PYTHON_VERSION.txt" \
      > "$WORKDIR/airflow-install.log" 2>&1 \
      || { tail -20 "$WORKDIR/airflow-install.log" >&2; die "airflow install failed"; }
  else
    local py="python$PYTHON_VERSION"
    command -v "$py" >/dev/null 2>&1 || die "$py not found and uv unavailable - install uv or Python $PYTHON_VERSION"
    "$py" -m venv "$AF_VENV" || die "venv creation failed"
    "$(af_python)" -m pip install -q --upgrade pip >/dev/null 2>&1
    "$(af_python)" -m pip install "apache-airflow==$AIRFLOW_VERSION" \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-$AIRFLOW_VERSION/constraints-$PYTHON_VERSION.txt" \
      > "$WORKDIR/airflow-install.log" 2>&1 \
      || { tail -20 "$WORKDIR/airflow-install.log" >&2; die "airflow install failed"; }
  fi
  ok "installed Airflow $("$(af_bin)" version 2>/dev/null | tail -1)"
}

init_db() {
  info "Initialising Airflow metadata DB"
  mkdir -p "$AF_HOME"
  local out
  out=$("$(af_bin)" db migrate 2>&1) || { echo "$out" | tail -15 >&2; die "airflow db migrate failed"; }
  ok "metadata DB ready"
}

# ---------------------------------------------------------------------- serve
start_airflow() {
  info "Starting Airflow standalone on port $PORT"
  nohup "$(af_bin)" standalone > "$LOGFILE" 2>&1 &
  echo $! > "$PIDFILE"

  local waited=0
  printf "  waiting for the web server "
  while [[ $waited -lt 180 ]]; do
    if [[ "$(http_code /health)" == "200" ]]; then printf " up\n"; break; fi
    printf "."; sleep 5; waited=$((waited+5))
  done
  [[ "$(http_code /health)" == "200" ]] || {
    printf "\n"; tail -30 "$LOGFILE" >&2; die "web server did not come up - see $LOGFILE"; }

  # The UI is reachable before the scheduler has serialized the DAGs into the
  # metadata DB. Until it has, DAGs cannot be triggered or unpaused, so wait.
  local expected; expected=$(find "$DAGS_DIR" -maxdepth 1 -name '*.py' | wc -l | tr -d ' ')
  printf "  waiting for the scheduler to register %s DAGs " "$expected"
  waited=0
  while [[ $waited -lt 300 ]]; do
    local n; n=$(dag_count)
    [[ "$n" -ge "$expected" ]] && { printf " %s registered\n" "$n"; break; }
    printf "."; sleep 5; waited=$((waited+5))
  done
  printf "\n"
}

dag_count() {
  local n
  n="$("$(af_python)" - "$AF_HOME/airflow.db" <<'PY' 2>/dev/null
import sqlite3, sys
try:
    c = sqlite3.connect(sys.argv[1])
    print(c.execute("select count(*) from dag").fetchone()[0])
except Exception:
    print(0)
PY
)"
  printf "%s" "${n:-0}"
}

import_error_count() {
  # grep -c prints 0 AND exits 1 when there are no matches, so a `|| echo 0`
  # fallback would emit a second value.
  local n
  n="$("$(af_bin)" dags list-import-errors 2>/dev/null | grep -c '\.py' || true)"
  printf "%s" "${n:-0}"
}

report_ready() {
  local pw="(see $AF_HOME/standalone_admin_password.txt)"
  [[ -f "$AF_HOME/standalone_admin_password.txt" ]] && pw="$(cat "$AF_HOME/standalone_admin_password.txt")"
  local errs; errs="$(import_error_count)"
  cat <<EOF

${GRN}${BLD}Airflow is up.${RST}

  URL         ${BLD}http://localhost:$PORT${RST}
  username    admin
  password    $pw

  DAGs        $(dag_count) registered   (import errors: $errs)
  dags dir    $DAGS_DIR   ($DAGS_DIR_SOURCE)
  selectors   $DBT_PROJECT_DIR/selectors.yml
  config      $CONFIG
  log         $LOGFILE

Every DAG is paused and unscheduled - nothing runs until you trigger it.
To run one:  UI > pick a DAG > unpause > "Trigger DAG"
       or:   AIRFLOW_HOME=$AF_HOME $(af_bin) dags trigger <dag_id>

Stop it with: $(basename "$0") down --workdir $WORKDIR
EOF
}

cmd_up() {
  preflight
  install_airflow
  ensure_manifest
  write_config
  generate
  init_db
  af_env
  start_airflow
  report_ready
}

# `prepare` + `serve` split `up` for process supervisors (Tilt, systemd, docker
# entrypoints). Those tools expect to own the long-running process: `up`
# backgrounds Airflow and returns, so a supervisor that reaps the command's
# process group on exit kills Airflow with it.
cmd_prepare() {
  preflight_skip_port=1 preflight
  install_airflow
  ensure_manifest
  write_config
  generate
  init_db
  ok "prepared - start Airflow in the foreground with: $(basename "$0") serve"
}

cmd_serve() {
  [[ -n "$DBT_PROJECT_DIR" ]] || die "DBT_PROJECT_DIR is required"
  [[ -x "$(af_bin)" ]] || die "no Airflow venv at $AF_VENV - run '$(basename "$0") prepare' first"
  af_env
  info "Starting Airflow standalone in the FOREGROUND on port $PORT"
  say "  UI will be at http://localhost:$PORT (user admin; password in"
  say "  $AF_HOME/standalone_admin_password.txt once created)"
  exec "$(af_bin)" standalone
}

cmd_regen() {
  [[ -n "$DBT_PROJECT_DIR" ]] || die "DBT_PROJECT_DIR is required"
  DBT_PROJECT_DIR="$(cd "$DBT_PROJECT_DIR" && pwd)"
  MAESTRO="$(resolve_maestro)"; [[ -n "$MAESTRO" ]] || die "maestro not found"
  af_env
  ensure_manifest
  write_config
  generate
  ok "regenerated - a running scheduler will pick the changes up within ~30s"
}

cmd_status() {
  af_env
  local code; code="$(http_code /health)"
  say "workdir : $WORKDIR"
  say "url     : http://localhost:$PORT  (health HTTP $code)"
  if [[ "$code" == "200" ]]; then
    say "state   : ${GRN}running${RST}"
    say "dags    : $(dag_count) registered"
    if [[ -x "$(af_bin)" ]]; then
      say "errors  : $(import_error_count) import errors"
    fi
  else
    say "state   : ${YLW}not running${RST}"
  fi
  say "dags dir: $DAGS_DIR ($(find "$DAGS_DIR" -maxdepth 1 -name '*.py' 2>/dev/null | wc -l | tr -d ' ') files, from $DAGS_DIR_SOURCE)"
}

cmd_down() {
  info "Stopping Airflow"
  local stopped=0
  # Everything below is scoped to THIS workdir's venv path. A bare
  # `pkill -f "airflow standalone"` would also kill unrelated Airflow instances
  # on other ports, including other people's.
  if [[ -f "$PIDFILE" ]]; then
    local pid; pid="$(cat "$PIDFILE" 2>/dev/null || true)"
    if [[ -n "${pid:-}" ]]; then
      pkill -TERM -P "$pid" 2>/dev/null && stopped=1   # standalone's children
      kill -TERM "$pid" 2>/dev/null && stopped=1
    fi
    rm -f "$PIDFILE"
  fi
  # scheduler / webserver / triggerer all execute from this venv
  pkill -f "^${AF_VENV}/bin/" 2>/dev/null && stopped=1
  pkill -f "${AF_VENV}/bin/airflow" 2>/dev/null && stopped=1
  sleep 3

  if [[ "$(http_code /health)" == "000" ]]; then
    ok "stopped"
  else
    warn "port $PORT is still answering - a gunicorn worker may have outlived its parent."
    warn "processes still running from this workdir:"
    pgrep -fl "$AF_VENV" 2>/dev/null | sed 's/^/    /' || true
    warn "if needed: pkill -f '$AF_VENV'"
  fi
  [[ $stopped -eq 1 ]] || warn "nothing looked like it was running for workdir $WORKDIR"
}

cmd_clean() { cmd_down; info "Removing $WORKDIR"; rm -rf "$WORKDIR"; ok "removed"; }
cmd_logs()  { [[ -f "$LOGFILE" ]] || die "no log at $LOGFILE"; tail -f "$LOGFILE"; }

case "$CMD" in
  up)      cmd_up;;
  prepare) cmd_prepare;;
  serve)   cmd_serve;;
  regen)  cmd_regen;;
  status) cmd_status;;
  down)   cmd_down;;
  clean)  cmd_clean;;
  logs)   cmd_logs;;
  *)      usage; die "unknown command: $CMD";;
esac
