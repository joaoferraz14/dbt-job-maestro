#!/usr/bin/env bash
# Drive the selector-generation matrix.
#
# Each config starts from a pristine copy of the project's baseline selectors.yml
# so manual-selector preservation is exercised from a known state. 07 is the
# exception: it runs immediately after 02 *without* a reset, proving that
# auto-generated freshness selectors are removed when the flag is turned off.
#
# Prereqs:
#   ./e2e/render_configs.sh          (renders configs for your project)
#   a manifest at $MAESTRO_E2E_WORKDIR/target/manifest.json (dbt parse)
#
# Env:
#   DBT_PROJECT_DIR       required - selectors.yml is written here
#   MAESTRO_E2E_WORKDIR   scratch dir                    [./.maestro-e2e]
#   MAESTRO_E2E_RENDERED  rendered configs               [$WORKDIR/configs]
#   MAESTRO_E2E_ARTIFACTS where to archive output        [e2e/artifacts]
#   BASELINE_SELECTORS    pristine selectors.yml to reset from
#                         [<project>/selectors.yml captured on first run]
#   MAESTRO_BIN           maestro executable             [maestro, then .venv]
set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT="${DBT_PROJECT_DIR:?set DBT_PROJECT_DIR}"
WORKDIR="${MAESTRO_E2E_WORKDIR:-${MAESTRO_E2E_SCRATCH:-$PWD/.maestro-e2e}}"
RENDERED="${MAESTRO_E2E_RENDERED:-$WORKDIR/configs}"
ART="${MAESTRO_E2E_ARTIFACTS:-$REPO/e2e/artifacts}"
MAESTRO="${MAESTRO_BIN:-$(command -v maestro || echo "$REPO/.venv/bin/maestro")}"

mkdir -p "$ART" "$WORKDIR"
[[ -x "$MAESTRO" ]] || { echo "maestro not found (set MAESTRO_BIN)"; exit 1; }
[[ -d "$RENDERED" ]] || { echo "no rendered configs at $RENDERED - run e2e/render_configs.sh"; exit 1; }

# Capture the project's original selectors.yml once, so resets are reproducible
# and the project file can always be restored.
BASELINE="${BASELINE_SELECTORS:-$WORKDIR/baseline-selectors.yml}"
if [[ ! -f "$BASELINE" ]]; then
  if [[ -f "$PROJECT/selectors.yml" ]]; then
    cp "$PROJECT/selectors.yml" "$BASELINE"
    echo "captured baseline selectors.yml -> $BASELINE"
  else
    printf 'selectors: []\n' > "$BASELINE"
    echo "no existing selectors.yml; using an empty baseline"
  fi
fi

reset_selectors() { cp "$BASELINE" "$PROJECT/selectors.yml"; }

run_config() {
  local cfg="$1" label="$2" reset="${3:-yes}"
  [[ ! -f "$RENDERED/$cfg" ]] && { echo "skip $label (no $cfg)"; return 0; }
  [[ "$reset" == "yes" ]] && reset_selectors
  printf '%.0s=' {1..62}; echo
  echo "CONFIG: $label   (reset=$reset)"
  printf '%.0s=' {1..62}; echo
  "$MAESTRO" generate --config "$RENDERED/$cfg" > "$ART/$label.log" 2>&1
  local rc=$?
  cp "$PROJECT/selectors.yml" "$ART/selectors_$label.yml" 2>/dev/null
  echo "exit=$rc  selectors=$(grep -c '^  - name:' "$PROJECT/selectors.yml" 2>/dev/null || echo '?')"
  grep -E "^(Generated|Writing|Found)" "$ART/$label.log" | tail -3
  echo
}

run_config 01-baseline.yml                01-baseline
run_config 02-special.yml                 02-special
run_config 07-freshness-removal.yml       07-freshness-removal no
run_config 03-special-fqn.yml             03-special-fqn
run_config 04-exclusions.yml              04-exclusions
run_config 05-exclusions-intersection.yml 05-exclusions-intersection
run_config 06-prefix-format.yml           06-prefix-format
run_config 08-freshness-all.yml           08-freshness-all
run_config 09-freshness-whitelist.yml     09-freshness-whitelist

reset_selectors
echo "project selectors.yml restored from $BASELINE"
echo "artifacts: $ART"
