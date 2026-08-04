#!/usr/bin/env bash
# Prove every generated selectors.yml is accepted AND resolvable by real dbt.
#
# `dbt ls --selector X` parses selectors.yml and resolves the selector against
# the manifest. It does not connect to the warehouse and materializes nothing, so
# this is a read-only check. An invalid selectors.yml fails the whole invocation,
# which is how the exclude-shape and indirect_selection defects were caught.
#
# Selector names are discovered from each generated file, so this is portable.
#
# Env:
#   DBT_PROJECT_DIR       required
#   DBT_PROFILES_DIR      [~/.dbt]
#   DBT_TARGET            [dev]
#   DBT_BIN               [dbt]
#   MAESTRO_E2E_WORKDIR   [./.maestro-e2e]
#   MAESTRO_E2E_ARTIFACTS [e2e/artifacts]
#   SAMPLE_PER_CONFIG     how many selectors to resolve per config [3]
set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT="${DBT_PROJECT_DIR:?set DBT_PROJECT_DIR}"
PROFILES="${DBT_PROFILES_DIR:-$HOME/.dbt}"
TARGET="${DBT_TARGET:-dev}"
DBT="${DBT_BIN:-dbt}"
WORKDIR="${MAESTRO_E2E_WORKDIR:-${MAESTRO_E2E_SCRATCH:-$PWD/.maestro-e2e}}"
ART="${MAESTRO_E2E_ARTIFACTS:-$REPO/e2e/artifacts}"
SAMPLE="${SAMPLE_PER_CONFIG:-3}"
PY="${PYTHON_BIN:-$(command -v python3)}"

BASELINE="${BASELINE_SELECTORS:-$WORKDIR/baseline-selectors.yml}"

dbt_ls() {
  "$DBT" ls --project-dir "$PROJECT" --profiles-dir "$PROFILES" --target "$TARGET" \
    --target-path "$WORKDIR/target" --log-path "$WORKDIR/logs" \
    --selector "$1" --output name 2>&1
}

# Prefer the special selectors (they exercise path/intersection shapes), then
# fill up to SAMPLE with the first auto-generated ones.
pick_selectors() {
  "$PY" - "$1" "$SAMPLE" <<'PY'
import sys, yaml
sels = (yaml.safe_load(open(sys.argv[1])) or {}).get("selectors", []) or []
names = [s["name"] for s in sels]
want, seen = [], set()
for n in names:
    if n.endswith(("_seeds", "_snapshots", "_full_refresh_incremental", "_orphan_models")):
        want.append(n); seen.add(n)
for n in names:
    if len(want) >= int(sys.argv[2]) + len(seen): break
    if n not in seen and not n.startswith("freshness_"):
        want.append(n)
print("\n".join(want))
PY
}

fails=0; total=0
printf "%-30s %-52s %-8s %s\n" CONFIG SELECTOR NODES STATUS
printf '%.0s-' {1..104}; echo
for f in "$ART"/selectors_*.yml; do
  [[ -f "$f" ]] || continue
  label="$(basename "$f" .yml)"; label="${label#selectors_}"
  cp "$f" "$PROJECT/selectors.yml"
  while IFS= read -r sel; do
    [[ -n "$sel" ]] || continue
    total=$((total+1))
    out="$(dbt_ls "$sel")"
    if grep -qE "Could not parse selector|Could not read selector|Encountered an error|Runtime Error|Compilation Error" <<<"$out"; then
      printf "%-30s %-52s %-8s %s\n" "$label" "$sel" "-" "PARSE/RESOLVE ERROR"
      grep -E "Could not (parse|read)|Runtime Error|Invalid value|Valid root-level" <<<"$out" \
        | head -2 | sed 's/^/      /'
      fails=$((fails+1))
    else
      nodes="$(grep -cE '^[a-zA-Z0-9_.]+$' <<<"$out")"
      printf "%-30s %-52s %-8s %s\n" "$label" "$sel" "$nodes" \
        "$([[ "$nodes" -eq 0 ]] && echo 'OK but 0 nodes' || echo OK)"
    fi
  done < <(pick_selectors "$f")
done

echo
[[ -f "$BASELINE" ]] && cp "$BASELINE" "$PROJECT/selectors.yml" \
  && echo "project selectors.yml restored from $BASELINE"
if [[ $fails -eq 0 ]]; then echo "dbt validation: all $total selector resolutions accepted"
else echo "dbt validation: $fails/$total FAILED"; fi
exit $fails
