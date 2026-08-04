#!/usr/bin/env bash
# Render e2e/configs/*.yml.tmpl into runnable configs for YOUR dbt project.
#
# The templates carry placeholders instead of paths so the matrix is portable:
#   __MANIFEST__      path to target/manifest.json
#   __PROJECT_DIR__   your dbt project root (selectors.yml is written here)
#   __PROFILES_DIR__  directory containing profiles.yml
#   __WORKDIR__       scratch dir for generated DAGs and jobs.yml
#   __TARGET__        dbt target name
#
# Usage:
#   DBT_PROJECT_DIR=/path/to/project \
#   DBT_PROFILES_DIR=~/.dbt \
#   MAESTRO_E2E_WORKDIR=/tmp/maestro-e2e \
#   DBT_TARGET=dev \
#     ./e2e/render_configs.sh
#
# Some templates reference example tags/paths/models/selectors (exclude_tags,
# full_refresh custom_schedules, selector_commands, freshness whitelists). Those
# are deliberately generic - edit the rendered configs to match real names in
# your project if you want those paths to select anything.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="${DBT_PROJECT_DIR:?set DBT_PROJECT_DIR}"
PROFILES_DIR="${DBT_PROFILES_DIR:-$HOME/.dbt}"
WORKDIR="${MAESTRO_E2E_WORKDIR:-${MAESTRO_E2E_SCRATCH:-$PWD/.maestro-e2e}}"
TARGET="${DBT_TARGET:-dev}"
MANIFEST="${MAESTRO_E2E_MANIFEST:-$WORKDIR/target/manifest.json}"
OUT="${MAESTRO_E2E_RENDERED:-$WORKDIR/configs}"

mkdir -p "$OUT"
n=0
for tmpl in "$HERE"/configs/*.yml.tmpl; do
  name="$(basename "$tmpl" .tmpl)"
  sed -e "s|__MANIFEST__|$MANIFEST|g" \
      -e "s|__PROJECT_DIR__|$PROJECT_DIR|g" \
      -e "s|__PROFILES_DIR__|$PROFILES_DIR|g" \
      -e "s|__WORKDIR__|$WORKDIR|g" \
      -e "s|__TARGET__|$TARGET|g" \
      "$tmpl" > "$OUT/$name"
  n=$((n+1))
done

echo "rendered $n configs -> $OUT"
echo "  project  : $PROJECT_DIR"
echo "  profiles : $PROFILES_DIR"
echo "  workdir  : $WORKDIR"
echo "  target   : $TARGET"
echo "  manifest : $MANIFEST"
[[ -f "$MANIFEST" ]] || echo "  NOTE: no manifest yet - run 'dbt parse --target-path $WORKDIR/target' first"
