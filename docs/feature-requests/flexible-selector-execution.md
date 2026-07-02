# Feature request: flexible selector execution

**Status:** implemented (see below)
**Targets:** dbt Cloud jobs (`generate-jobs`) and Airflow DAGs (`generate-dags`)

## Problem

Maestro turns dbt selectors into orchestration artifacts for two backends, but two
things were rigid:

1. **The command was fixed.** Every model selector ran `dbt build`. There was no way
   to say "this selector should only `run`", "only `test`", or "`run` then `test`".
2. **Multi-selector units were fail-fast.** When several selectors share a single unit
   (the auto-generated *combined small selectors* job/DAG created via
   `min_models_per_job` / `min_models_per_dag`), a failure in one selector stopped the
   rest.

## Request

### 1. Per-selector command flexibility

Each selector should be able to choose how it runs:

| Command | dbt invocation |
|---|---|
| `build` (default) | `dbt build --selector X` |
| `run` | `dbt run --selector X` |
| `test` | `dbt test --selector X` |
| `run_test` | `dbt run --selector X`, then `dbt test --selector X` |

To keep the automated set predictable, **auto-generated (`maestro_*`) selectors all use
one uniform `default_command`**, while **manual selectors can be overridden individually**
via `selector_commands`. Seeds, snapshots and full-refresh selectors keep their
type-specific command (`dbt seed` / `dbt snapshot` / `dbt build --full-refresh`).

### 2. Continue running sibling selectors when one fails

When multiple selectors share one unit, a failure in one should not stop the others.

**This is feasible in Airflow, but not in dbt Cloud** — called out explicitly:

- **dbt Cloud:** a job's `execute_steps` run strictly in order and the job **stops at
  the first failing step**; dbt-jobs-as-code exposes no continue-on-error, and
  `--selector` accepts only one named selector per invocation (so multiple selectors
  must be separate steps). The combined job is therefore inherently fail-fast.
  **Fallback:** set `min_models_per_job: 1` so each selector becomes its own isolated
  job — a failure then only affects that one job.
- **Airflow:** controlled by a new `airflow.continue_on_failure` flag. When `false`
  (default, non-breaking) the combined DAG chains its tasks (current behaviour). When
  `true`, the per-selector task groups are left independent so a failing selector never
  skips its siblings. Within a `run_test` selector, `run >> test` still holds.

## Behaviour summary

| Capability | dbt Cloud | Airflow |
|---|---|---|
| `build` / `run` / `test` per selector | ✅ single `execute_step` | ✅ single task |
| `run_test` per selector | ✅ two ordered `execute_steps` | ✅ `run >> test` (test skipped if run fails) |
| Uniform command for `maestro_*` selectors | ✅ | ✅ |
| Per-selector override for manual selectors | ✅ | ✅ |
| Continue other selectors after one fails | ❌ fail-fast — fallback `min_models_per_job: 1` | ✅ `continue_on_failure: true` |

## Configuration

```yaml
job:
  default_command: build          # build | run | run_test | test (all maestro_* selectors)
  selector_commands:              # manual selectors only
    critical_revenue: run_test

airflow:
  default_command: build
  continue_on_failure: false      # true = siblings keep running when one fails
  selector_commands:
    critical_revenue: test
```
