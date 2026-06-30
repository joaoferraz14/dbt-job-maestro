# Architecture

## Complete Workflow

Maestro generates selectors once, then targets **either** dbt Cloud **or** Airflow
(or both) from those same selectors.

```
┌─────────────────────────────────────────────────────────────┐
│                    LOCAL DEVELOPMENT                         │
└─────────────────────────────────────────────────────────────┘

1. Edit dbt models
2. dbt compile → target/manifest.json
3. maestro generate → selectors.yml
4a. maestro generate-jobs → jobs.yml          (dbt Cloud path)
4b. maestro generate-dags → dbt_maestro_dag.py (Airflow path)
5. git commit + push

┌─────────────────────────────────────────────────────────────┐
│                    VERSION CONTROL                           │
└─────────────────────────────────────────────────────────────┘

6. Pull request review
7. Merge to main branch

┌──────────────────────────────┐   ┌──────────────────────────────┐
│        DBT CLOUD PATH        │   │         AIRFLOW PATH         │
└──────────────────────────────┘   └──────────────────────────────┘

8a. dbt-jobs-as-code sync          8b. Copy dbt_maestro_dag.py into
    --config jobs.yml                  the Airflow dags/ folder
9a. Jobs deployed to dbt Cloud ✅   9b. DAG scheduled in Airflow ✅
```

## Components

### dbt-job-maestro (This Package)
- **Input**: `manifest.json`, `selectors.yml`
- **Output**: `selectors.yml`, `jobs.yml` (dbt Cloud), `*.py` Airflow DAGs
- **Purpose**: Generate selector definitions and orchestration artifacts for
  dbt Cloud and/or Airflow from the same dependency analysis

### Generator modules
| Module | Output | Target |
|--------|--------|--------|
| `selector_orchestrator.py` | `selectors.yml` | shared |
| `job_generator.py` | `jobs.yml` | dbt Cloud (`dbt-jobs-as-code`) |
| `airflow_dag_generator.py` | `*.py` DAG | Apache Airflow |

Both `JobGenerator` and `AirflowDAGGenerator` consume the same `selectors.yml`
and apply the same selector classification (models / seeds / snapshots / full
refresh, freshness selectors skipped), so the two orchestrators stay in sync.

## Selector Generation

### FQN-Based Dependency Analysis

Maestro uses **FQN (Fully Qualified Name) selector generation**. It analyzes the
dependency graph in `manifest.json`, finds connected components, and creates one
selector per component using `method: fqn`.

- **Multi-model components**: Models sharing dependencies are grouped into a single selector
- **Single-model components (orphans)**: Models with no dependency connections to other auto-generated models. By default, each gets its own selector. With `combine_single_model_selectors: true`, all orphans are combined into one selector (e.g., `maestro_orphan_models`)

### Special Selectors

In addition to dependency-based selectors, maestro can generate:
- **Seeds selector** (`maestro_seeds`): groups all seed files via `method: path` or `method: fqn`
- **Snapshots selector** (`maestro_snapshots`): groups all snapshot files
- **Full refresh selector** (`maestro_full_refresh_incremental`): selects all incremental models for full refresh runs

## Manual Selector Preservation

Across all methods, selectors NOT starting with the configured prefix (`maestro_` by default) are
considered **manual selectors** and are always preserved. Models covered by manual selectors are
automatically excluded from auto-generation to prevent duplicates.

- `critical_revenue`, `my_custom_selector` → Manual (preserved)
- `maestro_stg_customers` → Auto-generated (replaced on regeneration)

## Freshness Selector Identification

Freshness selectors follow specific naming patterns to determine if they are auto-generated or manual:

### Auto-Generated Freshness Selectors

These are managed by maestro and will be:
- **Created** when `include_freshness_selectors: true`
- **Removed** when `include_freshness_selectors: false`

**Patterns:**
```
freshness_{prefix}_*           → e.g., freshness_maestro_dim_customers
freshness_selector_independent → Special case for independent models
```

### Manual Freshness Selectors

These are always preserved regardless of `include_freshness_selectors` setting:

**Patterns (anything NOT matching auto-generated):**
```
freshness_my_custom_selector   → Manual (no prefix match)
freshness_critical_models      → Manual (no prefix match)
```

### Detection Logic

The `is_auto_generated_freshness()` method in `BaseSelector` determines if a selector is auto-generated:

```python
def is_auto_generated_freshness(self, selector_name: str) -> bool:
    freshness_prefix = f"freshness_{self.config.selector_prefix}_"

    # Pattern 1: freshness_{prefix}_* (e.g., "freshness_maestro_model_name")
    if selector_name.startswith(freshness_prefix):
        return True

    # Pattern 2: Special case for independent models
    if selector_name == "freshness_selector_independent":
        return True

    return False
```

This ensures:
1. Auto-generated freshness selectors can be cleanly removed when disabled
2. Custom freshness selectors created by users are never accidentally deleted

### Freshness Selector Whitelist/Blacklist

Fine-grained control over which selectors get freshness variants:

**Whitelist** (`freshness_selector_names`):
- If empty: All selectors get freshness (when `include_freshness_selectors: true`)
- If populated: Only listed selectors get freshness

**Blacklist** (`exclude_freshness_selector_names`):
- Listed selectors NEVER get freshness, even if in whitelist
- Blacklist takes priority over whitelist

```yaml
selector:
  include_freshness_selectors: true
  freshness_selector_names:           # Whitelist (empty = all)
    - maestro_staging
    - maestro_marts
  exclude_freshness_selector_names:   # Blacklist (always excluded)
    - maestro_debug
```

## Configuration

### maestro-maestro-config.yml Structure

```yaml
# Paths
manifest_path: target/manifest.json
selectors_output_file: selectors.yml
jobs_output_file: jobs.yml

# Selector generation
selector:
  exclude_tags: [deprecated, archived]
  exclude_paths: []
  exclude_models: []
  group_by_dependencies: true  # Group models by shared dependencies
  selector_prefix: maestro  # Prefix for auto-generated selectors

  # Freshness selector options
  include_freshness_selectors: false  # Enable freshness selector generation
  freshness_selector_names: []  # Only create freshness for these selectors (whitelist)
  exclude_freshness_selector_names: []  # Never create freshness for these (blacklist)

  # Auto-generated freshness selectors follow pattern: freshness_{selector_prefix}_*
  # Manual freshness selectors (other patterns) are always preserved

  # Seeds selector options
  include_seeds_selectors: false  # Enable seeds selector generation
  seeds_selector_method: path  # 'path' or 'fqn' (both create one {prefix}_seeds selector)
  seeds_path: ""  # Auto-detected if empty

  # Snapshots selector options
  include_snapshots_selectors: false  # Enable snapshots selector generation
  snapshots_selector_method: path  # 'path' or 'fqn' (both create one {prefix}_snapshots selector)
  snapshots_path: ""  # Auto-detected if empty

  # Full refresh selector options
  include_full_refresh_selector: false
  full_refresh_exclude_tags: []
  full_refresh_exclude_paths: []
  full_refresh_exclude_models: []

  # Indirect selection mode for tests (applies to ALL selectors)
  # Options: eager, cautious, buildable, empty
  indirect_selection: eager

  # Single-model selector grouping
  combine_single_model_selectors: false  # Combine orphan models into one selector
  single_model_selector_name: orphan_models  # → {prefix}_orphan_models

# Job definitions
job:
  account_id: 12345
  project_id: 67890
  environment_id: 11111
  cron_schedule: "0 */6 * * *"
  # ... more options
```

## Safety Features

### 1. Manual Selector Protection
- Selectors NOT starting with `maestro_` prefix are preserved
- Won't overwrite custom selector configurations during regeneration

### 2. Version-Controlled Definitions
- All changes in YAML files (version controlled)
- Merge conflicts resolved in git
- Single source of truth for job definitions

## Files Generated

### selectors.yml
```yaml
selectors:
  - name: maestro_stg_customers
    description: Selector for models...
    definition:
      union:
        - method: fqn
          value: stg_customers
```

### jobs.yml
```yaml
jobs:

  dbt_stg_customers:
    account_id: 12345
    dbt_version: null
    deferring_job_definition_id: null
    description: "Job 1 - maestro selector"
    environment_id: 11111
    execute_steps:
      - dbt build --selector maestro_stg_customers
    execution:
      timeout_seconds: 0
    generate_docs: false
    name: dbt-maestro_stg_customers
    project_id: 67890
    run_generate_sources: false
    schedule:
      cron: "0 */6 * * *"
    settings:
      target_name: prod
      threads: 8
    state: 1
    triggers:
      git_provider_webhook: false
      github_webhook: false
      schedule: true
```

### dbt_maestro_dag.py (Airflow)
```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="dbt_maestro_dag",
    default_args=default_args,
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dbt', 'maestro'],
) as dag:

    run_maestro_stg_customers = BashOperator(
        task_id="run_maestro_stg_customers",
        bash_command="dbt build --selector maestro_stg_customers --target prod --threads 8",
    )
```

## Airflow DAG Generation

`AirflowDAGGenerator` (`airflow_dag_generator.py`) renders a standalone Python DAG
file from `selectors.yml`. It has **no runtime dependency on Airflow** - it emits
source code as text, so it runs anywhere maestro runs. Airflow is only needed in
the environment where the DAG is deployed.

### Selector → dbt command mapping
| Selector type (by name) | dbt command |
|-------------------------|-------------|
| default (models) | `dbt build --selector <name>` |
| `*_seeds` | `dbt seed --selector <name>` |
| `*_snapshots` | `dbt snapshot --selector <name>` |
| `*_full_refresh_incremental` | `dbt build --full-refresh --selector <name>` |

Freshness selectors (`freshness_*`, `automatically_generated_freshness_*`) are
filtered out - they have no dbt build step. Each remaining selector becomes one
`BashOperator` with `task_id = run_<selector_name>`.

### Orchestration modes (`airflow.orchestration_mode`)
- **parallel** - no task dependencies; everything runs concurrently.
- **sequential** - each task depends on the previous one in selector list order.
- **dependency** (default) - type-based ordering (seeds → snapshots → models) using
  a rank map, supplemented by cross-selector edges derived from `manifest.json`:
  for each model node, if an upstream dbt node belongs to a *different* selector,
  an edge is added between the owning selectors. Self-edges within a selector are
  ignored. A single upstream renders as `a >> b`; multiple as `[a, b] >> c`.

### dbt runtime flags
`--target` and `--threads` are always appended. `--project-dir` and
`--profiles-dir` are appended only when `dbt_project_dir` / `dbt_profiles_dir`
are configured, keeping commands clean when the worker already runs in the
project directory.

## CLI Commands

```bash
# Create config template (with comments explaining every option)
maestro init --output maestro-config.yml

# Generate selectors from manifest
maestro generate --config maestro-config.yml

# Generate dbt Cloud jobs from selectors
maestro generate-jobs --config maestro-config.yml

# Generate an Airflow DAG from selectors
maestro generate-dags --config maestro-config.yml

# Analyze project
maestro info --manifest target/manifest.json
```

## Dependencies

### Required
- Python >= 3.8
- pyyaml >= 6.0
- click >= 8.0.0

### Optional
- apache-airflow >= 2.5.0 - only needed to *render/validate* generated DAGs in a
  live Airflow environment (`pip install dbt-job-maestro[airflow]`). Generating
  the DAG file itself requires no Airflow install.

## Testing

```bash
# Local testing
dbt list --selector <selector_name>

# Validate config
maestro info --manifest target/manifest.json

# Check generated files
cat selectors.yml
cat jobs.yml
```

## Best Practices

1. **Always use config file** - Consistency across team
2. **Review before merge** - Check diffs in selectors.yml and jobs.yml
3. **Test locally** - Run `dbt list --selector` before committing
4. **Use version control** - Track selectors.yml and jobs.yml in git
