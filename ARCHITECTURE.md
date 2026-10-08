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
4a. maestro generate-jobs → jobs.yml             (dbt Cloud path)
4b. maestro generate-dags → dbt_maestro_*.py     (Airflow path, one DAG per selector)
5. git commit + push

┌─────────────────────────────────────────────────────────────┐
│                    VERSION CONTROL                           │
└─────────────────────────────────────────────────────────────┘

6. Pull request review
7. Merge to main branch

┌──────────────────────────────┐   ┌──────────────────────────────┐
│        DBT CLOUD PATH        │   │         AIRFLOW PATH         │
└──────────────────────────────┘   └──────────────────────────────┘

8a. dbt-jobs-as-code sync          8b. Point dags_dir at (or copy the
    --config jobs.yml                  dbt_maestro_*.py files into) the
                                       Airflow dags/ folder
9a. Jobs deployed to dbt Cloud ✅   9b. DAGs scheduled in Airflow ✅
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
considered **manual selectors** and are always preserved. Automated model selectors are first
constructed in memory from the complete eligible dependency graph. Maestro then removes models
covered by manual selectors from the positive FQN entries before writing the automated selector
definitions. Configuration tag/path/model matches are also removed only after complete grouping,
and are also emitted as explicit exclusion clauses in automated dependency selectors.
Manual ownership alone removes positive FQNs without adding exclusion clauses. Manual definitions and
dedicated full-refresh exclusions remain intact.
This avoids duplicate positive FQN entries without splitting a connected component when a
manually selected model lies between upstream and downstream models; the remaining models stay
together in the same selector.

- `critical_revenue`, `my_custom_selector` → Manual (preserved)
- `maestro_stg_customers` → Auto-generated (replaced on regeneration)

### Model coverage accounting

`selector.exclude_rules` adds validated nested union/intersection exclusion trees
for `config.materialized` and `state` to automated dependency selectors. They
are emitted for dbt runtime evaluation without altering in-memory grouping;
state rules require a previous manifest supplied with dbt's `--state`. Manual
definitions are not changed, and coverage warns about unresolved runtime methods.

After generation, the orchestrator records unique selected, fully excluded, and
unexplained model sets. It reuses the uncovered-model check for the CLI summary.
Fully excluded models come from configuration exclusions or models removed by
selector exclusion clauses, minus all models selected elsewhere. The CLI shows
selected + fully excluded against the manifest model total, and reports gaps and
unsupported resolver methods separately. This does not change generated selectors
or invoke dbt; it is accounting through the existing FQN/tag/path resolver.

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

## selectors.yml shapes dbt accepts

Two spots in the schema are easy to get wrong, and both make dbt reject the **entire**
selectors.yml rather than just the offending selector.

**`exclude` must be a list**, not a dict. The list is implicitly a union, so intersection
semantics need a nested entry:

```yaml
# union mode - exclude anything matching ANY criterion
- exclude:
    - method: tag
      value: deprecated
    - method: path
      value: models/legacy

# intersection mode - exclude only what matches ALL criteria
- exclude:
    - intersection:
        - method: tag
          value: deprecated
        - method: path
          value: models/legacy
```

A dict (`exclude: {union: [...]}`) fails with
`Invalid value for key "exclude". Expected a list.`

**`indirect_selection` belongs on a selector method**, inside `definition`. It is not a
root-level selector key - `default` is reserved for dbt's boolean default-selector flag:

```yaml
definition:
  union:
    - method: fqn
      value: my_model
      indirect_selection: cautious
```

A root-level `default: {indirect_selection: ...}` fails with
`Could not parse selector file data ... Valid root-level selector definitions: union,
intersection, string, dictionary.`

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

### dbt_maestro_<selector>.py (Airflow)
```python
"""
Auto-generated by dbt-job-maestro.
DO NOT EDIT - regenerate with: maestro generate-dags
"""
# Auto-generated by dbt-job-maestro - DO NOT EDIT

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "sla": timedelta(minutes=120),   # only when sla_minutes > 0
}

with DAG(
    dag_id="dbt_maestro_maestro_stg_customers",
    default_args=default_args,
    schedule_interval="0 6 * * *",
    orchestration_mode="simple",
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

`AirflowDAGGenerator` (`airflow_dag_generator.py`) renders standalone Python DAG
files from `selectors.yml`. It has **no runtime dependency on Airflow** - it emits
source code as text, so it runs anywhere maestro runs. Airflow is only needed in
the environment where the DAGs are deployed.

### One DAG file per selector (mirrors dbt Cloud jobs)
Generation mirrors `JobGenerator`: each selector becomes its **own DAG file** (the
Airflow analogue of one dbt Cloud job per selector), so every unit of work gets
its own schedule, SLA, and retry policy. Because a single Airflow DAG has a single
`schedule_interval`, splitting per selector is what makes per-selector schedules
possible. There is intentionally **no cross-selector task wiring** - just like dbt
Cloud jobs, DAGs are independent and ordered via schedules.

### Selector → dbt command mapping
| Selector type (by name) | dbt command |
|-------------------------|-------------|
| default (models) | configurable: `build` (default) / `run` / `run_test` / `test` |
| `*_seeds` | `dbt seed --selector <name>` |
| `*_snapshots` | `dbt snapshot --selector <name>` |
| `*_full_refresh_incremental` | `dbt build --full-refresh --selector <name>` |

Freshness selectors (`freshness_*`, `automatically_generated_freshness_*`) and the
auto full-refresh selector are filtered out of the per-selector pass. Each remaining
selector becomes one DAG with a task whose operator is selected using
`airflow.operator` (`bash` by default, or `dbt` for Astronomer Cosmos local operators),
in a file named `<dag_id_prefix>_<selector_name>.py`. With `run_dbt_deps: true`,
the dependency-install setup task uses `BashOperator`; Cosmos has deprecated its
standalone deps operator.

**Per-selector command** (`default_command` + `selector_commands`) is resolved by the
shared `dbt_commands` module, used by both `JobGenerator` and `AirflowDAGGenerator`:
auto-generated (`maestro_*`) selectors use `default_command` uniformly; `selector_commands`
overrides **manual** selectors only. A selector maps to a *task group* - normally one
task, but `run_test` yields two tasks wired `run >> test` (so the test only runs if the
run succeeds). Seeds/snapshots/full-refresh keep their type-specific command above.

### Schedules & SLA (`airflow.orchestration_mode`)
- **simple** - every maestro DAG uses `schedule_interval`.
- **staggered** - DAGs are offset by `cron_increment_minutes` from
  `start_hour:start_minute`, in creation order (controlled by `execution_order`).
- **none** (default) - `schedule_interval=None`; all generated DAGs are
  externally/manual-triggered, including full-refresh and seed-refresh DAGs.
  Cron settings are optional and ignored in this mode. dbt Cloud jobs likewise
  default to `job.orchestration_mode: none`, with no schedule and a disabled
  schedule trigger. Explicit `simple`/`staggered` enables cron scheduling.

Manual selectors (no `maestro_` prefix) can take a different cadence via
`manual_schedule_interval` (wins only in scheduled modes) and a different SLA via
`manual_sla_minutes` (`-1` inherits `sla_minutes`, `0` disables, `>0` sets it).
SLA is emitted into `default_args` only when the resolved value is `> 0`.

### Combining small selectors (`min_models_per_dag`)
To avoid a battery of DAGs that each run only a model or two, maestro selectors
with fewer than `min_models_per_dag` fqn models are merged into a single
`<dag_id_prefix>_combined_small_selectors.py` DAG, with one chained task group per
selector (mirroring a dbt Cloud job's ordered `execute_steps`). Manual selectors,
seeds, snapshots, and full-refresh DAGs are never combined. The fqn-count logic is
shared with `JobGenerator` via `selector_types.count_fqn_models`.

**Continue-on-failure (Airflow only).** `airflow.continue_on_failure` controls how the
combined DAG's task groups relate: `false` (default) chains them sequentially - the
current fail-fast behaviour; `true` leaves them independent so a failing selector never
skips its siblings (tasks *within* a group, e.g. `run >> test`, are always chained). dbt
Cloud jobs cannot do this - their `execute_steps` are strictly sequential and stop at the
first failure - so the dbt Cloud fallback is `min_models_per_job: 1` (one job per selector).

### Full-refresh DAGs
`airflow.full_refresh` (auto + `custom_schedules`) and `airflow.seeds_full_refresh`
each produce their own DAG file with their own cron, matching the dbt Cloud job
equivalents.

### Idempotency
Every generated file carries the marker `# Auto-generated by dbt-job-maestro`.
`write_dags()` rewrites all current files, **deletes** files that no longer map to a
selector but carry the `Auto-generated by dbt-job-maestro` phrase (stale auto DAGs,
**including legacy single-DAG files** like an old `dbt_maestro_dag.py`), and **never
touches** files without the phrase (hand-written DAGs in the same folder).

### Backward compatibility
The pre-rewrite `airflow.orchestration_mode` values (`parallel`, `sequential`,
`dependency`) wired cross-selector ordering inside one DAG. They are normalized to
`simple` on load, so existing config files keep working without edits - selectors
become independent DAGs scheduled by cron. Unknown modes still raise a clear error.

### Airflow major version (`airflow.airflow_version`)
Airflow 3.0 removed the `schedule_interval` DAG kwarg, removed task SLAs, and moved
`BashOperator` into the standard provider package. `airflow_version` (`2` default, or `3`)
selects the matching output: `schedule=` vs `schedule_interval=`, the provider vs legacy
import, and whether `sla` appears in `default_args`. Version 2 output is byte-identical to
pre-option releases. Targeting Airflow 3 with `airflow_version: 2` makes every DAG fail to
import with `TypeError: DAG.__init__() got an unexpected keyword argument 'schedule_interval'`.

### dbt runtime flags
`--target` and `--threads` are always appended. `--project-dir` and
`--profiles-dir` are appended only when `dbt_project_dir` / `dbt_profiles_dir`
are configured, keeping commands clean when the worker already runs in the
project directory. For `airflow.operator: dbt`, Cosmos receives the project,
profile, target, threads, selector, and full-refresh settings through its local
dbt operators. Set `airflow.dbt_profile` or provide `profile` in the project's
`dbt_project.yml`; install the optional extra with
`pip install 'dbt-job-maestro[airflow-dbt]'` in the Airflow environment.

## CLI Commands

```bash
# Create config template (with comments explaining every option)
maestro init --output maestro-config.yml

# Generate selectors from manifest
maestro generate --config maestro-config.yml

# Generate dbt Cloud jobs from selectors
maestro generate-jobs --config maestro-config.yml

# Regenerate selectors and the local dbt Cloud jobs YAML together (no cloud push)
maestro build --config maestro-config.yml

# Generate Airflow DAGs from selectors (one DAG file per selector)
maestro generate-dags --config maestro-config.yml

# Analyze project
maestro info --manifest target/manifest.json
```

## Possible Future Implementation: Separate Generated-Artifacts Repository

**Proposal only:** this describes a possible CI/CD integration, not functionality
implemented by Maestro. No cross-repository trigger, rename detector, or deployment
workflow is currently provided by this design.

Keep dbt models and Maestro configuration in the source repository, and maintain
generated selectors, dbt Cloud job YAML, and/or Airflow DAGs in a separate
generated-artifacts repository. Merging to the source repository's `main` branch
would trigger generation and open a pull request in the artifacts repository.
Generation and deployment should remain separate, with review before deployment.

### Example Workflow

1. **Source merge:** CI runs after a merge to `main`, records the exact source
   commit SHA, and dispatches an authenticated event to the artifacts repository.
   Limit dispatch credentials to the required repository permissions.
2. **Reproducible checkout:** the artifacts workflow checks out that source SHA
   and the current artifacts branch. Pin Maestro, dbt, and adapter versions.
   Serialize updates per source project so concurrent merges cannot overwrite
   each other's output; reject obsolete results before publishing.
3. **Prepare inputs:** install the dbt dependencies and produce a manifest using
   the source project's configuration. Carry forward user-managed selectors and
   jobs from their designated source of truth; never replace them with an empty
   starting file. Configure generation paths inside an isolated staging directory
   containing the existing owned artifacts.
4. **Generate locally:** run `maestro build --config <config-path>` for selectors
   and job YAML. For Airflow, run `maestro generate --config <config-path>` followed
   by `maestro generate-dags --config <config-path>`. If both outputs are needed,
   build first and then generate DAGs from the same selectors. These commands
   generate files only; generation CI must not push jobs to dbt Cloud.
5. **Validate and reconcile:** validate selector YAML and dbt selection against
   the matching manifest, validate job definitions, and import DAGs in an Airflow
   environment with the configured operator dependencies. Generate a second time
   and require identical output. Compare the candidate artifacts with the
   artifacts repository's current approved revision.
6. **Review:** open or update an artifacts pull request containing generated-file
   diffs, source SHA, tool versions, validation results, and an identity-change
   report. Do not create a commit or pull request when output is unchanged.
7. **Deploy separately:** after approval and merge in the artifacts repository,
   a separately authorized deployment workflow may synchronize jobs or publish
   DAGs. Make selectors available to the runtime dbt project from the same
   approved artifact revision, and ensure the runtime models match the recorded
   source SHA. Publishing DAGs or job YAML alone is insufficient if their
   selector names are missing from the runtime project.

### Airflow-Only Example: Compile, Generate, Publish to S3

For an Airflow-only implementation, no dbt Cloud jobs need to be generated.
The external CI workflow runs `dbt compile` explicitly: Maestro does not compile
the dbt project or refresh its manifest, and `maestro build` is local selector/job
generation, not a dbt compilation command.

An example generation stage, run from the checked-out dbt project:

```bash
# CI installs pinned dbt/adapter/Maestro versions and supplies dbt credentials.
dbt deps
dbt compile --target ci

# Config points manifest_path at target/manifest.json and defines output paths.
maestro generate --config maestro-config.yml
maestro generate-dags --config maestro-config.yml

# CI validation and the identity-change checks above must pass before publishing.
```

`dbt compile` may query the warehouse while compiling macros; use an appropriate
CI profile and credentials. Selectors generated afterward must be included in
the runtime project, not just in the compile-time checkout. If validation needs
a manifest containing the newly generated selector definitions, re-run
`dbt compile --target ci` after generation before that validation.

After review and approval, an example publication stage uploads the validated
release to a dedicated, immutable S3 prefix:

```bash
# Example paths only: CI assembles release/ from the approved artifacts revision.
# SOURCE_SHA is the exact source commit recorded by the generation workflow.
aws s3 cp release/ "s3://example-airflow-artifacts/releases/${SOURCE_SHA}/" --recursive
```

The release should contain generated DAGs, selectors, the matching manifest,
and release metadata identifying source/tool versions. Also provide the matching
dbt project and its runtime dependencies, either in the release or through a
version-pinned worker image/checkout. The bucket upload is a CI responsibility,
not a Maestro command. Use short-lived AWS credentials and permissions scoped to
the designated artifact prefix; do not upload profiles containing credentials,
local virtual environments, or unrelated project files.

Uploading a release does not by itself make Airflow discover its DAGs. A deployment
step must configure or update the runtime's artifact loader to consume the approved
release and place DAGs in its discovery location and selectors in the dbt project.
For managed Airflow services with a fixed S3 DAG prefix, publish approved DAGs to
that configured prefix separately. Reconcile renamed/removed generated DAGs only
within the owned deployment scope after the identity checks; do not blindly sync
with deletion across a shared bucket. Promote only a complete validated release,
and retain the previous release reference for rollback.

### CI/CD Checks for Selector, Job, and DAG Identity Changes

Generated selector names can change as dependency groups or their naming model
change. Because job names and DAG IDs derive from selector names, a selector
rename can appear as a deleted job/DAG plus a newly created one. A file diff alone
must not be treated as proof of a safe rename.

The proposed CI would maintain a versioned inventory of selectors, selected dbt
model unique IDs, generated job keys/names, DAG IDs, and associated source SHA.
Compare old selections using the old manifest and new selections using the new
manifest, rather than resolving both against the latest project.

Required checks and report:

- **Unchanged identity, changed content:** report configuration or model-membership
  updates separately from renames.
- **Likely rename:** an old selector disappears and a new selector selects the
  same model unique-ID set. Report the old/new selector names and the corresponding
  old/new job names and DAG IDs. Treat this as a candidate requiring review, not
  an automatic migration.
- **Added or removed group:** report identities with no counterpart explicitly.
- **Split, merge, or ambiguous match:** report overlapping model sets and require
  review. Do not infer a rename from name similarity or partial overlap alone.
- **Unresolved selection:** if runtime state, selector methods, or exclusions
  prevent reliable model-set comparison, mark reconciliation unverified and block
  automatic identity migration rather than claiming a match.
- **Stale references:** check generated commands, selector references, DAG/task
  dependencies, and any version-controlled external-trigger mappings for removed
  names. AWS trigger targets outside the repository require a separate inventory
  or an explicit owner acknowledgement.

Removing an owned generated file locally does not delete a deployed dbt Cloud
job or an Airflow metadata record. A future deployment workflow must explicitly
plan retirement of old identities, prevent duplicate scheduled execution, update
AWS/API trigger targets, and preserve user-managed resources. Airflow DAG ID
changes also affect historical identity; decide how to retain history and handle
active runs before retiring the old DAG. Require approval for destructive changes
and retain the prior artifacts revision for rollback.

## Dependencies

### Required
- Python >= 3.8
- pyyaml >= 6.0
- click >= 8.0.0

### Optional
- apache-airflow >= 2.5.0 - only needed to *render/validate* generated DAGs in a
  live Airflow environment (`pip install dbt-job-maestro[airflow]`). Generating
  the DAG file itself requires no Airflow install.
- astronomer-cosmos >= 1.4.0 - optional, needed only when
  `airflow.operator: dbt` is selected (`pip install 'dbt-job-maestro[airflow-dbt]'`).
  The Airflow worker also needs dbt Core and the adapter used by the project.

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
