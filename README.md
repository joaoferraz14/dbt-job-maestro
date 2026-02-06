# dbt-job-maestro

**Automatically generate dbt selectors and dbt Cloud jobs from your manifest.json by analyzing dependencies.**

`dbt-job-maestro` creates organized, maintainable dbt selectors and jobs without manual configuration. It analyzes your dbt project's dependency graph and generates definitions that avoid duplicate models, group related models intelligently, and integrate with dbt-jobs-as-code for automated deployment.

---

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Core Concepts](#core-concepts)
  - [Selector Types](#selector-types)
  - [Job Management](#job-management)
- [CLI Usage](#cli-usage)
- [Configuration File Usage](#configuration-file-usage)
- [Selector Generation Methods](#selector-generation-methods)
- [Job Generation & Orchestration](#job-generation--orchestration)
- [CI/CD Integration](#cicd-integration)
- [Advanced Usage](#advanced-usage)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)

---

## Installation

```bash
pip install dbt-job-maestro
```

Or install from source:

```bash
git clone https://github.com/yourusername/dbt-job-maestro.git
cd dbt-job-maestro
pip install -e .
```

---

## Quick Start

### 1. Compile your dbt project

```bash
cd your-dbt-project
dbt compile
```

This generates `target/manifest.json` which maestro analyzes.

### 2. Generate selectors

```bash
# FQN-based (default - analyzes dependencies, groups by components)
maestro generate --manifest target/manifest.json

# Path-based (one selector per directory)
maestro generate --method path

# Tag-based (one selector per tag)
maestro generate --method tag

# Create configuration file for repeatable generation
maestro init --output maestro-config.yml
maestro generate --config maestro-config.yml
```

### 3. Use your selectors

```bash
# Test a selector
dbt list --selector maestro_stg_customers

# Run models
dbt build --selector maestro_stg_customers
```

### 4. (Optional) Generate dbt Cloud jobs

```bash
# Generate jobs.yml for dbt-jobs-as-code
maestro generate-jobs --selectors selectors.yml --output jobs.yml

# Deploy to dbt Cloud
dbt-jobs-as-code sync jobs.yml
```

---

## Core Concepts

### Selector Types

maestro uses a simple naming convention to distinguish selector types:

| Selector Name | Type | Behavior |
|--------------|------|----------|
| `maestro_*` | Auto-generated | Managed by maestro - regenerated on each run |
| `freshness_maestro_*` | Auto-generated freshness | Managed by maestro - regenerated based on `include_freshness_selectors` |
| Anything else | Manual | Preserved during regeneration |

**Examples:**
- `maestro_stg_customers` → Auto-generated (replaced on regeneration)
- `freshness_maestro_stg_customers` → Auto-generated freshness (removed if `include_freshness_selectors: false`)
- `critical_revenue` → Manual (preserved on regeneration)
- `freshness_my_custom` → Manual freshness (preserved on regeneration)
- `my_custom_selector` → Manual (preserved on regeneration)

**That's it!** No special metadata or description prefixes needed - just avoid `maestro_` prefix for your custom selectors.

**Customizable Prefix:** The default prefix is `maestro`, but you can configure it via `selector_prefix` in your config. If you change the prefix, all logic automatically uses the new prefix (e.g., `myprefix_` → auto-generated, anything else → manual).

### Freshness Selector Types

Freshness selectors follow similar conventions:

| Freshness Selector Name | Type | Behavior |
|------------------------|------|----------|
| `freshness_{prefix}_*` | Auto-generated | Created when `include_freshness_selectors: true`, removed when `false` |
| `freshness_selector_independent` | Auto-generated (special) | For independent models, same behavior as above |
| `freshness_*` (other patterns) | Manual | Always preserved during regeneration |

**How auto-generated freshness detection works:**

```
freshness_maestro_dim_customers  → Auto-generated (pattern: freshness_{prefix}_*)
freshness_selector_independent   → Auto-generated (special case for independent models)
freshness_my_critical_models     → Manual (no prefix match, preserved)
freshness_custom_selector        → Manual (no prefix match, preserved)
```

**Key behavior:**
- When `include_freshness_selectors: false` (default), auto-generated freshness selectors are **removed**
- When `include_freshness_selectors: true`, auto-generated freshness selectors are **created**
- Manual freshness selectors (those not matching `freshness_{prefix}_*`) are **always preserved**

### Job Management

maestro can generate dbt Cloud job definitions compatible with [dbt-jobs-as-code](https://github.com/dbt-labs/dbt-jobs-as-code):

| Selector Type | Default Job Behavior | Disable Job Generation |
|--------------|---------------------|----------------------|
| `maestro_*` selectors | ✅ **Included** (default) | Set `include_maestro_selectors_in_jobs: false` |
| Manual selectors | ✅ **Included** (default) | Set `include_manual_selectors_in_jobs: false` |

**Common patterns:**
- **All selectors as jobs**: Keep both flags `true` (default)
- **Maestro jobs only**: Set `include_manual_selectors_in_jobs: false`
- **Manual jobs only**: Set `include_maestro_selectors_in_jobs: false`
- **No jobs generated**: Set both flags to `false`, manage all jobs in dbt Cloud UI

---

## CLI Usage

maestro provides three main commands:

### `maestro generate`

Generate selectors from manifest.json.

```bash
# Basic usage
maestro generate --manifest target/manifest.json

# Specify generation method
maestro generate --method fqn        # FQN-based (default) - groups by dependencies
maestro generate --method path       # Path-based - one selector per directory
maestro generate --method tag        # Tag-based - one selector per tag

# Exclude models/tags/paths
maestro generate --exclude-tag deprecated --exclude-tag archived
maestro generate --exclude-path models/staging/legacy --exclude-path models/temp
maestro generate --exclude-model temp_model --exclude-model debug_model

# Enable or disable freshness selectors
maestro generate --include-freshness    # Generate freshness selectors
maestro generate --no-include-freshness # Skip freshness selectors (default)

# Use configuration file
maestro generate --config maestro-config.yml

# Mix config file with overrides
maestro generate --config maestro-config.yml --method fqn
```

**Options:**
- `--config, -c`: Path to configuration YAML file
- `--manifest, -m`: Path to manifest.json (default: `target/manifest.json`)
- `--output, -o`: Output file (default: `selectors.yml`)
- `--method`: Generation method (`fqn`, `path`, `tag`)
- `--exclude-tag`: Tags to exclude (can specify multiple)
- `--exclude-path`: Paths to exclude (can specify multiple, e.g., `models/staging/legacy`)
- `--exclude-model`: Models to exclude (can specify multiple)
- `--path-level`: Directory level for path grouping (default: 1)
- `--include-freshness/--no-include-freshness`: Enable or disable freshness selector generation (default: disabled)
- `--include-seeds/--no-include-seeds`: Enable or disable seeds selector generation (default: disabled)
- `--seeds-method`: Method to group seeds: `path` (uses path method) or `fqn` (uses fqn for each). Both create ONE selector.
- `--seeds-path`: Path to seeds folder (auto-detected if not specified)
- `--include-snapshots/--no-include-snapshots`: Enable or disable snapshots selector generation (default: disabled)
- `--snapshots-method`: Method to group snapshots: `path` (uses path method) or `fqn` (uses fqn for each). Both create ONE selector.
- `--snapshots-path`: Path to snapshots folder (auto-detected if not specified)

### `maestro generate-jobs`

Generate dbt Cloud jobs from selectors.

```bash
# Basic usage
maestro generate-jobs --selectors selectors.yml --output jobs.yml

# Use configuration file
maestro generate-jobs --config maestro-config.yml

# Override job settings
maestro generate-jobs --config maestro-config.yml --account-id 12345
```

**Options:**
- `--config, -c`: Path to configuration file
- `--selectors, -s`: Path to selectors.yml (default: `selectors.yml`)
- `--output, -o`: Output jobs file (default: `jobs.yml`)
- `--account-id`: dbt Cloud account ID
- `--project-id`: dbt Cloud project ID
- `--environment-id`: dbt Cloud environment ID

### `maestro info`

Analyze your dbt project structure.

```bash
maestro info --manifest target/manifest.json
```

Shows:
- Total models
- Available tags and counts
- Folder structure and counts
- Dependency analysis
- Recommendations for selector methods

### `maestro init`

Create a configuration template.

```bash
maestro init --output maestro-config.yml
```

Generates a fully-commented configuration file with documentation for every option. The generated file includes:
- Explanations of each setting
- Example values
- Recommended defaults
- Section headers for easy navigation

### `maestro check`

Validate deployment requirements before deploying to dbt Cloud.

```bash
# Basic check
maestro check

# Check with config file (uses deployment.deploy_branch setting)
maestro check --config maestro-config.yml

# Check specific dbt project directory
maestro check --dbt-project ./my-dbt-project
```

Checks:
- dbt-jobs-as-code package is installed
- Current git branch matches deployment branch
- packages.yml configuration (optional)
- Required files exist (selectors.yml, jobs.yml)

**Options:**
- `--config, -c`: Path to configuration YAML file
- `--dbt-project, -p`: Path to dbt project directory (default: current directory)

---

## Configuration File Usage

For consistent, repeatable generation, use a configuration file. This is **strongly recommended** for production use.

### Create Configuration

```bash
maestro init --output maestro-config.yml
```

### Complete Configuration Example

Here's a fully-documented configuration file with all options explained:

```yaml
# maestro-config.yml - Complete Configuration Template

# ============================================================================
# FILE PATHS
# ============================================================================
manifest_path: target/manifest.json
output_dir: .
selectors_output_file: selectors.yml
jobs_output_file: jobs.yml

# ============================================================================
# SELECTOR GENERATION
# ============================================================================
selector:
  # Generation method: 'fqn', 'path', or 'tag'
  # - fqn: Group models by dependencies (allows group_by_dependencies)
  # - path: One selector per directory (no dependency grouping)
  # - tag: One selector per tag (no dependency grouping)
  method: fqn

  # Group models by shared dependencies (only valid for 'fqn' method)
  # When true, finds connected components in the dependency graph
  # When false, creates one selector per model
  group_by_dependencies: true

  # -------------------------------------------------------------------------
  # EXCLUSIONS - Models to completely skip
  # -------------------------------------------------------------------------

  # Tags to exclude (models won't be in any selectors)
  exclude_tags:
    - deprecated
    - archived
    - test

  # Specific models to exclude by name
  exclude_models: []

  # Paths to exclude
  exclude_paths:
    - models/staging/legacy
    - models/temp

  # -------------------------------------------------------------------------
  # FRESHNESS SELECTORS
  # -------------------------------------------------------------------------

  # Generate source freshness selectors (default: false)
  # Creates paired selectors for checking source freshness
  include_freshness_selectors: false

  # Specific selectors to generate freshness for (optional whitelist)
  # If empty and include_freshness_selectors=true, ALL selectors get freshness
  # If provided, ONLY these selectors get freshness variants
  freshness_selector_names:
    - maestro_dim_customers
    - critical_revenue  # Works with both maestro and manual selectors

  # Selectors to EXCLUDE from freshness generation (optional blacklist)
  # These selectors will NOT get freshness variants even if:
  # - include_freshness_selectors is true, OR
  # - they're listed in freshness_selector_names
  # Exclusion always takes priority over inclusion
  exclude_freshness_selector_names:
    - maestro_debug
    - manual_testing

  # -------------------------------------------------------------------------
  # SEEDS SELECTORS
  # -------------------------------------------------------------------------

  # Generate selectors for seed files (default: false)
  include_seeds_selectors: false

  # Method to group seeds: 'path' (uses path method) or 'fqn' (uses fqn for each seed)
  # Both methods create ONE selector ({prefix}_seeds) containing all seeds
  seeds_selector_method: path

  # Path to seeds folder (auto-detected if empty)
  seeds_path: ""

  # -------------------------------------------------------------------------
  # SNAPSHOTS SELECTORS
  # -------------------------------------------------------------------------

  # Generate selectors for snapshot files (default: false)
  include_snapshots_selectors: false

  # Method to group snapshots: 'path' (uses path method) or 'fqn' (uses fqn for each snapshot)
  # Both methods create ONE selector ({prefix}_snapshots) containing all snapshots
  snapshots_selector_method: path

  # Path to snapshots folder (auto-detected if empty)
  snapshots_path: ""

  # -------------------------------------------------------------------------
  # FULL REFRESH SELECTOR
  # -------------------------------------------------------------------------

  # Generate a selector for full refresh of incremental models
  # Uses intersection of fqn:* and config.materialized:incremental
  include_full_refresh_selector: false

  # Exclusions from the full refresh selector
  full_refresh_exclude_tags: []
  full_refresh_exclude_paths: []
  full_refresh_exclude_models: []

  # Indirect selection mode for tests: eager, cautious, buildable, or empty
  # - eager: include all tests that touch selected models (default)
  # - cautious: only include tests whose parents are all selected
  # - buildable: include tests that can be built with selected models
  # - empty: exclude all tests
  full_refresh_indirect_selection: eager

  # -------------------------------------------------------------------------
  # ADVANCED OPTIONS
  # -------------------------------------------------------------------------

  # Include parent sources when a model depends on them
  include_parent_sources: true

  # Model prefix order for sorting (leave empty for alphabetical)
  # Examples: ['stg', 'int', 'fct', 'dim'] or ['bronze', 'silver', 'gold']
  prefix_order: []

  # Path grouping level (for path-based selectors)
  # 0=root, 1=first subdirectory, etc.
  path_grouping_level: 1

  # Prefix for auto-generated selectors (don't change unless you have a reason)
  # Manual selectors (those without this prefix) are always preserved
  selector_prefix: maestro

  # Warn when multiple manual selectors cover the same model
  warn_on_manual_overlaps: true

# ============================================================================
# JOB GENERATION (for dbt-jobs-as-code)
# ============================================================================
job:
  # -------------------------------------------------------------------------
  # DBT CLOUD CREDENTIALS
  # -------------------------------------------------------------------------
  account_id: null      # Your dbt Cloud account ID (e.g., 12345)
  project_id: null      # Your dbt Cloud project ID (e.g., 67890)
  environment_id: null  # Your production environment ID (e.g., 11111)

  # -------------------------------------------------------------------------
  # JOB SETTINGS
  # -------------------------------------------------------------------------
  dbt_version: ""           # Leave empty to use environment default
  threads: 8                # Number of threads for dbt execution
  target_name: prod         # Target name (usually 'prod' or 'production')
  timeout_seconds: 0        # Job timeout (0 = no timeout)
  generate_docs: false      # Generate dbt docs
  run_generate_sources: false  # Run source freshness
  job_name_prefix: dbt      # Job names: {prefix}_{selector_name}

  # -------------------------------------------------------------------------
  # SELECTOR INCLUSION CONTROL
  # -------------------------------------------------------------------------

  # Whether to create jobs for auto-generated selectors (maestro_ prefix)
  # ✅ Default: true - jobs are generated for all maestro_ selectors
  # Set to false to exclude auto-generated selectors from job generation
  include_maestro_selectors_in_jobs: true

  # Whether to create jobs for manual selectors (non-maestro_ prefix)
  # ✅ Default: true - jobs are generated for all manual selectors
  # Set to false to exclude manual selectors from job generation
  include_manual_selectors_in_jobs: true

  # Selector prefix for identifying auto-generated selectors
  # Should match selector.selector_prefix (synced automatically from config)
  # Default: "maestro" → selectors starting with "maestro_" are auto-generated
  # If you change selector.selector_prefix, this is automatically updated
  selector_prefix: maestro

  # -------------------------------------------------------------------------
  # ORCHESTRATION MODE
  # -------------------------------------------------------------------------

  # Job orchestration: "simple", "cron_incremental", or "cascade"
  #
  # - simple: All jobs use same cron_schedule (parallel execution)
  # - cron_incremental: Stagger jobs with time increments (6:00, 6:05, 6:10...)
  # - cascade: Chain jobs so each triggers after previous completes
  orchestration_mode: simple

  # For simple mode: cron schedule for all jobs
  cron_schedule: "0 */6 * * *"  # Every 6 hours

  # For cron_incremental and cascade modes:
  start_hour: 6                  # First job hour (0-23)
  start_minute: 0                # First job minute (0-59)
  cron_increment_minutes: 5      # Minutes between jobs (for cron_incremental)
  cron_days_of_week: []          # Empty=every day, or ["MON","TUE","WED","THU","FRI"]

  # -------------------------------------------------------------------------
  # MINIMUM MODELS PER JOB (FQN method only)
  # -------------------------------------------------------------------------

  # Minimum models per job for FQN selectors (default: 1)
  # When set > 1, selectors with fewer models are combined into a single job
  # that runs multiple selectors: dbt build --selector A --selector B ...
  # Only works with method='fqn' - other methods ignore this setting
  min_models_per_job: 1

  # -------------------------------------------------------------------------
  # CASCADE MODE (Two-Phase Deployment)
  # -------------------------------------------------------------------------

  # PHASE 1 - Initial Deployment:
  # Set to true, generate jobs, deploy to dbt Cloud, get job IDs
  cascade_initial_deployment: true

  # PHASE 2 - Add Cascade Triggers:
  # After deploying, set to false and populate job_id_mapping
  # Example:
  # job_id_mapping:
  #   dbt_revenue_critical: 12345
  #   dbt_customer_analytics: 12346
  job_id_mapping: {}

  # -------------------------------------------------------------------------
  # EXECUTION ORDER
  # -------------------------------------------------------------------------

  # Execution order for different resource types during job creation
  # Jobs will be created/ordered in this sequence (first in list runs first)
  # Valid values: 'seeds', 'snapshots', 'models'
  # Empty list = no specific ordering (alphabetical by selector name)
  execution_order:
    - seeds
    - snapshots
    - models

  # -------------------------------------------------------------------------
  # FULL REFRESH JOBS
  # -------------------------------------------------------------------------

  # Configuration for full refresh jobs (for incremental models)
  full_refresh:
    # Enable auto-generated full refresh job for all incremental models
    enabled: false

    # Cron schedule (minute hour day_of_month month day_of_week)
    # Example: "0 0 * * 0" = every Sunday at midnight
    cron_schedule: "0 0 * * 0"

    # Exclusions from the auto-generated full refresh job
    exclude_tags: []
    exclude_paths: []
    exclude_models: []

    # Custom full refresh schedules for specific resources
    # Each entry creates a separate full refresh job
    # custom_schedules:
    #   - name: weekly_customer_refresh
    #     cron_schedule: "0 0 * * 0"        # Every Sunday at midnight
    #     selector: maestro_customers       # Full refresh a selector
    #   - name: monthly_orders_refresh
    #     cron_schedule: "0 0 1 * *"        # First of month at midnight
    #     tags: ['orders', 'billing']       # Full refresh by tags
    #   - name: daily_inventory_refresh
    #     cron_schedule: "0 3 * * *"        # Every day at 3 AM
    #     paths: ['models/staging/inventory']  # Full refresh by path
    #   - name: specific_models_refresh
    #     cron_schedule: "0 6 * * 6"        # Every Saturday at 6 AM
    #     models: ['dim_product', 'fct_inventory']  # Specific models
    custom_schedules: []

# ============================================================================
# DEPLOYMENT
# ============================================================================
deployment:
  deploy_branch: main                    # Branch that triggers deployment
  require_dbt_jobs_as_code: true         # Validate package is installed
  dbt_project_path: .                    # Path to dbt project
```

### Use Configuration

```bash
# Generate selectors using config
maestro generate --config maestro-config.yml

# Generate jobs using config
maestro generate-jobs --config maestro-config.yml

# Override specific settings via CLI
maestro generate --config maestro-config.yml --method fqn
maestro generate-jobs --config maestro-config.yml --account-id 12345
```

---

## Selector Generation Methods

### 1. FQN (Fully Qualified Names) - Default

Groups models by shared dependencies using graph analysis.

**Best for:** Projects with clear dependency chains

```bash
maestro generate --method fqn
```

**Example output:**
```yaml
selectors:
  - name: maestro_stg_customers
    description: Selector for models in component starting with stg_customers
    definition:
      union:
        - method: fqn
          value: stg_customers
        - method: fqn
          value: int_customer_orders
          parents: true
```

### 2. Path

Groups models by folder structure.

**Best for:** Projects organized by folders (staging, marts, etc.)

```bash
maestro generate --method path --path-level 1
```

**Example output:**
```yaml
selectors:
  - name: path_models_staging
    description: Selector for models in models/staging
    definition:
      union:
        - method: path
          value: models/staging
```

### 3. Tag

Groups models by dbt tags.

**Best for:** Projects with comprehensive tagging strategies

**Warning:** Models without tags will NOT be included in any selector. Use `method=fqn` for complete coverage.

```bash
maestro generate --method tag
```

**Example output:**
```yaml
selectors:
  - name: tag_daily
    description: Selector for models tagged with daily
    definition:
      union:
        - method: tag
          value: daily
```

### Manual Selector Preservation

All selector methods preserve manual selectors (those without the `maestro_` prefix).

**How it works:**
- Manual selectors are read from existing `selectors.yml`
- Models covered by manual selectors are excluded from auto-generation
- Zero duplicates across all selectors
- Warns if manual selectors overlap (allowed)

**Example selectors.yml:**
```yaml
selectors:
  # MANUAL - preserved during regeneration (no maestro_ prefix)
  - name: critical_revenue
    description: "Critical revenue models - run first"
    definition:
      union:
        - method: fqn
          value: fct_revenue
          parents: true

  # AUTO-GENERATED - replaced on regeneration (has maestro_ prefix)
  - name: maestro_stg_customers
    description: Selector for models in component starting with stg_customers
    definition:
      union:
        - method: fqn
          value: stg_customers
```

---

## Job Generation & Orchestration

maestro generates dbt Cloud job definitions compatible with [dbt-jobs-as-code](https://github.com/dbt-labs/dbt-jobs-as-code).

### Job Inclusion

| Selector Type | Included in jobs.yml? | To Disable |
|--------------|----------------------|-----------|
| `maestro_*` | ✅ **Yes** (default) | Set `include_maestro_selectors_in_jobs: false` |
| Manual | ✅ **Yes** (default) | Set `include_manual_selectors_in_jobs: false` |

**Customize job generation in config:**
```yaml
job:
  # Default: true - generates jobs for auto-generated selectors
  include_maestro_selectors_in_jobs: true

  # Default: true - generates jobs for manual selectors
  include_manual_selectors_in_jobs: true

  # Set both to false to manage all jobs manually in dbt Cloud
```

### Orchestration Modes

#### Simple Mode (Parallel Execution)

All jobs run at the same time.

```yaml
job:
  orchestration_mode: simple
  cron_schedule: "0 6 * * *"  # 6:00 AM every day
```

**Result:**
```yaml
jobs:
  dbt_maestro_stg_customers:
    schedule: {cron: "0 6 * * *"}
    execute_steps: ["dbt build --selector maestro_stg_customers"]

  dbt_maestro_dim_products:
    schedule: {cron: "0 6 * * *"}
    execute_steps: ["dbt build --selector maestro_dim_products"]
```

**Best for:** Independent models, maximum parallelism

#### Cron Incremental Mode (Staggered Execution)

Jobs staggered with time increments.

```yaml
job:
  orchestration_mode: cron_incremental
  start_hour: 6
  start_minute: 0
  cron_increment_minutes: 5
  cron_days_of_week: ["MON", "TUE", "WED", "THU", "FRI"]
```

**Result:**
```yaml
jobs:
  dbt_maestro_stg_customers:
    schedule: {cron: "0 6 * * 1-5"}  # 6:00 AM

  dbt_maestro_dim_products:
    schedule: {cron: "5 6 * * 1-5"}  # 6:05 AM

  dbt_maestro_fct_orders:
    schedule: {cron: "10 6 * * 1-5"} # 6:10 AM
```

**Best for:** Spreading database load, predictable timing

#### Cascade Mode (Sequential Dependencies)

Jobs trigger when previous completes. **Requires two-phase deployment.**

**Phase 1: Initial Deployment**
```yaml
job:
  orchestration_mode: cascade
  cascade_initial_deployment: true  # All jobs get cron schedules
  start_hour: 6
  start_minute: 0
  cron_days_of_week: ["MON", "TUE", "WED", "THU", "FRI"]
```

```bash
# Generate and deploy
maestro generate-jobs --config maestro-config.yml
dbt-jobs-as-code sync jobs.yml

# Get job IDs from dbt Cloud API
curl -H "Authorization: Token YOUR_TOKEN" \
  "https://cloud.getdbt.com/api/v2/accounts/ACCOUNT_ID/jobs/" | jq
```

**Phase 2: Add Cascade Triggers**
```yaml
job:
  orchestration_mode: cascade
  cascade_initial_deployment: false  # Now use cascade triggers
  job_id_mapping:
    dbt_maestro_stg_customers: 12345
    dbt_maestro_dim_products: 12346
    dbt_maestro_fct_orders: 12347
```

```bash
# Regenerate with cascade triggers
maestro generate-jobs --config maestro-config.yml

# Redeploy
dbt-jobs-as-code sync jobs.yml
```

**Result:**
```yaml
jobs:
  dbt_maestro_stg_customers:
    schedule: {cron: "0 6 * * 1-5"}  # First job: scheduled

  dbt_maestro_dim_products:
    triggers:
      schedule: false
      on_job_completion:
        job_id: 12345  # Triggers after stg_customers
        statuses: ["success", "error", "cancelled"]

  dbt_maestro_fct_orders:
    triggers:
      schedule: false
      on_job_completion:
        job_id: 12346  # Triggers after dim_products
        statuses: ["success", "error", "cancelled"]
```

**Best for:** Guaranteed ordering, resource efficiency

#### Execution Order

Control the order in which different resource types are scheduled in jobs:

```yaml
job:
  execution_order:
    - seeds       # Run seeds first
    - snapshots   # Run snapshots second
    - models      # Run models last
```

**Use case:** Ensures seeds are loaded before snapshots run, and snapshots complete before models that depend on them.

**Note:** This affects job ordering in `cron_incremental` and `cascade` modes. In `simple` mode, all jobs run at the same time regardless of order.

#### Full Refresh Jobs

Automatically generate full refresh jobs for incremental models. Maestro uses a **selector-based approach** that creates a dbt selector using intersection logic:

```yaml
# Generated selector definition
selectors:
  - name: maestro_full_refresh_incremental
    description: Selector for full refresh of all incremental models
    definition:
      union:
        - intersection:
            - method: fqn
              value: "*"
            - method: config.materialized
              value: incremental
        - exclude:
            union:
              - method: tag
                value: no_refresh
    default:
      indirect_selection: cautious  # Only if not "eager" (default)
```

**Configuration (in two parts):**

```yaml
# 1. SELECTOR CONFIGURATION - Controls selector generation
selector:
  # Enable full refresh selector generation
  include_full_refresh_selector: true

  # Exclusions from the full refresh selector
  full_refresh_exclude_tags: ['no_refresh', 'deprecated']
  full_refresh_exclude_paths: ['models/staging/legacy']
  full_refresh_exclude_models: ['dim_temp']

  # Indirect selection mode for tests:
  # - eager: include all tests that touch selected models (default)
  # - cautious: only include tests whose parents are all selected
  # - buildable: include tests that can be built with selected models
  # - empty: exclude all tests
  full_refresh_indirect_selection: eager

# 2. JOB CONFIGURATION - Controls job creation
job:
  full_refresh:
    # Enable auto-generated full refresh job
    enabled: true
    cron_schedule: "0 0 * * 0"  # Every Sunday at midnight

    # Custom full refresh schedules for specific resources
    custom_schedules:
      - name: weekly_customer_refresh
        cron_schedule: "0 0 * * 0"       # Every Sunday
        selector: maestro_customers       # Full refresh a selector

      - name: monthly_orders_refresh
        cron_schedule: "0 0 1 * *"       # First of month
        tags: ['orders', 'billing']       # Full refresh by tags

      - name: daily_inventory_refresh
        cron_schedule: "0 3 * * *"       # Daily at 3 AM
        paths: ['models/staging/inventory']

      - name: specific_models_refresh
        cron_schedule: "0 6 * * 6"       # Every Saturday
        models: ['dim_product', 'fct_inventory']
```

**Cron format:** `minute hour day_of_month month day_of_week` (0=Sunday, 6=Saturday)

**Generated outputs:**
- **Selector:** `maestro_full_refresh_incremental` - Selects all incremental models with exclusions
- **Job:** `dbt_full_refresh_incremental` - Uses `dbt run --full-refresh --selector maestro_full_refresh_incremental`
- **Custom jobs:** `dbt_full_refresh_weekly_customer_refresh`, etc.

#### Seeds Full Refresh Job

Create a job that reloads all seed data using `dbt seed --full-refresh`:

```yaml
job:
  seeds_full_refresh:
    # Enable seeds full refresh job
    enabled: true

    # Cron schedule (minute hour day_of_month month day_of_week)
    cron_schedule: "0 0 * * 0"  # Every Sunday at midnight
```

**Generated job:** `dbt_seeds_full_refresh` - Runs `dbt seed --full-refresh`

### Complete Workflow Example

```bash
# 1. Create configuration
maestro init --output maestro-config.yml

# 2. Edit config (set account_id, project_id, environment_id, etc.)

# 3. Generate selectors
maestro generate --config maestro-config.yml

# 4. (Optional) Manually create custom selectors in selectors.yml
#    Use any name WITHOUT maestro_ prefix

# 5. Generate jobs
maestro generate-jobs --config maestro-config.yml

# 6. Deploy to dbt Cloud
dbt-jobs-as-code sync jobs.yml

# 7. When models change, regenerate
dbt compile
maestro generate --config maestro-config.yml
maestro generate-jobs --config maestro-config.yml
dbt-jobs-as-code sync jobs.yml
```

---

## CI/CD Integration

### GitHub Actions Workflow

Here's a complete GitHub Actions workflow that automates selector and job generation when your dbt project changes:

```yaml
# .github/workflows/maestro-sync.yml
name: Sync dbt Selectors and Jobs

on:
  push:
    branches:
      - main
    paths:
      - 'models/**'
      - 'dbt_project.yml'
      - 'maestro-config.yml'
  workflow_dispatch:  # Allow manual triggers

env:
  DBT_CLOUD_API_TOKEN: ${{ secrets.DBT_CLOUD_API_TOKEN }}

jobs:
  generate-and-deploy:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          pip install dbt-core dbt-postgres  # or your adapter
          pip install dbt-job-maestro
          pip install dbt-jobs-as-code

      - name: Compile dbt project
        run: dbt compile

      - name: Generate selectors
        run: maestro generate --config maestro-config.yml

      - name: Generate jobs
        run: maestro generate-jobs --config maestro-config.yml

      - name: Deploy jobs to dbt Cloud
        run: dbt-jobs-as-code sync jobs.yml
        env:
          DBT_CLOUD_API_TOKEN: ${{ secrets.DBT_CLOUD_API_TOKEN }}

      - name: Commit generated files
        uses: stefanzweifel/git-auto-commit-action@v5
        with:
          commit_message: "chore: update selectors and jobs [skip ci]"
          file_pattern: "selectors.yml jobs.yml"
```

### Workflow Variants

#### PR Preview (No Deploy)

Generate selectors on PRs for review without deploying:

```yaml
# .github/workflows/maestro-preview.yml
name: Preview Selector Changes

on:
  pull_request:
    paths:
      - 'models/**'
      - 'dbt_project.yml'
      - 'maestro-config.yml'

jobs:
  preview:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          pip install dbt-core dbt-postgres
          pip install dbt-job-maestro

      - name: Compile dbt project
        run: dbt compile

      - name: Generate selectors (preview)
        run: maestro generate --config maestro-config.yml

      - name: Show selector diff
        run: git diff selectors.yml || echo "No changes to selectors"

      - name: Upload selectors artifact
        uses: actions/upload-artifact@v4
        with:
          name: selectors-preview
          path: selectors.yml
```

#### Scheduled Sync

Run maestro on a schedule to keep jobs in sync:

```yaml
# .github/workflows/maestro-scheduled.yml
name: Scheduled Maestro Sync

on:
  schedule:
    - cron: '0 6 * * 1'  # Every Monday at 6 AM
  workflow_dispatch:

jobs:
  sync:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          pip install dbt-core dbt-postgres
          pip install dbt-job-maestro
          pip install dbt-jobs-as-code

      - name: Compile and generate
        run: |
          dbt compile
          maestro generate --config maestro-config.yml
          maestro generate-jobs --config maestro-config.yml

      - name: Deploy to dbt Cloud
        run: dbt-jobs-as-code sync jobs.yml
        env:
          DBT_CLOUD_API_TOKEN: ${{ secrets.DBT_CLOUD_API_TOKEN }}

      - name: Create PR if changes
        uses: peter-evans/create-pull-request@v6
        with:
          title: "chore: weekly maestro sync"
          commit-message: "chore: update selectors and jobs"
          branch: maestro-sync
          delete-branch: true
```

### Required Secrets

Configure these secrets in your GitHub repository settings:

| Secret | Description |
|--------|-------------|
| `DBT_CLOUD_API_TOKEN` | Your dbt Cloud API token (Service Account recommended) |

### Tips for CI/CD

1. **Use a config file**: Always use `--config maestro-config.yml` for reproducible builds
2. **Pin versions**: Pin dbt-job-maestro version in requirements.txt
3. **Skip CI on commits**: Use `[skip ci]` in auto-commit messages to avoid infinite loops
4. **Separate preview from deploy**: Use PRs for preview, main branch for deployment
5. **Cache dbt artifacts**: Consider caching `target/` directory for faster runs

---

## Advanced Usage

### Example: E-commerce Project

**Setup:**
- Critical revenue models (manual control)
- Customer analytics (auto-managed)
- Product catalog (auto-managed)
- Experimental features (manual control)

**maestro-config.yml:**
```yaml
manifest_path: target/manifest.json
selectors_output_file: selectors.yml
jobs_output_file: jobs.yml

selector:
  method: fqn
  group_by_dependencies: true
  exclude_tags:
    - deprecated
  include_freshness_selectors: false
  include_parent_sources: true

job:
  account_id: 12345
  project_id: 67890
  environment_id: 11111
  threads: 8
  target_name: prod
  job_name_prefix: dbt
  orchestration_mode: simple
  cron_schedule: "0 6 * * *"

  # Enable maestro selectors → jobs (default: true)
  include_maestro_selectors_in_jobs: true

  # Disable manual selectors from jobs (manage in dbt Cloud UI)
  include_manual_selectors_in_jobs: false
```

**selectors.yml (after generation and manual editing):**
```yaml
selectors:
  # MANUAL - Critical revenue (manual job in dbt Cloud)
  - name: critical_revenue
    description: "Critical revenue models - custom job config in dbt Cloud"
    definition:
      union:
        - method: fqn
          value: fct_revenue
          parents: true
        - method: fqn
          value: fct_subscriptions
          parents: true
        - method: tag
          value: revenue

  # MANUAL - Experimental (manual job in dbt Cloud)
  - name: experimental
    description: "Experimental features - not automated yet"
    definition:
      union:
        - method: path
          value: models/marts/experimental

  # AUTO - Customer analytics (maestro job)
  - name: maestro_dim_customers
    description: Selector for models in component starting with dim_customers
    definition:
      union:
        - method: fqn
          value: dim_customers
        - method: fqn
          value: fct_customer_metrics

  # AUTO - Product catalog (maestro job)
  - name: maestro_dim_products
    description: Selector for models in component starting with dim_products
    definition:
      union:
        - method: fqn
          value: dim_products
        - method: fqn
          value: stg_products
```

**Generate jobs:**
```bash
maestro generate-jobs --config maestro-config.yml
```

**Result (jobs.yml):**
```yaml
jobs:
  # Only maestro selectors included (explicitly opted in via include_maestro_selectors_in_jobs: true)
  dbt_dim_customers:
    account_id: 12345
    project_id: 67890
    environment_id: 11111
    name: dbt-maestro_dim_customers
    schedule: {cron: "0 6 * * *"}
    execute_steps: ["dbt build --selector maestro_dim_customers"]
    settings:
      threads: 8
      target_name: prod

  dbt_dim_products:
    account_id: 12345
    project_id: 67890
    environment_id: 11111
    name: dbt-maestro_dim_products
    schedule: {cron: "0 6 * * *"}
    execute_steps: ["dbt build --selector maestro_dim_products"]
    settings:
      threads: 8
      target_name: prod

  # Manual selectors (critical_revenue, experimental) NOT included
  # Create these manually in dbt Cloud UI with custom settings
```

**Deploy:**
```bash
dbt-jobs-as-code sync jobs.yml
```

**Manual jobs in dbt Cloud UI:**
- Create job "Critical Revenue" using selector `critical_revenue`
  - Custom schedule: Hourly during business hours
  - Custom alerting: Slack channel for failures
  - Higher thread count: 16 threads

- Create job "Experimental" using selector `experimental`
  - Manual trigger only
  - Development environment
  - Different warehouse

### Example: All Selectors as Jobs

If you want ALL selectors (both maestro and manual) as dbt Cloud jobs:

```yaml
job:
  include_maestro_selectors_in_jobs: true   # Maestro selectors → jobs
  include_manual_selectors_in_jobs: true    # Manual selectors → jobs
```

### Example: No Automatic Jobs

If you want to manage ALL jobs manually in dbt Cloud:

```yaml
job:
  include_maestro_selectors_in_jobs: false  # No maestro jobs
  include_manual_selectors_in_jobs: false   # No manual jobs
```

Then manually create jobs in dbt Cloud UI using the generated selectors.

### Example: Combining Small Selectors (FQN only)

If you have many small FQN selectors with only 1-3 models each, you can combine them into a single job to reduce job clutter:

```yaml
selector:
  method: fqn
  group_by_dependencies: true

job:
  min_models_per_job: 4  # Selectors with < 4 models will be combined
```

**Result:**
- Selectors with >= 4 models get their own job: `dbt build --selector maestro_large_component`
- Selectors with < 4 models are combined into one job: `dbt build --selector maestro_small1 --selector maestro_small2 --selector maestro_small3`

**Note:** This feature only works with `method: fqn`. Path and tag methods are not affected.

---

## Best Practices

### 1. Use Configuration Files

Always use config files for production:
```bash
maestro init --output maestro-config.yml
# Edit config
maestro generate --config maestro-config.yml
```

### 2. Version Control

Commit configuration and generated files:
```bash
git add maestro-config.yml selectors.yml jobs.yml
git commit -m "Add maestro configuration and selectors"
```

### 3. Manual Selector Naming

Use descriptive names WITHOUT `maestro_` prefix:
```yaml
# ✅ Good manual selector names
- name: critical_revenue
- name: customer_360
- name: experimental_features

# ❌ Avoid (will be replaced during regeneration)
- name: maestro_my_selector
```

### 4. Job Management Strategy

Choose based on your needs (both default to `true`):

**Option A: Full Automation (Default)**
- All selectors → Automated jobs
```yaml
job:
  include_maestro_selectors_in_jobs: true   # Default
  include_manual_selectors_in_jobs: true    # Default
```

**Option B: Hybrid**
- Maestro selectors → Automated jobs ✅
- Manual selectors → dbt Cloud UI ❌
```yaml
job:
  include_maestro_selectors_in_jobs: true   # Default
  include_manual_selectors_in_jobs: false   # Override default
```

**Option C: Manual Management**
- All jobs managed in dbt Cloud UI
- No job generation via dbt-jobs-as-code
```yaml
job:
  include_maestro_selectors_in_jobs: false  # Override default
  include_manual_selectors_in_jobs: false   # Override default
```

### 5. Regeneration Workflow

```bash
# After model changes
dbt compile
maestro generate --config maestro-config.yml
git diff selectors.yml  # Review changes
maestro generate-jobs --config maestro-config.yml
dbt-jobs-as-code sync jobs.yml
```

### 6. Testing Selectors

Always test before deployment:
```bash
# Test individual selectors
dbt list --selector maestro_stg_customers
dbt list --selector critical_revenue

# Dry run
dbt build --selector maestro_stg_customers --dry-run
```

---

## Troubleshooting

### "No models found"

**Cause:** manifest.json empty or not compiled

**Fix:**
```bash
dbt compile
maestro info --manifest target/manifest.json
```

### "Selector not found"

**Cause:** Selector name mismatch

**Fix:**
```bash
# List all selectors
cat selectors.yml | grep "name:"

# Test selector
dbt list --selector SELECTOR_NAME
```

### "Manual selector models still in auto-generated"

**Cause:** Selector name starts with `maestro_`

**Fix:** Rename selector to NOT start with `maestro_`:
```yaml
# ❌ Wrong - will be replaced
- name: maestro_my_critical_models

# ✅ Correct - will be preserved
- name: my_critical_models
```

### "Jobs not generating"

**Cause:** Job inclusion flags may be set to `false` in config

**Fix:** Check job generation settings in config:
```yaml
job:
  # Default: true - set to false to exclude maestro_ selectors
  include_maestro_selectors_in_jobs: true

  # Default: true - set to false to exclude manual selectors
  include_manual_selectors_in_jobs: true
```

**Note:** Both settings default to `true`. If jobs aren't generating, check your config file for explicit `false` values.

### "Freshness selectors not being removed"

**Cause:** Freshness selectors are being treated as manual selectors

**Fix:** Auto-generated freshness selectors must follow the pattern `freshness_{prefix}_*` to be managed by maestro:

```yaml
# ✅ Auto-generated (will be removed when include_freshness_selectors: false)
- name: freshness_maestro_dim_customers  # Pattern: freshness_{prefix}_*
- name: freshness_selector_independent   # Special case

# ❌ Manual (will be preserved regardless of include_freshness_selectors setting)
- name: freshness_my_custom_selector     # No prefix match
- name: freshness_critical_models        # No prefix match
```

If you have old freshness selectors that don't match the pattern, manually delete them or rename them to match `freshness_{prefix}_*`.

### "Path selector not matching models"

**Cause:** Path prefix mismatch

**Fix:** Try both formats:
```yaml
# With models/ prefix
- method: path
  value: models/staging/customers

# Without models/ prefix
- method: path
  value: staging/customers
```

Both work - maestro normalizes paths automatically.

---

## Contributing

Contributions welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Submit a pull request

---

## License

MIT License - See LICENSE file for details.

---

## Support

- GitHub Issues: [Report bugs or request features](https://github.com/yourusername/dbt-job-maestro/issues)
- Documentation: This README
- dbt-jobs-as-code: [Official documentation](https://github.com/dbt-labs/dbt-jobs-as-code)

---

## Changelog

### 0.3.0 (2026-02-06)

- **New:** Execution order for resource types (`execution_order`) - Control the order in which seeds, snapshots, and models run in cascade/incremental modes
- **New:** Full refresh selector (`include_full_refresh_selector`) - Generates a selector using intersection of `fqn:*` and `config.materialized:incremental`
- **New:** Full refresh jobs (`full_refresh`) - Auto-generate full refresh jobs that use the full refresh selector
- **New:** Custom full refresh schedules - Define specific selectors, tags, paths, or models to full refresh on different cron schedules
- **New:** Full refresh exclusions - Exclude specific tags, paths, or models from the full refresh selector
- **New:** Indirect selection mode (`full_refresh_indirect_selection`) - Control test inclusion with eager, cautious, buildable, or empty modes
- **New:** Seeds full refresh job (`seeds_full_refresh`) - Create a job that runs `dbt seed --full-refresh` on a schedule

### 0.2.0 (2026-02-05)

- **New:** Fully-commented configuration template from `maestro init`
- **New:** Seeds selector generation (`include_seeds_selectors`)
- **New:** Snapshots selector generation (`include_snapshots_selectors`)
- **New:** Freshness selector whitelist/blacklist (`freshness_selector_names`, `exclude_freshness_selector_names`)
- **New:** `maestro check` command for deployment validation
- **Fixed:** Path method no longer creates overlapping selectors for nested directories
- **Improved:** Better path matching - `stage/sap` no longer matches `stage/sap_snpglue`

### 0.1.0 (2026-01-22)

- Initial release
- Selector generation methods: fqn, path, tag
- Job generation with orchestration modes
- Configuration file support
- Simple naming convention (maestro_ prefix)
- Job inclusion controls for maestro/manual selectors
- CLI: `generate`, `generate-jobs`, `info`, `init`
