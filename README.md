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
# Basic - FQN-based (analyzes dependencies)
maestro generate --manifest target/manifest.json

# Recommended - Mixed mode (manual + automated)
maestro generate --method mixed

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
| Anything else | Manual | Preserved during regeneration |

**Examples:**
- `maestro_stg_customers` → Auto-generated (replaced on regeneration)
- `critical_revenue` → Manual (preserved on regeneration)
- `my_custom_selector` → Manual (preserved on regeneration)

**That's it!** No special metadata or description prefixes needed - just avoid `maestro_` prefix for your custom selectors.

**Customizable Prefix:** The default prefix is `maestro`, but you can configure it via `selector_prefix` in your config. If you change the prefix, all logic automatically uses the new prefix (e.g., `myprefix_` → auto-generated, anything else → manual).

### Job Management

maestro can generate dbt Cloud job definitions compatible with [dbt-jobs-as-code](https://github.com/dbt-labs/dbt-jobs-as-code):

| Selector Type | Default Job Behavior | Enable Job Generation |
|--------------|---------------------|----------------------|
| `maestro_*` selectors | ❌ **Excluded (explicit opt-in required)** | Set `include_maestro_selectors_in_jobs: true` |
| Manual selectors | ❌ **Excluded (explicit opt-in required)** | Set `include_manual_selectors_in_jobs: true` |

**Why explicit opt-in?**
- **Safety first**: Prevents accidental dbt Cloud job creation via dbt-jobs-as-code
- **Team control**: Orchestration config must be perfect before deployment
- **Flexibility**: Choose exactly which selectors become automated jobs

**Common patterns:**
- **Automated maestro jobs**: Set `include_maestro_selectors_in_jobs: true` (most common)
- **All selectors as jobs**: Set both flags to `true`
- **Manual management only**: Keep both flags `false` (default), manage all jobs in dbt Cloud UI

---

## CLI Usage

maestro provides three main commands:

### `maestro generate`

Generate selectors from manifest.json.

```bash
# Basic usage
maestro generate --manifest target/manifest.json

# Specify generation method
maestro generate --method fqn        # FQN-based (default)
maestro generate --method mixed      # Manual + auto FQN (recommended)
maestro generate --method path       # Path-based
maestro generate --method tag        # Tag-based

# Exclude models/tags/paths
maestro generate --exclude-tag deprecated --exclude-tag archived
maestro generate --exclude-model temp_model

# Use configuration file
maestro generate --config maestro-config.yml

# Mix config file with overrides
maestro generate --config maestro-config.yml --method fqn
```

**Options:**
- `--config, -c`: Path to configuration YAML file
- `--manifest, -m`: Path to manifest.json (default: `target/manifest.json`)
- `--output, -o`: Output file (default: `selectors.yml`)
- `--method`: Generation method (`fqn`, `path`, `tag`, `mixed`)
- `--exclude-tag`: Tags to exclude (can specify multiple)
- `--exclude-model`: Models to exclude (can specify multiple)
- `--min-models`: Minimum models per selector (default: 1)

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

Generates a fully-commented configuration file you can customize.

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
  # Generation method: 'fqn', 'path', 'tag', or 'mixed'
  # - fqn: Group ALL models by dependencies (fully qualified names)
  # - path: Group ALL models by folder structure
  # - tag: Group ALL models by dbt tags
  # - mixed: (RECOMMENDED) Preserve manual selectors + auto-generate FQN for remaining
  method: mixed

  # Group models by shared dependencies (applies to 'fqn' and 'mixed' methods)
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

  # Generate source freshness selectors
  # Creates paired selectors for checking source freshness
  include_freshness_selectors: false

  # Specific selectors to generate freshness for (optional)
  # If empty and include_freshness_selectors=true, ALL selectors get freshness
  # If provided, ONLY these selectors get freshness variants
  freshness_selector_names:
    - maestro_dim_customers
    - critical_revenue  # Works with both maestro and manual selectors

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

  # Minimum models per selector
  min_models_per_selector: 1

  # Prefix for auto-generated selectors (don't change unless you have a reason)
  selector_prefix: maestro

  # Preserve manual selectors during regeneration (always true)
  preserve_manual_selectors: true

  # Overlap detection settings
  warn_on_manual_overlaps: true    # Warn if manual selectors overlap (allowed)
  fail_on_auto_overlaps: true      # Error if auto selectors overlap (bug)

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
  # SELECTOR INCLUSION CONTROL (Explicit Opt-In Required)
  # -------------------------------------------------------------------------

  # Whether to create jobs for auto-generated selectors (maestro_ prefix)
  # ❌ Default: false - EXPLICIT OPT-IN REQUIRED
  # ✅ Set to true to enable dbt-jobs-as-code deployment for auto-generated selectors
  #
  # IMPORTANT: Must explicitly set to true to generate jobs
  include_maestro_selectors_in_jobs: false

  # Whether to create jobs for manual selectors (non-maestro_ prefix)
  # ❌ Default: false - EXPLICIT OPT-IN REQUIRED
  # ✅ Set to true to enable dbt-jobs-as-code deployment for manual selectors
  #
  # IMPORTANT: Must explicitly set to true to generate jobs
  include_manual_selectors_in_jobs: false

  # Why explicit opt-in?
  # - Prevents accidental job creation via dbt-jobs-as-code API
  # - Ensures orchestration config is perfect before deployment
  # - Teams have full control over what becomes automated

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

**Note:** Standalone method only - not used in mixed mode auto-generation.

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

**Note:** Standalone method only - not used in mixed mode auto-generation.

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

### 4. Mixed (⭐ Recommended)

**The most powerful method:** Combines manual customization with automated FQN-based generation.

**Priority order:**
1. **Manual selectors** (HIGHEST) - No `maestro_` prefix
   - Can use ANY method: fqn, tag, or path
   - Preserved exactly during regeneration
   - Models excluded from auto-generation

2. **Auto-generated selectors** (LOWER) - Has `maestro_` prefix
   - Uses ONLY fqn method (dependency analysis)
   - Processes models NOT in manual selectors
   - Replaced during regeneration

```bash
maestro generate --method mixed
```

**How it works:**
- Manual selectors preserved exactly as-is
- Models referenced in manual selectors automatically resolved
- Remaining models grouped by dependencies (FQN method)
- Zero duplicates across all selectors
- Warns if manual selectors overlap (allowed)
- Errors if auto selectors overlap (bug)

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
        - method: tag
          value: revenue_critical

  # MANUAL - using path method (no maestro_ prefix)
  - name: experimental
    description: "Experimental features"
    definition:
      union:
        - method: path
          value: models/marts/experimental

  # AUTO-GENERATED - replaced on regeneration (has maestro_ prefix)
  - name: maestro_stg_customers
    description: Selector for models in component starting with stg_customers
    definition:
      union:
        - method: fqn
          value: stg_customers
        - method: fqn
          value: int_customer_orders
```

**Workflow:**
```bash
# 1. Initial generation
maestro generate --method mixed

# 2. Add your custom selectors (without maestro_ prefix)
#    Edit selectors.yml manually

# 3. Regenerate (manual selectors preserved, auto updated)
maestro generate --method mixed

# 4. Review changes
git diff selectors.yml
```

---

## Job Generation & Orchestration

maestro generates dbt Cloud job definitions compatible with [dbt-jobs-as-code](https://github.com/dbt-labs/dbt-jobs-as-code).

### Job Inclusion (Explicit Opt-In Required)

| Selector Type | Included in jobs.yml? | To Enable |
|--------------|----------------------|-----------|
| `maestro_*` | ❌ **No** (default) | Set `include_maestro_selectors_in_jobs: true` |
| Manual | ❌ **No** (default) | Set `include_manual_selectors_in_jobs: true` |

**Both selector types require explicit opt-in** to prevent accidental job creation.

**Enable job generation in config:**
```yaml
job:
  # Enable job generation for auto-generated selectors (most common)
  include_maestro_selectors_in_jobs: true

  # Enable job generation for manual selectors (optional)
  include_manual_selectors_in_jobs: true

  # Keep both false (default) to manage all jobs manually in dbt Cloud
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
  method: mixed
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

  # EXPLICIT OPT-IN: Enable maestro selectors → jobs
  include_maestro_selectors_in_jobs: true

  # Manual selectors NOT included (manage in dbt Cloud UI)
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

Choose based on your needs (both default to `false` - explicit opt-in required):

**Option A: Hybrid (Recommended)**
- Maestro selectors → Automated jobs ✅
- Manual selectors → dbt Cloud UI ❌
```yaml
job:
  include_maestro_selectors_in_jobs: true   # Explicit opt-in
  include_manual_selectors_in_jobs: false   # Keep default
```

**Option B: Full Automation**
- All selectors → Automated jobs
```yaml
job:
  include_maestro_selectors_in_jobs: true   # Explicit opt-in
  include_manual_selectors_in_jobs: true    # Explicit opt-in
```

**Option C: Manual Management (Default)**
- All jobs managed in dbt Cloud UI
- No job generation via dbt-jobs-as-code
```yaml
job:
  include_maestro_selectors_in_jobs: false  # Default
  include_manual_selectors_in_jobs: false   # Default
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

**Cause:** Job inclusion defaults are `false` - explicit opt-in required

**Fix:** Enable job generation in config:
```yaml
job:
  # For maestro_ selectors (REQUIRED - defaults to false)
  include_maestro_selectors_in_jobs: true

  # For manual selectors (REQUIRED - defaults to false)
  include_manual_selectors_in_jobs: true
```

**Note:** Both settings default to `false` to prevent accidental job creation.

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

### 0.1.0 (2026-01-22)

- Initial release
- Selector generation methods: fqn, path, tag, mixed
- Job generation with orchestration modes
- Configuration file support
- Simple naming convention (maestro_ prefix)
- Job inclusion controls for maestro/manual selectors
- CLI: `generate`, `generate-jobs`, `info`, `init`
