# dbt-job-maestro

Automatically generate dbt selectors from your `manifest.json` by analyzing model dependencies, folder structures, and tags.

## Overview

`dbt-job-maestro` helps you create organized, maintainable dbt selectors without manual configuration. It analyzes your dbt project's dependency graph and generates selector definitions that:

- **Avoid duplicate models** across selectors
- **Group related models** by shared dependencies, paths, or tags
- **Include source freshness** checks automatically
- **Preserve manual customizations** when regenerating

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

## Quick Start

### 1. Compile your dbt project

```bash
cd your-dbt-project
dbt compile
```

This generates the `target/manifest.json` file that dbt-job-maestro analyzes.

### 2. Generate selectors

```bash
# Basic usage - FQN-based (analyzes dependencies)
dbt-job-maestro generate --manifest target/manifest.json

# Recommended - Mixed mode (manual + automated FQN)
dbt-job-maestro generate --method mixed

# Other methods
dbt-job-maestro generate --method path  # Group by folder structure
dbt-job-maestro generate --method tag   # Group by tags

# Exclude specific tags
dbt-job-maestro generate --exclude-tag deprecated --exclude-tag archived
```

### 3. Review and use

The generated `selectors.yml` file is ready to use:

```bash
# Test a selector
dbt list --selector automatically_generated_selector_stg_customers

# Run models with a selector
dbt build --selector automatically_generated_selector_stg_customers
```

## Selector Generation Methods

dbt-job-maestro supports four generation methods:

| Method | Type | Auto-Generation Method | Use Case |
|--------|------|----------------------|----------|
| **fqn** | Standalone | FQN (dependency-based) | Projects with clear dependency chains |
| **path** | Standalone | Path (folder structure) | Projects organized by folders |
| **tag** | Standalone | Tag (dbt tags) | Projects with comprehensive tagging |
| **mixed** | Hybrid ⭐ | FQN only (for auto) | Manual selectors (any method) + automated FQN |

**Key Difference:**
- **Standalone methods** (`fqn`, `path`, `tag`): Generate selectors for ALL models using that method
- **Mixed method** (⭐ recommended):
  - Manual selectors can use **any method** (fqn/tag/path) and are preserved
  - Auto-generated selectors use **only FQN method**
  - Models in manual selectors are excluded from auto-generation

---

### FQN (Fully Qualified Names) - Default

Groups models by shared dependencies using graph analysis.

**Best for:** Projects where models have clear dependency chains

```bash
dbt-job-maestro generate --method fqn
```

**Example output:**
```yaml
selectors:
  - name: automatically_generated_selector_stg_customers
    description: Selector for models in component starting with stg_customers
    definition:
      union:
        - method: fqn
          value: stg_customers
        - method: fqn
          value: fct_orders
          parents: true
```

### Path-based

Groups models by their folder structure.

**Best for:** Projects organized by folder (staging, marts, etc.)

**Note:** This is a standalone method (not used in mixed mode auto-generation). For mixed mode, only manual selectors can use path method.

```bash
dbt-job-maestro generate --method path --path-level 1
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

### Tag-based

Groups models by dbt tags.

**Best for:** Projects with comprehensive tagging strategies

**Note:** This is a standalone method (not used in mixed mode auto-generation). For mixed mode, only manual selectors can use tag method.

```bash
dbt-job-maestro generate --method tag
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

### Mixed (Recommended)

**The most powerful method:** Combines manual customization with automated FQN-based generation, ensuring NO duplicate models across selectors.

**Priority order:**
1. **Manually created selectors** (HIGHEST PRIORITY) - Preserved from existing file
   - Identified by `manually_created_` prefix in name
   - Can use ANY method: fqn, tag, or path
   - Models from these selectors are excluded from auto-generation
2. **FQN-based auto-generated selectors** (LOWER PRIORITY) - Created automatically
   - Uses ONLY fqn method (dependency analysis)
   - Only processes models NOT in manual selectors

**Best for:** Most production projects that need both custom control and automated maintenance

```bash
dbt-job-maestro generate --method mixed
```

**How it works:**
- **Manual selectors** can use ANY method (fqn, tag, or path) and are preserved exactly as-is
- **Auto-generated selectors** ONLY use FQN method (dependency-based grouping)
- Models referenced in manual selectors are automatically resolved and excluded from auto-generation
- Overlap detection warns if models appear in multiple manual selectors (allowed)
- Overlap detection errors if models appear in multiple auto-generated selectors (bug)

**Important:** Path and tag methods are available for manual selectors only. Auto-generated selectors use FQN exclusively.

## Configuration Files

Create a configuration file for consistent, repeatable selector generation:

### 1. Initialize config

```bash
dbt-job-maestro init --output myproject-config.yml
```

### 2. Customize settings

Edit `myproject-config.yml`:

```yaml
manifest_path: target/manifest.json
output_dir: .
selectors_output_file: selectors.yml

selector:
  method: fqn
  group_by_dependencies: true

  # Exclude specific tags (e.g., deprecated, experimental)
  exclude_tags:
    - deprecated
    - archived

  # Exclude specific models
  exclude_models:
    - temp_model

  # Exclude paths
  exclude_paths:
    - models/staging/legacy

  include_freshness_selectors: true

  # Optional: Specific selectors to generate freshness for (if empty, all selectors get freshness when include_freshness_selectors is true)
  # Example: Only create freshness selectors for critical models
  freshness_selector_names:
    - automatically_generated_selector_dim_customers
    - manually_created_revenue_critical

  include_parent_sources: true

  # Optional: Model prefix order for sorting (leave empty [] for alphabetical)
  # Examples: ['bronze', 'silver', 'gold'] or ['raw', 'staging', 'marts']
  prefix_order: []

  path_grouping_level: 1
  min_models_per_selector: 1
  selector_prefix: automatically_generated_selector
  preserve_manual_selectors: true
```

### 3. Generate with config

```bash
dbt-job-maestro generate --config myproject-config.yml
```

See [examples/config.yml](examples/config.yml) for a fully documented configuration template.

## CLI Commands

### `generate`

Generate selectors from manifest.json

```bash
# Using config file
dbt-job-maestro generate --config config.yml

# Using command line options
dbt-job-maestro generate --manifest target/manifest.json --method fqn --exclude-tag deprecated

# Mix both (CLI options override config)
dbt-job-maestro generate --config config.yml --method path
```

**Options:**
- `--config, -c`: Path to configuration YAML file
- `--manifest, -m`: Path to manifest.json
- `--output, -o`: Output file for selectors
- `--method, -t`: Selector generation method (`fqn`, `path`, `tag`, `mixed`)
- `--group-by-dependencies`: Group models by shared dependencies
- `--exclude-tag`: Tags to exclude (repeatable)
- `--path-level`: Directory level for path grouping
- `--min-models`: Minimum models per selector
- `--no-freshness`: Disable freshness selector generation

### `info`

Analyze your dbt project structure

```bash
dbt-job-maestro info --manifest target/manifest.json
```

Shows:
- Total number of models
- Available tags and model counts
- Folder structure and model counts
- Dependency analysis
- Recommendations for selector methods

### `init`

Create a configuration file template

```bash
dbt-job-maestro init --output config.yml
```

## Creating dbt Cloud Jobs

`dbt-job-maestro` can generate job definitions with intelligent orchestration. Configure via `jobs.yml`:

### Job Orchestration Modes

#### 1. Simple Mode (Default)
All jobs use the same cron schedule. Best for independent jobs that can run in parallel.

```yaml
job:
  orchestration_mode: simple
  cron_schedule: "0 6 * * *"  # All jobs run at 6:00 AM
```

#### 2. Cron Incremental Mode
Stagger jobs with time increments to spread load across time while keeping predictable schedules.

```yaml
job:
  orchestration_mode: cron_incremental
  start_hour: 6          # First job at 6:00 AM
  start_minute: 0
  cron_increment_minutes: 5  # Each subsequent job +5 minutes
  cron_days_of_week: ["MON", "TUE", "WED", "THU", "FRI"]  # Weekdays only
```

**Result:**
- Job 1: `0 6 * * 1-5` (6:00 AM weekdays)
- Job 2: `5 6 * * 1-5` (6:05 AM weekdays)
- Job 3: `10 6 * * 1-5` (6:10 AM weekdays)
- Job 4: `15 6 * * 1-5` (6:15 AM weekdays)

**Use cases:**
- Spread database load across time
- Avoid resource contention
- Maintain predictable schedules for monitoring

#### 3. Cascade Mode (Two-Phase Deployment)
Chain jobs so each triggers after the previous completes (any status: success, error, or cancelled).

**Important:** Cascade mode requires a two-phase deployment because job IDs are only available AFTER creating jobs in dbt Cloud.

##### Phase 1: Initial Deployment

```yaml
# config.yml
job:
  orchestration_mode: cascade
  start_hour: 6
  start_minute: 0
  cron_days_of_week: ["MON", "TUE", "WED", "THU", "FRI"]
  cascade_initial_deployment: true  # Generate all jobs with cron schedules
```

```bash
# Generate jobs (all will have cron schedules initially)
dbt-job-maestro generate --config config.yml

# Deploy to dbt Cloud
dbt-jobs-as-code create --config dbt-cloud-config.yml

# Get job IDs from dbt Cloud API (Option 1: Using helper script)
python scripts/fetch_job_ids.py --account-id 12345 --project-id 67890

# Or Option 2: Using curl + jq
curl -H "Authorization: Token $DBT_API_TOKEN" \
  https://cloud.getdbt.com/api/v2/accounts/{account_id}/jobs/ \
  | jq '.data[] | {name: .name, id: .id}'
```

**Output example from helper script:**
```yaml
# Add this to your config.yml under job:
job_id_mapping:
  dbt_revenue_critical: 12345
  dbt_customer_analytics: 12346
  dbt_product_metrics: 12347
```

##### Phase 2: Add Cascade Triggers

Update config with job IDs and regenerate:

```yaml
# config.yml
job:
  orchestration_mode: cascade
  cascade_initial_deployment: false  # Now add cascade triggers
  job_id_mapping:
    dbt_revenue_critical: 12345
    dbt_customer_analytics: 12346
    dbt_product_metrics: 12347
    dbt_reporting: 12348
```

```bash
# Regenerate jobs (now with cascade triggers using job IDs)
dbt-job-maestro generate --config config.yml

# Redeploy to dbt Cloud to activate cascading
dbt-jobs-as-code update --config dbt-cloud-config.yml
```

**Result:**
- Job 1 (dbt_revenue_critical): Scheduled at 6:00 AM weekdays
- Job 2 (dbt_customer_analytics): Triggers when job ID 12345 completes (any status)
- Job 3 (dbt_product_metrics): Triggers when job ID 12346 completes (any status)
- Job 4 (dbt_reporting): Triggers when job ID 12347 completes (any status)

**Use cases:**
- Sequential dependencies where order matters
- Guaranteed ordering of model builds
- Resource-constrained environments (one job at a time)

### Deploying Jobs to dbt Cloud

Use the **[dbt-jobs-as-code](https://pypi.org/project/dbt-jobs-as-code/)** package to deploy generated jobs:

```bash
# Install dbt-jobs-as-code
pip install dbt-jobs-as-code

# Create jobs from selectors via dbt Cloud API
dbt-jobs-as-code create --selectors selectors.yml --config dbt-cloud-config.yml
```

This separation of concerns allows:
- **dbt-job-maestro**: Focus on selector logic and job orchestration configuration
- **dbt-jobs-as-code**: Handle dbt Cloud API integration and job deployment

### Automated Cascade Deployment (GitHub Actions)

For cascade mode, this repository includes a complete GitHub Actions workflow that automates the two-phase deployment process.

**Working Example:** [.github/workflows/deploy-dbt-jobs.yml](.github/workflows/deploy-dbt-jobs.yml)

The workflow automatically:
- ✅ Detects which deployment phase you're in (initial or cascade)
- ✅ Deploys jobs to dbt Cloud using dbt-jobs-as-code
- ✅ Fetches job IDs from dbt Cloud API (Phase 1)
- ✅ Updates config.yml with job IDs (Phase 1)
- ✅ Regenerates jobs with cascade triggers (Phase 1)
- ✅ Redeploys with cascade configuration (Phase 1)
- ✅ Commits updated config back to repo with `[skip ci]` (Phase 1)
- ✅ Handles subsequent deployments automatically (Phase 2+)

**Required GitHub Secrets:**
- `DBT_CLOUD_SERVICE_TOKEN`: dbt Cloud service token for deploying jobs
- `DBT_API_TOKEN`: dbt Cloud API token for fetching job IDs
- `GITHUB_TOKEN`: Automatically provided for committing config updates

**How it works:**
1. On push to main (when jobs.yml, selectors.yml, or config.yml changes)
2. Workflow checks if `orchestration_mode: cascade` and `job_id_mapping` is empty
3. **Phase 1** (if cascade + empty mapping):
   - Deploys jobs with cron schedules
   - Fetches job IDs from dbt Cloud API
   - Updates config.yml with `job_id_mapping` and sets `cascade_initial_deployment: false`
   - Regenerates jobs.yml with cascade triggers
   - Redeploys jobs with cascade triggers
   - Commits updated config with `[skip ci]`
4. **Phase 2+** (subsequent pushes):
   - Simply deploys the jobs (already configured with cascade triggers)

**One-Time Setup:**
1. Add the required secrets to your GitHub repository settings
2. Set `orchestration_mode: cascade` in your config.yml
3. Ensure `cascade_initial_deployment: true` and `job_id_mapping: {}`
4. Push to main - the workflow handles everything automatically!

See [.github/workflows/deploy-dbt-jobs.yml](.github/workflows/deploy-dbt-jobs.yml) for the complete implementation.

## Advanced Usage

### Programmatic API

Use dbt-job-maestro as a Python library:

```python
from dbt_job_maestro import ManifestParser, GraphBuilder
from dbt_job_maestro.selector_orchestrator import SelectorOrchestrator
from dbt_job_maestro.config import Config

# Load configuration
config = Config.from_yaml("config.yml")

# Parse manifest
parser = ManifestParser(config.manifest_path)
models = parser.get_models()

# Build dependency graph
graph = GraphBuilder(models)

# Generate selectors with priority-based orchestrator
orchestrator = SelectorOrchestrator(parser, graph, config.selector)
selectors = orchestrator.generate_selectors()

# Write to file
orchestrator.write_selectors(selectors, config.selectors_output_file)
```

**For backward compatibility with legacy code:**

```python
from dbt_job_maestro import SelectorGenerator

# Old API still works for path and tag methods
generator = SelectorGenerator(parser, graph, config.selector)
selectors = generator.generate_selectors()
```

See [examples/example_usage.py](examples/example_usage.py) for more examples.

### Excluding Models and Tags

Exclude specific models, tags, or paths from selector generation:

```yaml
selector:
  exclude_tags:
    - deprecated
    - experimental
    - manual_only

  exclude_models:
    - temp_*
    - test_model

  exclude_paths:
    - models/staging/legacy
    - models/deprecated
```

### Creating Manual Selectors

You can create custom selectors that will be preserved during regeneration. This is especially useful with the `mixed` method, where manual selectors get **highest priority** and their models are excluded from automated grouping.

**How to mark a selector as manual:**

**Option 1 (Recommended)**: Use `manually_created_` prefix in the selector name:

```yaml
selectors:
  # Manual selector - PRESERVED during regeneration
  - name: manually_created_critical_revenue
    description: "Critical revenue tracking models"
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

  # Manual selector using path
  - name: manually_created_experimental
    description: "Experimental features under development"
    definition:
      union:
        - method: path
          value: models/marts/experimental

  # Auto-generated selector - REPLACED during regeneration
  - name: automatically_generated_selector_stg_customers
    description: Selector for models in component starting with stg_customers
    definition:
      union:
        - method: fqn
          value: stg_customers
```

**Option 2 (Alternative)**: Add metadata field:

```yaml
selectors:
  - name: critical_revenue
    metadata:
      manually_created: true
    description: "Critical revenue tracking models"
    definition:
      union:
        - method: fqn
          value: fct_revenue
```

**Option 3 (Legacy)**: Include `manually_created` in the description (backward compatibility only)

**With mixed mode:**

```yaml
selector:
  method: mixed
  preserve_manual_selectors: true  # Enable manual selector preservation (default: true)
  warn_on_manual_overlaps: true    # Warn if models appear in multiple manual selectors (default: true)
  fail_on_auto_overlaps: true      # Error if auto-generated selectors overlap (default: true)
```

When regenerating with `mixed` method, models in manual selectors are **automatically resolved** and **excluded** from FQN-based auto-generation, ensuring no duplicates.

**Smart Model Resolution:** Manual selectors can use **any method** (fqn, tag, path), and all referenced models are automatically resolved and excluded from auto-generated selectors:

```yaml
selectors:
  - name: manually_created_critical_pipeline
    description: "Critical revenue pipeline"
    definition:
      union:
        - method: fqn
          value: fct_revenue         # Direct model reference
        - method: tag
          value: critical            # All models with "critical" tag
        - method: path
          value: models/marts/revenue  # All models in revenue path
```

**All models** from these definitions (fqn, tag, and path) are resolved and excluded from auto-generated selectors.

**Overlap Detection:**
- **Manual selector overlaps**: Allowed with WARNING (useful for cross-cutting concerns)
- **Auto-generated overlaps**: Reported as ERROR (indicates a bug in deduplication)
- **Manual + Auto overlaps**: Reported as ERROR (indicates improper exclusion)

See [examples/selectors_with_manual.yml](examples/selectors_with_manual.yml) for a complete example.

### Complete Workflow Example

Here's a complete workflow showing how to create manual selectors and regenerate:

#### Step 1: Initial Setup (First Time)

```bash
# Compile your dbt project
cd your-dbt-project
dbt compile

# Generate initial selectors
dbt-job-maestro generate --manifest target/manifest.json --method mixed
```

This creates `selectors.yml` with auto-generated FQN-based selectors.

#### Step 2: Create Manual Selectors

Edit `selectors.yml` to add your custom selectors with the `manually_created_` prefix:

```yaml
selectors:
  # Manual selector 1: Critical revenue models (highest priority)
  - name: manually_created_critical_revenue
    description: "Critical revenue models that run first"
    definition:
      union:
        - method: fqn
          value: fct_revenue
          parents: true
        - method: fqn
          value: fct_subscriptions
          parents: true
        - method: tag
          value: critical

  # Manual selector 2: Legacy models that need special handling
  - name: manually_created_legacy
    description: "Legacy staging models with custom logic"
    definition:
      union:
        - method: path
          value: models/staging/legacy

  # Manual selector 3: Experimental features (can overlap with others)
  - name: manually_created_experimental
    description: "Experimental features under development"
    definition:
      union:
        - method: tag
          value: experimental

  # Auto-generated selectors below (will be regenerated)
  - name: automatically_generated_selector_stg_customers
    description: "Selector for models in component starting with stg_customers"
    definition:
      union:
        - method: fqn
          value: stg_customers
        - method: fqn
          value: stg_orders
```

#### Step 3: Regenerate Selectors

When you add/remove models or change dependencies:

```bash
# Recompile dbt project
dbt compile

# Regenerate selectors
dbt-job-maestro generate --manifest target/manifest.json --method mixed

# Check what changed
git diff selectors.yml
```

**What happens:**
- ✅ Manual selectors (`manually_created_*`) are **preserved exactly**
- ✅ Models from manual selectors are **automatically excluded** from auto-generated selectors
- ✅ Auto-generated selectors are **regenerated** based on new dependencies
- ✅ Warnings shown if manual selectors have overlapping models

#### Step 4: Test Your Selectors

```bash
# Test individual selectors
dbt list --selector manually_created_critical_revenue
dbt list --selector automatically_generated_selector_stg_customers

# Run models with a selector
dbt build --selector manually_created_critical_revenue

# Run specific selectors in order
dbt build --selector manually_created_critical_revenue
dbt build --selector automatically_generated_selector_stg_customers
```

#### Step 5: Ongoing Maintenance

Every time you modify your dbt models:

```bash
# 1. Make changes to your dbt models
# 2. Compile
dbt compile

# 3. Regenerate selectors (manual ones preserved)
dbt-job-maestro generate --manifest target/manifest.json --method mixed

# 4. Review changes
git diff selectors.yml

# 5. Commit if satisfied
git add selectors.yml
git commit -m "Update selectors after model changes"
```

### Real-World Example: E-commerce Project

```yaml
selectors:
  # Manual: Critical daily revenue pipeline (runs first, highest priority)
  - name: manually_created_revenue_critical
    description: "Daily revenue models - run first every day"
    definition:
      union:
        - method: fqn
          value: fct_orders
          parents: true
        - method: fqn
          value: fct_revenue
          parents: true
        - method: tag
          value: revenue_critical

  # Manual: Customer 360 models (cross-cutting concern, may overlap)
  - name: manually_created_customer_360
    description: "Customer analytics models"
    definition:
      union:
        - method: path
          value: models/marts/customer
        - method: tag
          value: customer_facing

  # Manual: Legacy models that need special configuration
  - name: manually_created_legacy_migration
    description: "Legacy models being migrated - custom warehouse"
    definition:
      union:
        - method: path
          value: models/staging/legacy
        - method: path
          value: models/marts/legacy

  # Auto-generated: Staging area models (auto-managed by dependencies)
  - name: automatically_generated_selector_stg_orders
    description: "Selector for models in component starting with stg_orders"
    definition:
      union:
        - method: fqn
          value: stg_orders
        - method: fqn
          value: stg_order_items

  # Auto-generated: Product analytics models
  - name: automatically_generated_selector_dim_products
    description: "Selector for models in component starting with dim_products"
    definition:
      union:
        - method: fqn
          value: dim_products
        - method: fqn
          value: fct_product_metrics
```

**Running this project:**

```bash
# Run critical revenue models first (every morning)
dbt build --selector manually_created_revenue_critical

# Run other auto-generated selectors in parallel
dbt build --selector automatically_generated_selector_stg_orders &
dbt build --selector automatically_generated_selector_dim_products &

# Or use dbt Cloud jobs to orchestrate
```

### Configuring Freshness Selectors

Freshness selectors automatically check source data freshness before running dependent models. You can control which selectors get freshness variants:

**Option 1: Enable freshness for all selectors (default)**

```yaml
selector:
  method: mixed
  include_freshness_selectors: true  # All selectors get freshness variants
```

This generates:
- `manually_created_revenue_critical` → `freshness_manually_created_revenue_critical`
- `automatically_generated_selector_stg_orders` → `freshness_automatically_generated_selector_stg_orders`

**Option 2: Disable freshness globally**

```yaml
selector:
  method: mixed
  include_freshness_selectors: false  # No freshness selectors generated
```

**Option 3: Enable freshness for specific selectors only**

```yaml
selector:
  method: mixed
  include_freshness_selectors: true

  # Only these selectors will have freshness variants
  freshness_selector_names:
    - manually_created_revenue_critical
    - automatically_generated_selector_stg_orders
```

This generates freshness variants ONLY for:
- `freshness_manually_created_revenue_critical`
- `freshness_automatically_generated_selector_stg_orders`

But NOT for other selectors like `automatically_generated_selector_dim_products`.

**When to use selective freshness:**
- Production-critical selectors that depend on external sources
- Hourly/real-time pipelines that need fresh data
- Selectors that don't need freshness checks (pure transformations) can be excluded

**Example usage:**

```bash
# Run freshness check, then build critical revenue models
dbt build --selector freshness_manually_created_revenue_critical

# Build models without freshness (for selectors without source dependencies)
dbt build --selector automatically_generated_selector_dim_products
```

### Job Orchestration Example

Here's a complete example showing how to configure job orchestration for different scenarios:

**Scenario:** E-commerce project with 4 job groups:
1. Critical revenue models (must run first)
2. Customer analytics
3. Product metrics
4. Reporting aggregations

#### Option A: Cron Incremental (Spread Load)

```yaml
# config.yml
job:
  orchestration_mode: cron_incremental
  start_hour: 6
  start_minute: 0
  cron_increment_minutes: 15  # 15-minute increments
  cron_days_of_week: ["MON", "TUE", "WED", "THU", "FRI"]
```

**Generated jobs.yml:**
```yaml
jobs:
  dbt_revenue_critical:
    schedule: {cron: "0 6 * * 1-5"}    # 6:00 AM
    execute_steps: ["dbt build --selector manually_created_revenue_critical"]

  dbt_customer_analytics:
    schedule: {cron: "15 6 * * 1-5"}   # 6:15 AM
    execute_steps: ["dbt build --selector automatically_generated_selector_customer"]

  dbt_product_metrics:
    schedule: {cron: "30 6 * * 1-5"}   # 6:30 AM
    execute_steps: ["dbt build --selector automatically_generated_selector_product"]

  dbt_reporting:
    schedule: {cron: "45 6 * * 1-5"}   # 6:45 AM
    execute_steps: ["dbt build --selector automatically_generated_selector_reporting"]
```

**Benefits:** Spreads database load, predictable timing for monitoring

#### Option B: Cascade (Sequential Dependencies)

**Note:** Requires two-phase deployment (see Cascade Mode section for details)

```yaml
# config.yml (Phase 2 - after getting job IDs from dbt Cloud)
job:
  orchestration_mode: cascade
  start_hour: 6
  start_minute: 0
  cron_days_of_week: ["MON", "TUE", "WED", "THU", "FRI"]
  cascade_initial_deployment: false
  job_id_mapping:
    dbt_revenue_critical: 12345
    dbt_customer_analytics: 12346
    dbt_product_metrics: 12347
```

**Generated jobs.yml (after Phase 2):**
```yaml
jobs:
  dbt_revenue_critical:
    schedule: {cron: "0 6 * * 1-5"}    # Starts at 6:00 AM
    triggers: {schedule: true}
    execute_steps: ["dbt build --selector manually_created_revenue_critical"]

  dbt_customer_analytics:
    triggers:
      schedule: false
      on_job_completion:
        job_id: 12345  # References revenue job by ID
        statuses: ["success", "error", "cancelled"]
    execute_steps: ["dbt build --selector automatically_generated_selector_customer"]

  dbt_product_metrics:
    triggers:
      schedule: false
      on_job_completion:
        job_id: 12346  # References customer job by ID
        statuses: ["success", "error", "cancelled"]
    execute_steps: ["dbt build --selector automatically_generated_selector_product"]

  dbt_reporting:
    triggers:
      schedule: false
      on_job_completion:
        job_id: 12347  # References product job by ID
        statuses: ["success", "error", "cancelled"]
    execute_steps: ["dbt build --selector automatically_generated_selector_reporting"]
```

**Benefits:** Guaranteed ordering, resource efficiency (one job at a time), handles failures gracefully

#### Option C: Simple (Parallel Execution)

```yaml
# config.yml
job:
  orchestration_mode: simple
  cron_schedule: "0 6 * * 1-5"  # All jobs at same time
```

**Generated jobs.yml:**
```yaml
jobs:
  dbt_revenue_critical:
    schedule: {cron: "0 6 * * 1-5"}   # All start at 6:00 AM
    execute_steps: ["dbt build --selector manually_created_revenue_critical"]

  dbt_customer_analytics:
    schedule: {cron: "0 6 * * 1-5"}
    execute_steps: ["dbt build --selector automatically_generated_selector_customer"]

  dbt_product_metrics:
    schedule: {cron: "0 6 * * 1-5"}
    execute_steps: ["dbt build --selector automatically_generated_selector_product"]

  dbt_reporting:
    schedule: {cron: "0 6 * * 1-5"}
    execute_steps: ["dbt build --selector automatically_generated_selector_reporting"]
```

**Benefits:** Maximum parallelism, fastest total execution time

## Best Practices

### 1. Version Control

Commit generated selectors to track changes over time:

```bash
git add selectors.yml
git commit -m "Update selectors from latest manifest"
```

### 2. CI/CD Integration

Add selector generation to your CI pipeline:

```yaml
# .github/workflows/generate-selectors.yml
name: Generate Selectors

on:
  push:
    paths:
      - 'models/**'
      - 'dbt_project.yml'

jobs:
  generate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2

      - name: Setup Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.10'

      - name: Install dependencies
        run: |
          pip install dbt-core dbt-job-maestro

      - name: Compile dbt project
        run: dbt compile

      - name: Generate selectors
        run: dbt-job-maestro generate --config config.yml

      - name: Commit changes
        run: |
          git config user.name "GitHub Actions"
          git config user.email "actions@github.com"
          git add selectors.yml
          git commit -m "Auto-generate selectors" || echo "No changes"
          git push
```

### 3. Regular Updates

Regenerate selectors when your model structure changes:

```bash
# After adding/removing models
dbt compile
dbt-job-maestro generate --config config.yml
```

### 4. Review Generated Selectors

Always review generated selectors before deploying:

```bash
# Check what changed
git diff selectors.yml

# Test selectors
dbt list --selector <selector_name>
```

## Troubleshooting

### "Manifest file not found"

Make sure to compile your dbt project first:

```bash
dbt compile
```

### "No models found"

Check that your manifest.json contains model definitions:

```bash
dbt-job-maestro info --manifest target/manifest.json
```

### Selectors not updating

Ensure you're not marking selectors as `manually_created` unintentionally:

```yaml
# This will be preserved
description: "manually_created. My selector"

# This will be regenerated
description: "automatically_created. Generated selector"
```

## Contributing

Contributions welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Make your changes with tests
4. Submit a pull request

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Related Projects

- **[dbt-jobs-as-code](https://pypi.org/project/dbt-jobs-as-code/)**: Create and manage dbt Cloud jobs from YAML
- **[dbt-core](https://github.com/dbt-labs/dbt-core)**: The dbt data transformation tool

## Support

- **Issues**: [GitHub Issues](https://github.com/yourusername/dbt-job-maestro/issues)
- **Documentation**: This README and inline code documentation
- **Examples**: See [examples/](examples/) directory

## Architecture

### Refactored Selector System (v0.2.0+)

The selector generation system has been refactored with clean architecture principles:

**Core Components:**
- **`SelectorOrchestrator`**: Coordinates selector generation with priority-based deduplication
- **`BaseSelector`** (ABC): Abstract base class for all selector generators
- **`ManualSelector`**: Preserves manually created selectors from existing files
- **`FQNSelector`**: Generates dependency-based selectors using graph analysis
- **`ModelResolver`**: Resolves models from selector definitions (handles fqn/tag/path methods)
- **`OverlapDetector`**: Detects and reports model overlaps with appropriate severity

**Design Patterns:**
- Strategy Pattern for different selector generation strategies
- Template Method for common selector functionality
- Factory Pattern for creating appropriate generators
- Composite Pattern for selector definitions (union/intersection/exclude)

**Priority System:**
1. Manual selectors (highest priority)
2. FQN-based auto-generated selectors (lower priority)
3. Automatic deduplication prevents model overlap

## Changelog

### 0.2.0 (2026-01-23) - Architecture Refactor

**Major Changes:**
- Refactored selector system with abstract base classes and design patterns
- New `SelectorOrchestrator` replaces `SelectorGenerator` for FQN and mixed modes
- Manual selector identification via `manually_created_` name prefix (primary method)
- Comprehensive model resolution across fqn, tag, and path methods
- Overlap detection with warnings and errors
- Priority-based deduplication system

**Breaking Changes:**
- Mixed mode now only generates FQN-based auto-selectors (no longer generates path/tag auto-selectors)
- Manual selector priority is now highest (models excluded from all auto-generation)

**Backward Compatibility:**
- `SelectorGenerator` still available for path and tag methods
- Legacy manual selector detection (via description) still supported
- CLI remains unchanged

### 0.1.0 (2026-01-22)

- Initial release
- Support for FQN, path, tag, and mixed selector generation
- Configuration file support
- CLI interface with `generate`, `info`, and `init` commands
- Automatic source freshness selector generation
- Model, tag, and path exclusion support
