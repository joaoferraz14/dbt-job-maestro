# Architecture

## Complete Workflow

```
┌─────────────────────────────────────────────────────────────┐
│                    LOCAL DEVELOPMENT                         │
└─────────────────────────────────────────────────────────────┘

1. Edit dbt models
2. dbt compile → target/manifest.json
3. maestro generate → selectors.yml
4. maestro generate-jobs → jobs.yml
5. git commit + push

┌─────────────────────────────────────────────────────────────┐
│                    VERSION CONTROL                           │
└─────────────────────────────────────────────────────────────┘

6. Pull request review
7. Merge to main branch (configured in maestro-config.yml)

┌─────────────────────────────────────────────────────────────┐
│                    CI/CD PIPELINE                            │
└─────────────────────────────────────────────────────────────┘

8. GitHub Actions triggers on main branch
9. Check: current branch == config.deployment.deploy_branch
10. Validate: dbt-jobs-as-code installed & in packages.yml
11. dbt-jobs-as-code sync-jobs jobs.yml
12. Jobs deployed to dbt Cloud ✅
```

## Components

### dbt-job-maestro (This Package)
- **Input**: `manifest.json`
- **Output**: `selectors.yml`, `jobs.yml` (YAML files)
- **Purpose**: Generate selector and job definitions

### dbt-jobs-as-code (dbt-labs)
- **Input**: `jobs.yml`
- **Output**: Jobs in dbt Cloud (via API)
- **Purpose**: Deploy jobs to dbt Cloud

## Selector Generation Methods

### Method: fqn (Fully Qualified Names)
Groups models by shared dependencies using graph analysis.

**Use when:** Your models have clear dependency chains

### Method: path
Groups models by their folder structure at the configured `path_grouping_level`.

**Use when:** Your models are well-organized by directory

**Path Matching:** Uses strict directory boundary matching to prevent overlaps:
- `stage/sap` matches `stage/sap/model.sql`
- `stage/sap` does NOT match `stage/sap_snpglue/model.sql`

Models are assigned to their first matching path only (sorted by path length, shortest first).

### Method: tag
Groups models by dbt tags.

**Use when:** You have a comprehensive tagging strategy

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
  method: fqn | path | tag
  exclude_tags: [deprecated, archived]
  exclude_paths: []
  exclude_models: []
  group_by_dependencies: true  # Only for fqn method
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

# Job definitions
job:
  account_id: 12345
  project_id: 67890
  environment_id: 11111
  cron_schedule: "0 */6 * * *"
  # ... more options

# Deployment control
deployment:
  deploy_branch: main  # Only deploy from this branch
  require_dbt_jobs_as_code: true
```

## Safety Features

### 1. Branch Protection
- Jobs only deployed from configured branch (e.g., `main`)
- Prevents concurrent deployments by multiple devs
- CI/CD validates branch before deploying

### 2. Package Validation
- Checks if dbt-jobs-as-code installed
- Verifies it's in packages.yml
- Fails fast if requirements not met

### 3. Manual Selector Protection
- Selectors NOT starting with `maestro_` prefix are preserved
- Won't overwrite custom selector configurations during regeneration

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

### jobs.yml (dbt-jobs-as-code format)
```yaml
jobs:
  dbt_stg_customers:
    account_id: 12345
    dbt_version: null
    deferring_job_definition_id: null
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

## CLI Commands

```bash
# Create config template (with comments explaining every option)
maestro init --output maestro-config.yml

# Generate selectors from manifest
maestro generate --config maestro-config.yml

# Generate jobs from selectors
maestro generate-jobs --config maestro-config.yml

# Analyze project
maestro info --manifest target/manifest.json

# Check deployment requirements
maestro check --config maestro-config.yml
```

## Dependencies

### Required
- Python >= 3.8
- pyyaml >= 6.0
- click >= 8.0.0

### For Deployment
- dbt-jobs-as-code (from dbt-labs)
- DBT_CLOUD_SERVICE_TOKEN environment variable

## Security

### Service Token Permissions
- Required: "Job Admin" in dbt Cloud
- Scope: Account level or project level
- Storage: CI/CD secrets only (never in code)

### Branch Protection
- Enforce in GitHub/GitLab settings
- Require PR reviews before merge
- Prevent force pushes to main

## Concurrency Handling

**Problem**: Multiple devs editing jobs simultaneously

**Solution**:
1. All changes in YAML files (version controlled)
2. Merge conflicts resolved in git
3. Only CI/CD deploys (single source of truth)
4. Deployment only on configured branch

## Error Handling

```python
from dbt_job_maestro.deployment import validate_deployment_requirements

is_valid, issues = validate_deployment_requirements(
    dbt_project_path=".",
    deploy_branch="main"
)

if not is_valid:
    for issue in issues:
        print(f"❌ {issue}")
    exit(1)
```

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

## Monitoring

### CI/CD Logs
- Check GitHub Actions / GitLab CI logs
- Verify dbt-jobs-as-code sync output

### dbt Cloud
- Review jobs in dbt Cloud UI
- Check job run history
- Validate schedules

## Rollback

```bash
# Revert to previous version
git revert <commit-hash>
git push origin main

# CI/CD will deploy previous version
```

## Best Practices

1. **Always use config file** - Consistency across team
2. **Review before merge** - Check diffs in selectors.yml and jobs.yml
3. **Test locally** - Run `dbt list --selector` before committing
4. **Deploy from CI/CD only** - Never manual deployments
5. **Use branch protection** - Enforce review process
