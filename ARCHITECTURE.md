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
7. Merge to main branch (configured in config.yml)

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
Groups models by their folder structure.

**Use when:** Your models are well-organized by directory

### Method: tag
Groups models by dbt tags.

**Use when:** You have a comprehensive tagging strategy

## Manual Selector Preservation

Across all methods, selectors NOT starting with the configured prefix (`maestro_` by default) are
considered **manual selectors** and are always preserved. Models covered by manual selectors are
automatically excluded from auto-generation to prevent duplicates.

- `critical_revenue`, `my_custom_selector` → Manual (preserved)
- `maestro_stg_customers` → Auto-generated (replaced on regeneration)

## Configuration

### config.yml Structure

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
  # ... more options

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
# Generate selectors from manifest
maestro generate --config config.yml

# Generate jobs from selectors
maestro generate-jobs --config config.yml

# Analyze project
maestro info

# Create config template
maestro init
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
