# Smart Exclusion Example

This example demonstrates how manual selectors that use paths or tags prevent duplicate automated selectors.

## Scenario

You have a dbt project with:
- Models in `models/staging/legacy` (legacy ETL)
- Models tagged with `critical` (high-priority business metrics)
- Models tagged with `daily` and `hourly` (refresh schedules)
- Various other models

## Configuration

**config.yml:**
```yaml
selector:
  method: mixed
  preserve_manual_selectors: true

  # You want path-based selectors for these
  include_path_groups:
    - models/staging/legacy  # BUT this is in a manual selector!
    - models/marts/finance

  exclude_tags:
    - deprecated
```

## Manual Selectors

**selectors.yml (before generation):**
```yaml
selectors:
  # Manual selector using BOTH path and tag
  - name: legacy_special_handling
    description: "manually_created - Legacy models requiring custom preprocessing"
    definition:
      union:
        - method: path
          value: models/staging/legacy
        - method: tag
          value: legacy_etl

  # Manual selector using tag
  - name: critical_hourly_reports
    description: "manually_created - Critical reports run every hour with alerts"
    definition:
      union:
        - method: tag
          value: critical
        - method: tag
          value: hourly
```

## What Happens During Generation

When you run:
```bash
maestro generate --config config.yml
```

### Stage 1: Manual Selectors (Priority 1)
✅ Both manual selectors are preserved

**Tracked for exclusion:**
- **Models:** All models in `models/staging/legacy` + all models tagged `legacy_etl` + all models tagged `critical` + all models tagged `hourly`
- **Paths:** `models/staging/legacy`
- **Tags:** `legacy_etl`, `critical`, `hourly`

### Stage 2: Path Groups (Priority 2)
Config has:
```yaml
include_path_groups:
  - models/staging/legacy  # ❌ SKIPPED - already in manual selector
  - models/marts/finance   # ✅ CREATED
```

**Result:**
```yaml
selectors:
  # Path selector created
  - name: path_models_marts_finance
    description: Selector for models in models/marts/finance
    definition:
      union:
        - method: path
          value: models/marts/finance
```

Note: NO `path_models_staging_legacy` is created because it's in the manual selector!

### Stage 3: Tag-Based (Priority 3)
Available tags in project: `daily`, `weekly`, `legacy_etl`, `critical`, `hourly`

Filtered tags:
- ❌ `legacy_etl` - used in manual selector
- ❌ `critical` - used in manual selector
- ❌ `hourly` - used in manual selector
- ✅ `daily` - available for auto-generation
- ✅ `weekly` - available for auto-generation

**Result:**
```yaml
selectors:
  # Tag selectors created
  - name: tag_daily
    description: Selector for models tagged with daily
    definition:
      union:
        - method: tag
          value: daily

  - name: tag_weekly
    description: Selector for models tagged with weekly
    definition:
      union:
        - method: tag
          value: weekly
```

Note: NO `tag_critical` or `tag_hourly` created because they're in manual selectors!

### Stage 4: FQN-Based (Priority 4)
All remaining models (not in manual, path, or tag selectors) are grouped by dependencies.

```yaml
selectors:
  - name: maestro_stg_customers
    description: Selector for models in component starting with stg_customers
    definition:
      union:
        - method: fqn
          value: stg_customers
        - method: fqn
          value: int_customer_metrics
```

## Final Result

**selectors.yml (after generation):**
```yaml
selectors:
  # === PRIORITY 1: Manual selectors (PRESERVED) ===
  - name: legacy_special_handling
    description: "manually_created - Legacy models requiring custom preprocessing"
    definition:
      union:
        - method: path
          value: models/staging/legacy
        - method: tag
          value: legacy_etl

  - name: critical_hourly_reports
    description: "manually_created - Critical reports run every hour with alerts"
    definition:
      union:
        - method: tag
          value: critical
        - method: tag
          value: hourly

  # === PRIORITY 2: Path groups (AUTO-GENERATED) ===
  - name: path_models_marts_finance
    description: Selector for models in models/marts/finance
    definition:
      union:
        - method: path
          value: models/marts/finance

  # === PRIORITY 3: Tag-based (AUTO-GENERATED) ===
  - name: tag_daily
    description: Selector for models tagged with daily
    definition:
      union:
        - method: tag
          value: daily

  - name: tag_weekly
    description: Selector for models tagged with weekly
    definition:
      union:
        - method: tag
          value: weekly

  # === PRIORITY 4: FQN-based (AUTO-GENERATED) ===
  - name: maestro_stg_customers
    description: Selector for models in component starting with stg_customers
    definition:
      union:
        - method: fqn
          value: stg_customers
        - method: fqn
          value: int_customer_metrics
```

## Verification: Zero Duplicates

To verify each model appears in exactly one selector:

```bash
# List models in each selector
dbt list --selector legacy_special_handling
dbt list --selector critical_hourly_reports
dbt list --selector path_models_marts_finance
dbt list --selector tag_daily
dbt list --selector tag_weekly
dbt list --selector maestro_stg_customers

# Write a script to check for overlaps
```

## Key Takeaways

1. **Manual selectors have ultimate control**
   - Use them for complex business logic
   - They prevent duplicate automated selectors

2. **Smart path exclusion**
   - If a path is in a manual selector, it won't be auto-generated
   - Even if it's in `include_path_groups`

3. **Smart tag exclusion**
   - If a tag is in a manual selector, it won't be auto-generated
   - No `tag_X` selector created

4. **Combines perfectly**
   - You can mix manual control with automated convenience
   - Manual for special cases, automated for standard patterns

5. **Regeneration-safe**
   - Running `generate` again preserves manual selectors
   - Updates automated selectors based on current manifest
   - Smart exclusion still applies

## Common Use Cases

### Use Case 1: Override path grouping
```yaml
# Config has: include_path_groups: [models/staging/api_sources]
# But you need custom logic for it:

selectors:
  - name: api_sources_with_retries
    description: "manually_created - API sources with retry logic"
    definition:
      union:
        - method: path
          value: models/staging/api_sources
```

Result: Your manual selector is used, no `path_models_staging_api_sources` created.

### Use Case 2: Combine multiple tags
```yaml
selectors:
  - name: critical_realtime_dashboard
    description: "manually_created - Real-time dashboard models"
    definition:
      union:
        - method: tag
          value: critical
        - method: tag
          value: realtime
        - method: tag
          value: dashboard
```

Result: No `tag_critical`, `tag_realtime`, or `tag_dashboard` auto-generated.

### Use Case 3: Mix all methods
```yaml
selectors:
  - name: executive_reporting_suite
    description: "manually_created - Executive reports with SLA guarantees"
    definition:
      union:
        - method: fqn
          value: fct_executive_revenue
        - method: path
          value: models/marts/executive
        - method: tag
          value: executive
        - method: tag
          value: sla_1hour
```

Result: Models from FQN + path + tags all handled by this one selector, excluded from automated generation.
