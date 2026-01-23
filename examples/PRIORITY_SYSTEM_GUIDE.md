# Priority System Guide for Mixed Mode

This guide explains how to use the `mixed` method's priority system to ensure zero duplicate models across selectors.

## Overview

The mixed mode uses a **4-stage priority system** where each model is assigned to exactly ONE selector:

```
Priority 1: Manual Selectors (highest)
    ↓
Priority 2: Path Groups
    ↓
Priority 3: Tag-Based
    ↓
Priority 4: FQN-Based (lowest)
```

## Priority 1: Manual Selectors

**What:** Selectors you create manually for special cases

**When to use:**
- Critical models needing special handling
- Complex business logic requiring custom grouping
- Models with unique scheduling requirements

**How to create:**

1. Edit `selectors.yml` and add your selector
2. Include `manually_created` in the description

```yaml
selectors:
  - name: critical_revenue_models
    description: "manually_created - Critical revenue models run every hour"
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
```

**Result:**
- These models are **excluded** from all automated selector generation
- Any **paths** or **tags** used in manual selectors are also excluded from automated generation

### Smart Exclusion

If your manual selector uses paths or tags, those are automatically excluded from automated generation:

**Example 1: Manual selector with path**
```yaml
selectors:
  - name: legacy_special_handling
    description: "manually_created - Legacy models with custom preprocessing"
    definition:
      union:
        - method: path
          value: models/staging/legacy
```

**Config:**
```yaml
selector:
  method: mixed
  include_path_groups:
    - models/staging/legacy  # This will be SKIPPED
    - models/marts/critical
```

**Result:**
- `models/staging/legacy` is already handled by the manual selector
- No `path_models_staging_legacy` auto-generated
- Only `path_models_marts_critical` is created

**Example 2: Manual selector with tag**
```yaml
selectors:
  - name: hourly_critical_reports
    description: "manually_created - Hourly reports requiring special monitoring"
    definition:
      union:
        - method: tag
          value: hourly
        - method: tag
          value: critical
```

**Result:**
- No `tag_hourly` selector auto-generated
- No `tag_critical` selector auto-generated
- Other tags like `daily`, `weekly` still get their own selectors

## Priority 2: Path Groups

**What:** Specific directories that get dedicated selectors

**When to use:**
- Legacy models requiring separate jobs
- Experimental features in development
- Critical business areas needing isolation
- Models with different SLAs

**How to configure:**

```yaml
selector:
  method: mixed
  include_path_groups:
    - models/staging/legacy      # Legacy ETL models
    - models/marts/critical      # Critical business metrics
    - models/marts/experimental  # Features under development
```

**Result:** Each path gets its own selector:
- `path_models_staging_legacy`
- `path_models_marts_critical`
- `path_models_marts_experimental`

Models in these paths are **excluded** from tag/FQN grouping.

## Priority 3: Tag-Based Selectors

**What:** Models grouped by their dbt tags

**When to use:**
- Models with common characteristics (daily, hourly)
- Business domains (finance, marketing)
- Data layers already tagged

**How it works:**

After manual and path selectors are created, remaining tagged models get tag-based selectors.

**Example:**

If you have:
- `@tag daily` on 20 models
- 5 already in manual selectors
- 3 already in path groups

Result: `tag_daily` selector contains **12 models** (20 - 5 - 3)

**Configuration:**

```yaml
selector:
  method: mixed
  exclude_tags:
    - deprecated  # Don't create selectors for these tags
    - archived
```

## Priority 4: FQN-Based Selectors

**What:** Dependency-based grouping for remaining models

**When to use:** Automatically applied to all unassigned models

**How it works:**

Models not yet assigned to manual/path/tag selectors are grouped by:
- Shared dependencies
- Connected components in dependency graph

**Result:**
- `maestro_stg_customers`
- `maestro_dim_products`

## Complete Example

### Initial Setup

**config.yml:**
```yaml
selector:
  method: mixed
  preserve_manual_selectors: true

  # Priority 2: Path groups
  include_path_groups:
    - models/staging/legacy
    - models/marts/critical

  # Exclude these tags from selector generation
  exclude_tags:
    - deprecated
    - wip

  # Standard options
  group_by_dependencies: true
  min_models_per_selector: 1
```

### Step 1: Create Manual Selectors

**selectors.yml:**
```yaml
selectors:
  # MANUAL: Revenue tracking (Priority 1)
  - name: critical_revenue
    description: "manually_created - Revenue models run hourly"
    definition:
      union:
        - method: fqn
          value: fct_revenue
        - method: fqn
          value: fct_subscriptions
```

**Models assigned:** `fct_revenue`, `fct_subscriptions`

### Step 2: Generate Selectors

```bash
maestro generate --config config.yml
```

### Step 3: Review Results

**selectors.yml (after generation):**
```yaml
selectors:
  # Priority 1: PRESERVED manual selector
  - name: critical_revenue
    description: "manually_created - Revenue models run hourly"
    definition:
      union:
        - method: fqn
          value: fct_revenue
        - method: fqn
          value: fct_subscriptions

  # Priority 2: Path-based selectors
  - name: path_models_staging_legacy
    description: Selector for models in models/staging/legacy
    definition:
      union:
        - method: path
          value: models/staging/legacy

  - name: path_models_marts_critical
    description: Selector for models in models/marts/critical
    definition:
      union:
        - method: path
          value: models/marts/critical

  # Priority 3: Tag-based selectors (excluding models already assigned)
  - name: tag_daily
    description: Selector for models tagged with daily
    definition:
      union:
        - method: tag
          value: daily

  - name: tag_hourly
    description: Selector for models tagged with hourly
    definition:
      union:
        - method: tag
          value: hourly

  # Priority 4: FQN-based for remaining models
  - name: maestro_stg_customers
    description: Selector for models in component starting with stg_customers
    definition:
      union:
        - method: fqn
          value: stg_customers
        - method: fqn
          value: int_customer_summary
          parents: true
```

## Verification: Zero Duplicates

To verify no duplicates:

```bash
# List models in each selector
dbt list --selector critical_revenue
dbt list --selector path_models_staging_legacy
dbt list --selector tag_daily
dbt list --selector maestro_stg_customers

# Check for overlap (should be empty)
# Use your favorite scripting language to compare outputs
```

## Common Patterns

### Pattern 1: Critical + Everything Else

**Use case:** Isolate critical models, group the rest by tags

```yaml
selector:
  method: mixed
  preserve_manual_selectors: true  # Manual: critical models
  # No include_path_groups
  # Tags and FQN handle the rest
```

### Pattern 2: Legacy + New Architecture

**Use case:** Separate legacy models while organizing new ones

```yaml
selector:
  method: mixed
  include_path_groups:
    - models/staging/legacy_etl
    - models/marts/old_reporting
  # New models organized by tags/dependencies
```

### Pattern 3: Full Control

**Use case:** Maximum customization

```yaml
selector:
  method: mixed
  preserve_manual_selectors: true     # Priority 1
  include_path_groups:                # Priority 2
    - models/staging/critical
    - models/marts/executive
  exclude_tags:                       # Excluded from Priority 3
    - deprecated
    - experimental
  # Priority 4 handles remaining models
```

## Troubleshooting

### Issue: Models appearing in multiple selectors

**Cause:** Not using `mixed` method

**Solution:** Set `method: mixed` in config

### Issue: Manual selectors not preserved

**Cause:** Missing `manually_created` in description

**Solution:** Update description:
```yaml
description: "manually_created - Your description"
```

### Issue: Path selectors not created

**Cause:** Paths not in `include_path_groups`

**Solution:** Add paths to config:
```yaml
selector:
  include_path_groups:
    - models/your/path
```

### Issue: Too many small selectors

**Cause:** Low `min_models_per_selector` threshold

**Solution:** Increase minimum:
```yaml
selector:
  min_models_per_selector: 3
```

## Best Practices

1. **Start Simple:** Begin with manual selectors only, then add path groups as needed

2. **Document Everything:** Explain why each manual selector exists

3. **Review Regularly:** Check generated selectors after major model changes

4. **Version Control:** Commit both config.yml and selectors.yml

5. **Test Locally:** Verify selectors with `dbt list --selector <name>` before deploying

6. **Incremental Adoption:** Don't try to use all priority levels at once

## See Also

- [config_mixed_mode.yml](config_mixed_mode.yml) - Complete configuration example
- [selectors_with_manual.yml](selectors_with_manual.yml) - Manual selector examples
- [ARCHITECTURE.md](../ARCHITECTURE.md) - System design documentation
- [README.md](../README.md) - Package overview
