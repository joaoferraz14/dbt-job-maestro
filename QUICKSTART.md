# Quick Start Guide

Get started with dbt-job-maestro in 3 steps!

## Step 1: Install

```bash
pip install dbt-job-maestro
```

## Step 2: Generate Selectors

### Option A: Use CLI (Quick)

```bash
# Navigate to your dbt project
cd your-dbt-project

# Compile to generate manifest.json
dbt compile

# Generate selectors with defaults
maestro generate --manifest target/manifest.json
```

### Option B: Use Config File (Recommended)

```bash
# Create config template
maestro init

# Edit config.yml with your preferences
# Then generate
maestro generate --config config.yml
```

## Step 3: Use Your Selectors

```bash
# Test a selector
dbt list --selector <selector_name>

# Run models with a selector
dbt build --selector <selector_name>
```

## Common Configurations

### Exclude Deprecated Models

```yaml
selector:
  exclude_tags:
    - deprecated
    - archived
```

### Group by Folder Structure

```yaml
selector:
  method: path
  path_grouping_level: 1  # Group by first subdirectory
```

### Group by Tags

```yaml
selector:
  method: tag
```

### Custom Model Prefixes

If your models follow a naming convention:

```yaml
selector:
  prefix_order:
    - bronze
    - silver
    - gold
```

Or leave empty for alphabetical sorting:

```yaml
selector:
  prefix_order: []
```

## Creating dbt Cloud Jobs

To materialize selectors as dbt Cloud jobs, use the **dbt-jobs-as-code** package:

```bash
pip install dbt-jobs-as-code
dbt-jobs-as-code create --selectors selectors.yml
```

## Get Help

```bash
# See all commands
dbt-job-maestro --help

# Analyze your project
maestro info

# See what each method does
maestro generate --help
```

## Examples

See [examples/](examples/) directory for:
- [config.yml](examples/config.yml) - Fully documented configuration
- [example_usage.py](examples/example_usage.py) - Python API examples

## Documentation

See [README.md](README.md) for comprehensive documentation.
