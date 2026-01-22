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
# Basic usage - analyzes dependencies
dbt-job-maestro generate --manifest target/manifest.json

# Group by folder structure
dbt-job-maestro generate --method path

# Group by tags
dbt-job-maestro generate --method tag

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

### Mixed

Combines multiple methods with a priority system to ensure NO duplicate models across selectors.

**Priority order:**
1. **Manually created selectors** - Preserved from existing file (marked with "manually_created" in description)
2. **Path-based selectors** - For specific paths in `include_path_groups` config
3. **Tag-based selectors** - For models with tags (excluding already assigned)
4. **FQN-based selectors** - For remaining models using dependency analysis

**Best for:** Complex projects needing flexible, customized selector strategies without duplicates

```bash
dbt-job-maestro generate --method mixed
```

**Example config:**
```yaml
selector:
  method: mixed
  include_path_groups:
    - models/staging/legacy  # Gets its own selector first
    - models/marts/critical
  exclude_tags:
    - deprecated
```

This ensures models in `models/staging/legacy` get their own dedicated selector, then tagged models get selectors, and finally remaining models are grouped by dependencies - with zero overlap.

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

`dbt-job-maestro` focuses on selector generation. To materialize these selectors as dbt Cloud jobs, use the **[dbt-jobs-as-code](https://pypi.org/project/dbt-jobs-as-code/)** package:

```bash
# Install dbt-jobs-as-code
pip install dbt-jobs-as-code

# Create jobs from selectors via dbt Cloud API
dbt-jobs-as-code create --selectors selectors.yml --config dbt-cloud-config.yml
```

This separation of concerns allows:
- **dbt-job-maestro**: Focus on selector logic and generation
- **dbt-jobs-as-code**: Handle dbt Cloud API integration and job management

## Advanced Usage

### Programmatic API

Use dbt-job-maestro as a Python library:

```python
from dbt_job_maestro import ManifestParser, GraphBuilder, SelectorGenerator
from dbt_job_maestro.config import Config

# Load configuration
config = Config.from_yaml("config.yml")

# Parse manifest
parser = ManifestParser(config.manifest_path)
models = parser.get_models()

# Build dependency graph
graph = GraphBuilder(models)

# Generate selectors
generator = SelectorGenerator(parser, graph, config.selector)
selectors = generator.generate_selectors()

# Write to file
generator.write_selectors(selectors, config.selectors_output_file)
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

Include `manually_created` anywhere in the description:

```yaml
selectors:
  # Manual selector - PRESERVED during regeneration
  - name: critical_revenue_models
    description: "manually_created - Critical revenue tracking models"
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
  - name: experimental_features
    description: "manually_created - Experimental features under development"
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

**With mixed mode:**

```yaml
selector:
  method: mixed
  preserve_manual_selectors: true  # Enable manual selector preservation
```

When regenerating, models in manual selectors are **excluded** from tag-based and FQN-based grouping, ensuring no duplicates.

**Smart Exclusion:** If a manual selector uses `method: path` or `method: tag`, those specific paths/tags are also excluded from automated generation. For example:

```yaml
selectors:
  - name: legacy_custom_handling
    description: "manually_created - Legacy models with special logic"
    definition:
      union:
        - method: path
          value: models/staging/legacy
        - method: tag
          value: critical
```

With this manual selector:
- No `path_models_staging_legacy` will be auto-generated (even if in `include_path_groups`)
- No `tag_critical` will be auto-generated
- Models in that path or with that tag are completely handled by your manual selector

See [examples/selectors_with_manual.yml](examples/selectors_with_manual.yml) for a complete example.

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

## Changelog

### 0.1.0 (2026-01-22)

- Initial release
- Support for FQN, path, tag, and mixed selector generation
- Configuration file support
- CLI interface with `generate`, `info`, and `init` commands
- Automatic source freshness selector generation
- Model, tag, and path exclusion support
