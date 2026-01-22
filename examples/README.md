# Examples

This directory contains example usage of dbt-job-maestro.

## Running the Examples

1. Make sure you have a dbt project with a compiled manifest:

```bash
cd your-dbt-project
dbt compile
```

2. Copy the `manifest.json` to a `target/` directory or update the path in the examples

3. Run the example script:

```bash
python examples/example_usage.py
```

## Example Files

- `example_usage.py`: Comprehensive examples showing different ways to use dbt-job-maestro

## What the Examples Show

1. **Project Info**: Analyze your dbt project structure
2. **FQN Selectors**: Generate selectors based on model dependencies
3. **Path Selectors**: Generate selectors based on folder structure
4. **Tag Selectors**: Generate selectors based on dbt tags
5. **Job Generation**: Create dbt Cloud jobs from selectors
