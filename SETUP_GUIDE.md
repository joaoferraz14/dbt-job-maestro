# Setup Guide for dbt-job-maestro

This guide will help you set up and publish your dbt-job-maestro package.

## Development Setup

### 1. Install in Development Mode

```bash
cd dbt-job-maestro
pip install -e ".[dev]"
```

This installs the package in "editable" mode with development dependencies.

### 2. Test the Installation

```bash
# Test the CLI
dbt-job-maestro --help

# Run tests
pytest

# Format code
black dbt_job_maestro/
```

## Using with Your DBT Project

### 1. Compile Your DBT Project

```bash
cd your-dbt-project
dbt compile
```

This generates the `target/manifest.json` file.

### 2. Generate Selectors

```bash
# Navigate to your dbt project
cd your-dbt-project

# Generate selectors
dbt-job-maestro generate-selectors --manifest target/manifest.json --output selectors.yml
```

### 3. Generate Jobs

```bash
dbt-job-maestro generate-jobs \
  --selectors selectors.yml \
  --output jobs.yml \
  --account-id YOUR_ACCOUNT_ID \
  --project-id YOUR_PROJECT_ID \
  --environment-id YOUR_ENVIRONMENT_ID
```

## Publishing to PyPI

### 1. Build the Package

```bash
# Install build tools
pip install build twine

# Build the package
python -m build
```

This creates files in the `dist/` directory:
- `dbt_job_maestro-0.1.0.tar.gz` (source distribution)
- `dbt_job_maestro-0.1.0-py3-none-any.whl` (wheel distribution)

### 2. Test Upload to TestPyPI (Optional)

```bash
# Upload to TestPyPI
python -m twine upload --repository testpypi dist/*

# Install from TestPyPI to test
pip install --index-url https://test.pypi.org/simple/ dbt-job-maestro
```

### 3. Upload to PyPI

```bash
# Upload to PyPI
python -m twine upload dist/*
```

You'll need:
- A PyPI account (https://pypi.org/account/register/)
- An API token (https://pypi.org/manage/account/token/)

### 4. Install from PyPI

Once published, users can install with:

```bash
pip install dbt-job-maestro
```

## Before Publishing Checklist

- [ ] Update version in `pyproject.toml`
- [ ] Update version in `dbt_job_maestro/__init__.py`
- [ ] Update version in `dbt_job_maestro/cli.py`
- [ ] Update author information in `pyproject.toml`
- [ ] Update repository URLs in `pyproject.toml`
- [ ] Add tests and ensure they pass
- [ ] Update README.md with correct information
- [ ] Add CHANGELOG.md for version history
- [ ] Test package installation in a clean environment

## Package Structure

```
dbt-job-maestro/
├── dbt_job_maestro/           # Main package code
│   ├── __init__.py
│   ├── cli.py
│   ├── config.py
│   ├── manifest_parser.py
│   ├── graph_builder.py
│   ├── selector_generator.py
│   └── job_generator.py
├── tests/                      # Test files
│   ├── __init__.py
│   └── test_manifest_parser.py
├── examples/                   # Example scripts
│   ├── README.md
│   └── example_usage.py
├── pyproject.toml             # Package configuration
├── MANIFEST.in                # Package data files
├── README.md                  # Package documentation
├── LICENSE                    # MIT License
├── SETUP_GUIDE.md            # This file
└── .gitignore                # Git ignore rules
```

## Configuration Files to Update

### pyproject.toml

Update the following fields:
```toml
[project]
name = "dbt-job-maestro"
version = "0.1.0"  # Update for each release
authors = [
    { name = "Your Name", email = "your.email@example.com" }  # Update
]

[project.urls]
Homepage = "https://github.com/yourusername/dbt-job-maestro"  # Update
Repository = "https://github.com/yourusername/dbt-job-maestro"  # Update
```

## Versioning

Follow Semantic Versioning (https://semver.org/):
- MAJOR version for incompatible API changes
- MINOR version for new functionality (backwards compatible)
- PATCH version for bug fixes (backwards compatible)

Examples:
- `0.1.0` - Initial release
- `0.1.1` - Bug fix
- `0.2.0` - New features
- `1.0.0` - First stable release

## Git Tags

Tag releases in git:

```bash
git tag -a v0.1.0 -m "Release version 0.1.0"
git push origin v0.1.0
```

## Continuous Integration (Optional)

Consider setting up GitHub Actions for:
- Running tests on pull requests
- Automatically publishing to PyPI on release
- Code formatting checks
- Type checking with mypy

## Support and Maintenance

- Create issues for bug reports and feature requests
- Use pull requests for contributions
- Keep dependencies up to date
- Update documentation as features change

## Resources

- Python Packaging Guide: https://packaging.python.org/
- PyPI: https://pypi.org/
- TestPyPI: https://test.pypi.org/
- Semantic Versioning: https://semver.org/
