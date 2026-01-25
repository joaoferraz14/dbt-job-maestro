.PHONY: install install-dev install-hooks lint format test clean all

# Install production dependencies
install:
	pip install -e .

# Install development dependencies
install-dev:
	pip install -e ".[dev]"
	pip install pre-commit

# Install pre-commit hooks
install-hooks: install-dev
	pre-commit install

# Run linting
lint:
	flake8 dbt_job_maestro tests --max-line-length=100 --extend-ignore=E203,E501,W503

# Run code formatting
format:
	black dbt_job_maestro tests --line-length=100

# Check formatting without modifying files
format-check:
	black dbt_job_maestro tests --line-length=100 --check

# Run tests
test:
	pytest tests/ -v --tb=short

# Run all pre-commit hooks on all files
pre-commit:
	pre-commit run --all-files

# Clean up build artifacts
clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf .pytest_cache/
	rm -rf __pycache__/
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true

# Setup development environment (install deps + hooks)
setup: install-dev install-hooks
	@echo "Development environment ready!"

# Run all checks (format check, lint, test)
all: format-check lint test
