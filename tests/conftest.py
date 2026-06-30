"""Shared pytest fixtures and test isolation.

Several code paths (notably ManualSelector.generate) look for a ``selectors.yml``
in the current working directory to preserve hand-written selectors. Without
isolation, running the suite from a directory that happens to contain a real
``selectors.yml`` (e.g. a dbt project root) pollutes tests and causes spurious
failures. The autouse fixture below runs every test from a clean temp directory.
"""

import os

import pytest


@pytest.fixture(autouse=True)
def _isolated_cwd(tmp_path, monkeypatch):
    """Run each test from a clean temporary working directory."""
    monkeypatch.chdir(tmp_path)
    yield
    # monkeypatch restores the original cwd automatically
    os.getcwd()
