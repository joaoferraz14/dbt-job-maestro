"""Tests for ManifestParser"""

import pytest
import json
import tempfile
from pathlib import Path

from dbt_job_maestro.manifest_parser import ManifestParser


def test_manifest_parser_basic():
    """Test basic manifest parsing functionality"""
    # Create a minimal manifest
    manifest = {
        "nodes": {
            "model.my_project.model_a": {
                "name": "model_a",
                "fqn": ["my_project", "staging", "model_a"],
                "path": "staging/model_a.sql",
                "original_file_path": "models/staging/model_a.sql",
                "tags": ["staging"],
                "resource_type": "model",
                "depends_on": {"nodes": ["source.my_project.raw.users"]},
            },
            "model.my_project.model_b": {
                "name": "model_b",
                "fqn": ["my_project", "marts", "model_b"],
                "path": "marts/model_b.sql",
                "original_file_path": "models/marts/model_b.sql",
                "tags": ["marts"],
                "resource_type": "model",
                "depends_on": {"nodes": ["model.my_project.model_a"]},
            },
        }
    }

    # Write manifest to temp file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(manifest, f)
        manifest_path = f.name

    try:
        parser = ManifestParser(manifest_path)
        models = parser.get_models()

        assert len(models) == 2
        assert "model_a" in models
        assert "model_b" in models

        # Check model_a
        assert models["model_a"]["name"] == "model_a"
        assert models["model_a"]["tags"] == ["staging"]
        assert len(models["model_a"]["sources"]) == 1

        # Check model_b
        assert models["model_b"]["name"] == "model_b"
        assert models["model_b"]["tags"] == ["marts"]
        assert "model_a" in models["model_b"]["dependencies"]

    finally:
        Path(manifest_path).unlink()


def test_get_models_by_tag():
    """Test filtering models by tag"""
    manifest = {
        "nodes": {
            "model.my_project.model_a": {
                "name": "model_a",
                "fqn": ["my_project", "model_a"],
                "path": "model_a.sql",
                "original_file_path": "models/model_a.sql",
                "tags": ["daily"],
                "resource_type": "model",
                "depends_on": {"nodes": []},
            },
            "model.my_project.model_b": {
                "name": "model_b",
                "fqn": ["my_project", "model_b"],
                "path": "model_b.sql",
                "original_file_path": "models/model_b.sql",
                "tags": ["hourly"],
                "resource_type": "model",
                "depends_on": {"nodes": []},
            },
        }
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(manifest, f)
        manifest_path = f.name

    try:
        parser = ManifestParser(manifest_path)
        daily_models = parser.get_models_by_tag("daily")
        hourly_models = parser.get_models_by_tag("hourly")

        assert len(daily_models) == 1
        assert "model_a" in daily_models
        assert len(hourly_models) == 1
        assert "model_b" in hourly_models

    finally:
        Path(manifest_path).unlink()
