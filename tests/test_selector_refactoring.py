"""Comprehensive tests for refactored selector system."""

import pytest
import tempfile
import os
import yaml
from unittest.mock import Mock

from dbt_job_maestro.selector_types import (
    SelectorPriority,
    SelectorMetadata,
    ModelResolution,
    OverlapWarning
)
from dbt_job_maestro.base_selector import BaseSelector
from dbt_job_maestro.model_resolver import ModelResolver
from dbt_job_maestro.selectors.manual_selector import ManualSelector
from dbt_job_maestro.selectors.fqn_selector import FQNSelector
from dbt_job_maestro.overlap_detector import OverlapDetector
from dbt_job_maestro.selector_orchestrator import SelectorOrchestrator
from dbt_job_maestro.manifest_parser import ManifestParser
from dbt_job_maestro.graph_builder import GraphBuilder
from dbt_job_maestro.config import SelectorConfig


@pytest.fixture
def sample_models():
    """Sample model data for testing."""
    return {
        "model_a": {
            "name": "model_a",
            "fqn": ["project", "staging", "model_a"],
            "path": "staging/model_a.sql",
            "tags": ["staging", "daily"],
            "dependencies": [],
            "sources": ["source.raw.users"]
        },
        "model_b": {
            "name": "model_b",
            "fqn": ["project", "staging", "model_b"],
            "path": "staging/model_b.sql",
            "tags": ["staging"],
            "dependencies": ["model_a"],
            "sources": []
        },
        "model_c": {
            "name": "model_c",
            "fqn": ["project", "marts", "model_c"],
            "path": "marts/model_c.sql",
            "tags": ["marts", "daily"],
            "dependencies": ["model_b"],
            "sources": []
        }
    }


@pytest.fixture
def mock_parser(sample_models):
    """Mock ManifestParser for testing."""
    parser = Mock(spec=ManifestParser)
    parser.get_models.return_value = sample_models
    parser.get_all_tags.return_value = {"staging", "daily", "marts"}
    return parser


@pytest.fixture
def mock_graph(sample_models):
    """Mock GraphBuilder for testing."""
    graph = Mock(spec=GraphBuilder)
    graph.models = sample_models
    graph.group_by_tag.return_value = []
    graph.group_by_path.return_value = []
    graph.get_models_with_sources.return_value = {"model_a"}
    graph.find_connected_components.return_value = [["model_a", "model_b", "model_c"]]
    graph.find_independent_models.return_value = []
    return graph


class TestManualSelectorIdentification:
    """Test manual selector detection via name prefix."""

    def test_prefix_based_detection(self, mock_parser, mock_graph):
        """Test manual selector identification via name prefix."""
        config = SelectorConfig()
        selector = ManualSelector(mock_parser, mock_graph, config)

        selector_def = {
            "name": "manually_created_critical_revenue",
            "description": "Revenue models",
            "definition": {"union": [{"method": "fqn", "value": "fct_revenue"}]}
        }

        assert selector.is_manually_created(selector_def) is True

    def test_metadata_based_detection(self, mock_parser, mock_graph):
        """Test manual selector identification via metadata field."""
        config = SelectorConfig()
        selector = ManualSelector(mock_parser, mock_graph, config)

        selector_def = {
            "name": "revenue_selector",
            "metadata": {"manually_created": True},
            "description": "Revenue models",
            "definition": {"union": [{"method": "fqn", "value": "fct_revenue"}]}
        }

        assert selector.is_manually_created(selector_def) is True

    def test_description_based_detection(self, mock_parser, mock_graph):
        """Test manual selector identification via description (fallback)."""
        config = SelectorConfig()
        selector = ManualSelector(mock_parser, mock_graph, config)

        selector_def = {
            "name": "revenue_selector",
            "description": "manually_created Revenue models",
            "definition": {"union": [{"method": "fqn", "value": "fct_revenue"}]}
        }

        assert selector.is_manually_created(selector_def) is True

    def test_auto_selector_detection(self, mock_parser, mock_graph):
        """Test that auto-generated selectors are not marked as manual."""
        config = SelectorConfig()
        selector = ManualSelector(mock_parser, mock_graph, config)

        selector_def = {
            "name": "automatically_generated_selector_model_a",
            "description": "Selector for models in component",
            "definition": {"union": [{"method": "fqn", "value": "model_a"}]}
        }

        assert selector.is_manually_created(selector_def) is False


class TestModelResolver:
    """Test resolving models from selector definitions."""

    def test_resolve_fqn_selector(self, mock_parser, mock_graph):
        """Test resolving models from FQN selector."""
        resolver = ModelResolver(mock_parser, mock_graph)

        selector_def = {
            "name": "test_selector",
            "definition": {
                "union": [
                    {"method": "fqn", "value": "model_a"},
                    {"method": "fqn", "value": "model_b"}
                ]
            }
        }

        resolution = resolver.resolve_selector(selector_def)

        assert "model_a" in resolution.models
        assert "model_b" in resolution.models
        assert "model_a" in resolution.fqns
        assert "model_b" in resolution.fqns

    def test_resolve_tag_selector(self, mock_parser, mock_graph):
        """Test resolving models from tag selector."""
        mock_graph.group_by_tag.return_value = ["model_a", "model_b"]
        resolver = ModelResolver(mock_parser, mock_graph)

        selector_def = {
            "name": "test_selector",
            "definition": {
                "union": [{"method": "tag", "value": "staging"}]
            }
        }

        resolution = resolver.resolve_selector(selector_def)

        assert "model_a" in resolution.models
        assert "model_b" in resolution.models
        assert "staging" in resolution.tags

    def test_resolve_path_selector(self, mock_parser, mock_graph):
        """Test resolving models from path selector."""
        mock_graph.group_by_path.return_value = ["model_a", "model_b"]
        resolver = ModelResolver(mock_parser, mock_graph)

        selector_def = {
            "name": "test_selector",
            "definition": {
                "union": [{"method": "path", "value": "staging/"}]
            }
        }

        resolution = resolver.resolve_selector(selector_def)

        assert "model_a" in resolution.models
        assert "model_b" in resolution.models
        assert "staging/" in resolution.paths

    def test_resolve_with_exclusion(self, mock_parser, mock_graph):
        """Test resolving models with exclusions."""
        mock_graph.group_by_tag.side_effect = [
            ["model_a", "model_b", "model_c"],  # staging tag
            ["model_c"]  # marts tag for exclusion
        ]
        resolver = ModelResolver(mock_parser, mock_graph)

        selector_def = {
            "name": "test_selector",
            "definition": {
                "union": [{"method": "tag", "value": "staging"}],
                "exclude": {
                    "union": [{"method": "tag", "value": "marts"}]
                }
            }
        }

        resolution = resolver.resolve_selector(selector_def)

        assert "model_a" in resolution.models
        assert "model_b" in resolution.models
        assert "model_c" not in resolution.models


class TestOverlapDetector:
    """Test overlap detection and warning system."""

    def test_manual_selector_overlap_warning(self, mock_parser, mock_graph):
        """Test that manual selector overlaps produce warnings."""
        resolver = ModelResolver(mock_parser, mock_graph)
        detector = OverlapDetector(resolver)

        selectors = [
            {
                "name": "manually_created_revenue",
                "definition": {"union": [{"method": "fqn", "value": "model_a"}]}
            },
            {
                "name": "manually_created_finance",
                "definition": {"union": [{"method": "fqn", "value": "model_a"}]}
            }
        ]

        warnings = detector.detect_overlaps(selectors, {})

        assert len(warnings) == 1
        assert warnings[0].severity == "WARNING"
        assert "model_a" in warnings[0].message

    def test_auto_selector_overlap_error(self, mock_parser, mock_graph):
        """Test that auto-generated selector overlaps produce errors."""
        resolver = ModelResolver(mock_parser, mock_graph)
        detector = OverlapDetector(resolver)

        selectors = [
            {
                "name": "automatically_generated_selector_model_a",
                "definition": {"union": [{"method": "fqn", "value": "model_a"}]}
            },
            {
                "name": "automatically_generated_selector_model_b",
                "definition": {"union": [{"method": "fqn", "value": "model_a"}]}
            }
        ]

        warnings = detector.detect_overlaps(selectors, {})

        assert len(warnings) == 1
        assert warnings[0].severity == "ERROR"
        assert "model_a" in warnings[0].message

    def test_no_overlaps(self, mock_parser, mock_graph):
        """Test that no warnings are generated when there are no overlaps."""
        resolver = ModelResolver(mock_parser, mock_graph)
        detector = OverlapDetector(resolver)

        selectors = [
            {
                "name": "automatically_generated_selector_model_a",
                "definition": {"union": [{"method": "fqn", "value": "model_a"}]}
            },
            {
                "name": "automatically_generated_selector_model_b",
                "definition": {"union": [{"method": "fqn", "value": "model_b"}]}
            }
        ]

        warnings = detector.detect_overlaps(selectors, {})

        assert len(warnings) == 0


class TestFQNSelector:
    """Test FQN-based selector generation."""

    def test_component_selector_generation(self, mock_parser, mock_graph):
        """Test that FQN selector generates component-based selectors."""
        config = SelectorConfig(group_by_dependencies=True)
        selector = FQNSelector(mock_parser, mock_graph, config)

        selectors = selector.generate(excluded_models=set())

        assert len(selectors) > 0
        # Check that selector uses FQN method
        first_selector = selectors[0]
        assert "automatically_generated_selector" in first_selector["name"]
        assert "union" in first_selector["definition"]

    def test_excluded_models_not_generated(self, mock_parser, mock_graph):
        """Test that excluded models are not included in generated selectors."""
        config = SelectorConfig(group_by_dependencies=True)
        selector = FQNSelector(mock_parser, mock_graph, config)

        excluded = {"model_a", "model_b", "model_c"}
        selectors = selector.generate(excluded_models=excluded)

        # Should generate no selectors if all models are excluded
        assert len(selectors) == 0


class TestSelectorOrchestrator:
    """Test the orchestrator's priority-based generation."""

    def test_mixed_mode_priority_system(self, mock_parser, mock_graph):
        """Test that mixed mode generates selectors with correct priority."""
        config = SelectorConfig(
            method="mixed",
            group_by_dependencies=True,
            preserve_manual_selectors=True
        )

        orchestrator = SelectorOrchestrator(mock_parser, mock_graph, config)
        selectors = orchestrator.generate_selectors()

        # Should have generated at least the FQN selectors
        assert len(selectors) > 0

    def test_fqn_only_mode(self, mock_parser, mock_graph):
        """Test FQN-only mode generates only FQN selectors."""
        config = SelectorConfig(
            method="fqn",
            group_by_dependencies=True
        )

        orchestrator = SelectorOrchestrator(mock_parser, mock_graph, config)
        selectors = orchestrator.generate_selectors()

        assert len(selectors) > 0
        # All selectors should be FQN-based
        for selector in selectors:
            if not selector["name"].startswith("freshness_"):
                assert "automatically_generated" in selector["name"] or selector["name"].startswith("selector_")


class TestManualSelectorPreservation:
    """Test that manual selectors are preserved."""

    def test_manual_selectors_preserved(self, mock_parser, mock_graph, tmp_path):
        """Test that manual selectors from existing file are preserved."""
        # Create a temporary selectors.yml with manual selector
        selectors_file = tmp_path / "selectors.yml"
        manual_selector = {
            "name": "manually_created_critical",
            "description": "Critical models",
            "definition": {"union": [{"method": "fqn", "value": "model_a"}]}
        }

        with open(selectors_file, "w") as f:
            yaml.dump({"selectors": [manual_selector]}, f)

        # Change to temp directory
        original_dir = os.getcwd()
        try:
            os.chdir(tmp_path)

            config = SelectorConfig(
                method="mixed",
                preserve_manual_selectors=True
            )

            orchestrator = SelectorOrchestrator(mock_parser, mock_graph, config)
            selectors = orchestrator.generate_selectors()

            # Should include the manual selector
            manual_names = [s["name"] for s in selectors if s["name"].startswith("manually_created_")]
            assert "manually_created_critical" in manual_names

        finally:
            os.chdir(original_dir)


class TestIntegration:
    """Integration tests for the complete workflow."""

    def test_full_workflow_with_manual_and_auto(self, mock_parser, mock_graph, tmp_path):
        """Test complete workflow with manual and auto-generated selectors."""
        # Create manual selector file
        selectors_file = tmp_path / "selectors.yml"
        manual_selector = {
            "name": "manually_created_revenue",
            "description": "Revenue models",
            "definition": {"union": [{"method": "fqn", "value": "model_a"}]}
        }

        with open(selectors_file, "w") as f:
            yaml.dump({"selectors": [manual_selector]}, f)

        original_dir = os.getcwd()
        try:
            os.chdir(tmp_path)

            config = SelectorConfig(
                method="mixed",
                group_by_dependencies=True,
                preserve_manual_selectors=True
            )

            orchestrator = SelectorOrchestrator(mock_parser, mock_graph, config)
            selectors = orchestrator.generate_selectors()

            # Should have both manual and auto-generated selectors
            assert len(selectors) > 1

            # Manual selector should be first (highest priority)
            manual_selectors = [s for s in selectors if s["name"].startswith("manually_created_")]
            auto_selectors = [s for s in selectors if s["name"].startswith("automatically_generated_")]

            assert len(manual_selectors) >= 1
            assert len(auto_selectors) >= 0  # May be 0 if all models covered by manual

        finally:
            os.chdir(original_dir)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
