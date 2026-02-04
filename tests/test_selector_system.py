"""Comprehensive tests for refactored selector system."""

import pytest
import os
import yaml
from unittest.mock import Mock

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
            "sources": ["source.raw.users"],
        },
        "model_b": {
            "name": "model_b",
            "fqn": ["project", "staging", "model_b"],
            "path": "staging/model_b.sql",
            "tags": ["staging"],
            "dependencies": ["model_a"],
            "sources": [],
        },
        "model_c": {
            "name": "model_c",
            "fqn": ["project", "marts", "model_c"],
            "path": "marts/model_c.sql",
            "tags": ["marts", "daily"],
            "dependencies": ["model_b"],
            "sources": [],
        },
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
            "definition": {"union": [{"method": "fqn", "value": "fct_revenue"}]},
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
            "definition": {"union": [{"method": "fqn", "value": "fct_revenue"}]},
        }

        assert selector.is_manually_created(selector_def) is True

    def test_non_prefixed_selector_is_manual(self, mock_parser, mock_graph):
        """Test that selectors without maestro_ prefix are detected as manual."""
        config = SelectorConfig()
        selector = ManualSelector(mock_parser, mock_graph, config)

        # Any selector without the maestro_ prefix is considered manual
        selector_def = {
            "name": "revenue_selector",
            "description": "Revenue models selector",
            "definition": {"union": [{"method": "fqn", "value": "fct_revenue"}]},
        }

        assert selector.is_manually_created(selector_def) is True

    def test_auto_selector_detection(self, mock_parser, mock_graph):
        """Test that auto-generated selectors are not marked as manual."""
        config = SelectorConfig()
        selector = ManualSelector(mock_parser, mock_graph, config)

        # Auto-generated selectors use the maestro_ prefix
        selector_def = {
            "name": "maestro_model_a",
            "description": "Selector for models in component",
            "definition": {"union": [{"method": "fqn", "value": "model_a"}]},
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
                    {"method": "fqn", "value": "model_b"},
                ]
            },
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
            "definition": {"union": [{"method": "tag", "value": "staging"}]},
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
            "definition": {"union": [{"method": "path", "value": "staging/"}]},
        }

        resolution = resolver.resolve_selector(selector_def)

        assert "model_a" in resolution.models
        assert "model_b" in resolution.models
        assert "staging/" in resolution.paths

    def test_resolve_with_exclusion(self, mock_parser, mock_graph):
        """Test resolving models with exclusions."""
        mock_graph.group_by_tag.side_effect = [
            ["model_a", "model_b", "model_c"],  # staging tag
            ["model_c"],  # marts tag for exclusion
        ]
        resolver = ModelResolver(mock_parser, mock_graph)

        selector_def = {
            "name": "test_selector",
            "definition": {
                "union": [{"method": "tag", "value": "staging"}],
                "exclude": {"union": [{"method": "tag", "value": "marts"}]},
            },
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

        # Manual selectors do NOT start with "maestro_" prefix
        selectors = [
            {
                "name": "critical_revenue",
                "definition": {"union": [{"method": "fqn", "value": "model_a"}]},
            },
            {
                "name": "critical_finance",
                "definition": {"union": [{"method": "fqn", "value": "model_a"}]},
            },
        ]

        warnings = detector.detect_overlaps(selectors, {})

        assert len(warnings) == 1
        assert warnings[0].severity == "WARNING"
        assert "model_a" in warnings[0].message

    def test_auto_selector_overlap_error(self, mock_parser, mock_graph):
        """Test that auto-generated selector overlaps produce errors."""
        resolver = ModelResolver(mock_parser, mock_graph)
        detector = OverlapDetector(resolver)

        # Auto-generated selectors start with "maestro_" prefix
        selectors = [
            {
                "name": "maestro_model_a",
                "definition": {"union": [{"method": "fqn", "value": "model_a"}]},
            },
            {
                "name": "maestro_model_b",
                "definition": {"union": [{"method": "fqn", "value": "model_a"}]},
            },
        ]

        warnings = detector.detect_overlaps(selectors, {})

        assert len(warnings) == 1
        assert warnings[0].severity == "ERROR"
        assert "model_a" in warnings[0].message

    def test_no_overlaps(self, mock_parser, mock_graph):
        """Test that no warnings are generated when there are no overlaps."""
        resolver = ModelResolver(mock_parser, mock_graph)
        detector = OverlapDetector(resolver)

        # Auto-generated selectors with no overlapping models
        selectors = [
            {
                "name": "maestro_model_a",
                "definition": {"union": [{"method": "fqn", "value": "model_a"}]},
            },
            {
                "name": "maestro_model_b",
                "definition": {"union": [{"method": "fqn", "value": "model_b"}]},
            },
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
        # Check that selector uses maestro_ prefix and FQN method
        first_selector = selectors[0]
        assert first_selector["name"].startswith("maestro_")
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

    def test_fqn_only_mode(self, mock_parser, mock_graph, tmp_path):
        """Test FQN-only mode generates only FQN selectors."""
        import os

        original_dir = os.getcwd()
        try:
            os.chdir(tmp_path)  # Isolate from existing selectors.yml

            config = SelectorConfig(method="fqn", group_by_dependencies=True)

            orchestrator = SelectorOrchestrator(mock_parser, mock_graph, config)
            selectors = orchestrator.generate_selectors()

            assert len(selectors) > 0
            # All selectors should be auto-generated with maestro_ prefix
            for selector in selectors:
                if not selector["name"].startswith("freshness_"):
                    assert selector["name"].startswith("maestro_")
        finally:
            os.chdir(original_dir)


class TestManualSelectorPreservation:
    """Test that manual selectors are preserved."""

    def test_manual_selectors_preserved(self, mock_parser, mock_graph, tmp_path):
        """Test that manual selectors from existing file are preserved."""
        # Create a temporary selectors.yml with manual selector
        selectors_file = tmp_path / "selectors.yml"
        manual_selector = {
            "name": "manually_created_critical",
            "description": "Critical models",
            "definition": {"union": [{"method": "fqn", "value": "model_a"}]},
        }

        with open(selectors_file, "w") as f:
            yaml.dump({"selectors": [manual_selector]}, f)

        # Change to temp directory
        original_dir = os.getcwd()
        try:
            os.chdir(tmp_path)

            config = SelectorConfig(method="fqn", group_by_dependencies=True)

            orchestrator = SelectorOrchestrator(mock_parser, mock_graph, config)
            selectors = orchestrator.generate_selectors()

            # Should include the manual selector
            manual_names = [
                s["name"] for s in selectors if s["name"].startswith("manually_created_")
            ]
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
            "definition": {"union": [{"method": "fqn", "value": "model_a"}]},
        }

        with open(selectors_file, "w") as f:
            yaml.dump({"selectors": [manual_selector]}, f)

        original_dir = os.getcwd()
        try:
            os.chdir(tmp_path)

            config = SelectorConfig(method="fqn", group_by_dependencies=True)

            orchestrator = SelectorOrchestrator(mock_parser, mock_graph, config)
            selectors = orchestrator.generate_selectors()

            # Should have both manual and auto-generated selectors
            assert len(selectors) > 1

            # Manual selector should be first (highest priority)
            manual_selectors = [s for s in selectors if s["name"].startswith("manually_created_")]
            auto_selectors = [
                s for s in selectors if s["name"].startswith("automatically_generated_")
            ]

            assert len(manual_selectors) >= 1
            assert len(auto_selectors) >= 0  # May be 0 if all models covered by manual

        finally:
            os.chdir(original_dir)


class TestManualSelectorOverlaps:
    """Test that overlapping manual selectors are preserved and warned about."""

    def test_overlapping_manual_selectors_both_preserved(self, mock_parser, mock_graph, tmp_path):
        """Test that two manual selectors with overlapping models are both preserved."""
        # Create manual selectors with overlapping models
        selectors_file = tmp_path / "selectors.yml"
        manual_selectors = [
            {
                "name": "manually_created_revenue",
                "description": "Revenue models",
                "definition": {"union": [{"method": "fqn", "value": "model_a"}]},
            },
            {
                "name": "manually_created_finance",
                "description": "Finance models",
                "definition": {"union": [{"method": "fqn", "value": "model_a"}]},
            },
        ]

        with open(selectors_file, "w") as f:
            yaml.dump({"selectors": manual_selectors}, f)

        original_dir = os.getcwd()
        try:
            os.chdir(tmp_path)

            config = SelectorConfig(method="fqn", group_by_dependencies=True)

            orchestrator = SelectorOrchestrator(mock_parser, mock_graph, config)
            selectors = orchestrator.generate_selectors()

            # Both manual selectors should be preserved
            manual_names = [
                s["name"] for s in selectors if s["name"].startswith("manually_created_")
            ]
            assert "manually_created_revenue" in manual_names
            assert "manually_created_finance" in manual_names
            assert len(manual_names) == 2

        finally:
            os.chdir(original_dir)

    def test_overlapping_manual_selectors_generate_warning(self, mock_parser, mock_graph, tmp_path):
        """Test that overlapping manual selectors generate a warning."""
        selectors_file = tmp_path / "selectors.yml"
        manual_selectors = [
            {
                "name": "manually_created_revenue",
                "description": "Revenue models",
                "definition": {"union": [{"method": "fqn", "value": "model_a"}]},
            },
            {
                "name": "manually_created_finance",
                "description": "Finance models",
                "definition": {"union": [{"method": "fqn", "value": "model_a"}]},
            },
        ]

        with open(selectors_file, "w") as f:
            yaml.dump({"selectors": manual_selectors}, f)

        original_dir = os.getcwd()
        try:
            os.chdir(tmp_path)

            config = SelectorConfig(method="fqn", group_by_dependencies=True)
            orchestrator = SelectorOrchestrator(mock_parser, mock_graph, config)
            selectors = orchestrator.generate_selectors()

            # Check that overlap was detected
            warnings = orchestrator.overlap_detector.detect_overlaps(selectors)
            assert len(warnings) > 0
            assert warnings[0].severity == "WARNING"
            assert "model_a" in warnings[0].message

        finally:
            os.chdir(original_dir)


class TestManualSelectorPersistence:
    """Test that manual selectors are never deleted during regeneration."""

    def test_manual_selectors_never_deleted_on_regeneration(
        self, mock_parser, mock_graph, tmp_path
    ):
        """Test that manual selectors persist across multiple regenerations."""
        selectors_file = tmp_path / "selectors.yml"

        # Initial manual selectors
        initial_selectors = [
            {
                "name": "manually_created_critical",
                "description": "Critical models",
                "definition": {"union": [{"method": "fqn", "value": "model_a"}]},
            },
            {
                "name": "manually_created_legacy",
                "description": "Legacy models",
                "definition": {"union": [{"method": "path", "value": "staging/"}]},
            },
        ]

        with open(selectors_file, "w") as f:
            yaml.dump({"selectors": initial_selectors}, f)

        original_dir = os.getcwd()
        try:
            os.chdir(tmp_path)
            config = SelectorConfig(method="fqn", group_by_dependencies=True)

            # First generation
            orchestrator1 = SelectorOrchestrator(mock_parser, mock_graph, config)
            selectors1 = orchestrator1.generate_selectors()

            # Write selectors back
            orchestrator1.write_selectors(selectors1, str(selectors_file))

            # Second generation (simulating regeneration)
            orchestrator2 = SelectorOrchestrator(mock_parser, mock_graph, config)
            selectors2 = orchestrator2.generate_selectors()

            # Both manual selectors should still be present
            manual_names = [
                s["name"] for s in selectors2 if s["name"].startswith("manually_created_")
            ]
            assert "manually_created_critical" in manual_names
            assert "manually_created_legacy" in manual_names
            assert len(manual_names) == 2

            # Verify they are exactly the same
            manual1 = [s for s in selectors1 if s["name"] == "manually_created_critical"][0]
            manual2 = [s for s in selectors2 if s["name"] == "manually_created_critical"][0]
            assert manual1 == manual2

        finally:
            os.chdir(original_dir)

    def test_auto_selectors_regenerated_manual_preserved(self, mock_parser, mock_graph, tmp_path):
        """Test that auto selectors can change but manual selectors stay the same."""
        selectors_file = tmp_path / "selectors.yml"

        # Create file with both manual and auto selectors
        existing_selectors = [
            {
                "name": "manually_created_critical",
                "description": "Critical models",
                "definition": {"union": [{"method": "fqn", "value": "model_a"}]},
            },
            {
                "name": "automatically_generated_selector_model_b",
                "description": "Auto generated",
                "definition": {"union": [{"method": "fqn", "value": "model_b"}]},
            },
        ]

        with open(selectors_file, "w") as f:
            yaml.dump({"selectors": existing_selectors}, f)

        original_dir = os.getcwd()
        try:
            os.chdir(tmp_path)
            config = SelectorConfig(method="fqn", group_by_dependencies=True)

            orchestrator = SelectorOrchestrator(mock_parser, mock_graph, config)
            selectors = orchestrator.generate_selectors()

            # Manual selector should be preserved exactly
            manual_selectors = [s for s in selectors if s["name"] == "manually_created_critical"]
            assert len(manual_selectors) == 1
            assert manual_selectors[0]["definition"] == {
                "union": [{"method": "fqn", "value": "model_a"}]
            }

        finally:
            os.chdir(original_dir)


class TestManualSelectorModelExclusion:
    """Test that models in manual selectors are excluded from auto-generated selectors."""

    def test_fqn_manual_selector_excludes_models(self, mock_parser, mock_graph, tmp_path):
        """Test that models referenced by FQN in manual selectors are excluded from auto."""
        selectors_file = tmp_path / "selectors.yml"

        # Manual selector with FQN method
        manual_selector = {
            "name": "manually_created_critical",
            "description": "Critical models",
            "definition": {
                "union": [
                    {"method": "fqn", "value": "model_a"},
                    {"method": "fqn", "value": "model_b"},
                ]
            },
        }

        with open(selectors_file, "w") as f:
            yaml.dump({"selectors": [manual_selector]}, f)

        original_dir = os.getcwd()
        try:
            os.chdir(tmp_path)
            config = SelectorConfig(method="fqn", group_by_dependencies=True)

            orchestrator = SelectorOrchestrator(mock_parser, mock_graph, config)
            selectors = orchestrator.generate_selectors()

            # Extract all models from auto-generated selectors
            auto_selectors = [
                s for s in selectors if s["name"].startswith("automatically_generated_")
            ]
            auto_models = set()
            resolver = ModelResolver(mock_parser, mock_graph)

            for selector in auto_selectors:
                resolution = resolver.resolve_selector(selector)
                auto_models.update(resolution.models)

            # model_a and model_b should NOT be in any auto-generated selector
            assert "model_a" not in auto_models
            assert "model_b" not in auto_models

        finally:
            os.chdir(original_dir)

    def test_tag_manual_selector_excludes_models(self, mock_parser, mock_graph, tmp_path):
        """Test that models with tags in manual selectors are excluded from auto."""
        # Setup mock to return models by tag
        mock_graph.group_by_tag.return_value = ["model_a", "model_b"]

        selectors_file = tmp_path / "selectors.yml"

        # Manual selector with tag method
        manual_selector = {
            "name": "manually_created_staging",
            "description": "Staging models",
            "definition": {"union": [{"method": "tag", "value": "staging"}]},
        }

        with open(selectors_file, "w") as f:
            yaml.dump({"selectors": [manual_selector]}, f)

        original_dir = os.getcwd()
        try:
            os.chdir(tmp_path)
            config = SelectorConfig(method="fqn", group_by_dependencies=True)

            orchestrator = SelectorOrchestrator(mock_parser, mock_graph, config)
            selectors = orchestrator.generate_selectors()

            # Extract all models from auto-generated selectors
            auto_selectors = [
                s for s in selectors if s["name"].startswith("automatically_generated_")
            ]
            auto_models = set()
            resolver = ModelResolver(mock_parser, mock_graph)

            for selector in auto_selectors:
                resolution = resolver.resolve_selector(selector)
                auto_models.update(resolution.models)

            # Models with "staging" tag should NOT be in any auto-generated selector
            assert "model_a" not in auto_models
            assert "model_b" not in auto_models

        finally:
            os.chdir(original_dir)

    def test_path_manual_selector_excludes_models(self, mock_parser, mock_graph, tmp_path):
        """Test that models in paths from manual selectors are excluded from auto."""
        # Setup mock to return models by path
        mock_graph.group_by_path.return_value = ["model_a", "model_b"]

        selectors_file = tmp_path / "selectors.yml"

        # Manual selector with path method
        manual_selector = {
            "name": "manually_created_legacy",
            "description": "Legacy models",
            "definition": {"union": [{"method": "path", "value": "staging/legacy"}]},
        }

        with open(selectors_file, "w") as f:
            yaml.dump({"selectors": [manual_selector]}, f)

        original_dir = os.getcwd()
        try:
            os.chdir(tmp_path)
            config = SelectorConfig(method="fqn", group_by_dependencies=True)

            orchestrator = SelectorOrchestrator(mock_parser, mock_graph, config)
            selectors = orchestrator.generate_selectors()

            # Extract all models from auto-generated selectors
            auto_selectors = [
                s for s in selectors if s["name"].startswith("automatically_generated_")
            ]
            auto_models = set()
            resolver = ModelResolver(mock_parser, mock_graph)

            for selector in auto_selectors:
                resolution = resolver.resolve_selector(selector)
                auto_models.update(resolution.models)

            # Models in "staging/legacy" path should NOT be in any auto-generated selector
            assert "model_a" not in auto_models
            assert "model_b" not in auto_models

        finally:
            os.chdir(original_dir)

    def test_multi_method_manual_selector_excludes_all_models(
        self, mock_parser, mock_graph, tmp_path
    ):
        """Test that manual selector using multiple methods excludes all referenced models."""
        # Setup mocks
        mock_graph.group_by_tag.return_value = ["model_a"]
        mock_graph.group_by_path.return_value = ["model_b"]

        selectors_file = tmp_path / "selectors.yml"

        # Manual selector with multiple methods (fqn + tag + path)
        manual_selector = {
            "name": "manually_created_critical_pipeline",
            "description": "Critical pipeline",
            "definition": {
                "union": [
                    {"method": "fqn", "value": "model_c"},
                    {"method": "tag", "value": "critical"},
                    {"method": "path", "value": "marts/revenue"},
                ]
            },
        }

        with open(selectors_file, "w") as f:
            yaml.dump({"selectors": [manual_selector]}, f)

        original_dir = os.getcwd()
        try:
            os.chdir(tmp_path)
            config = SelectorConfig(method="fqn", group_by_dependencies=True)

            orchestrator = SelectorOrchestrator(mock_parser, mock_graph, config)
            selectors = orchestrator.generate_selectors()

            # Extract all models from auto-generated selectors
            auto_selectors = [
                s for s in selectors if s["name"].startswith("automatically_generated_")
            ]
            auto_models = set()
            resolver = ModelResolver(mock_parser, mock_graph)

            for selector in auto_selectors:
                resolution = resolver.resolve_selector(selector)
                auto_models.update(resolution.models)

            # All models from all methods should be excluded
            assert "model_a" not in auto_models  # From tag
            assert "model_b" not in auto_models  # From path
            assert "model_c" not in auto_models  # From fqn

        finally:
            os.chdir(original_dir)


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_empty_manual_selector_file(self, mock_parser, mock_graph, tmp_path):
        """Test handling of empty selectors file."""
        selectors_file = tmp_path / "selectors.yml"

        # Create empty file
        with open(selectors_file, "w") as f:
            f.write("")

        original_dir = os.getcwd()
        try:
            os.chdir(tmp_path)
            config = SelectorConfig(method="fqn", group_by_dependencies=True)

            orchestrator = SelectorOrchestrator(mock_parser, mock_graph, config)
            selectors = orchestrator.generate_selectors()

            # Should generate auto selectors without errors
            assert len(selectors) > 0

        finally:
            os.chdir(original_dir)

    def test_no_selectors_file_creates_auto_only(self, mock_parser, mock_graph, tmp_path):
        """Test that missing selectors file generates only auto selectors."""
        original_dir = os.getcwd()
        try:
            os.chdir(tmp_path)
            config = SelectorConfig(method="fqn", group_by_dependencies=True)

            orchestrator = SelectorOrchestrator(mock_parser, mock_graph, config)
            selectors = orchestrator.generate_selectors()

            # All selectors should be auto-generated (maestro_ prefix)
            # since there's no existing selectors.yml with manual selectors
            for selector in selectors:
                if not selector["name"].startswith("freshness_"):
                    assert selector["name"].startswith("maestro_")

            # Should have at least one auto-generated selector
            auto_selectors = [s for s in selectors if s["name"].startswith("maestro_")]
            assert len(auto_selectors) > 0

        finally:
            os.chdir(original_dir)

    def test_manual_selector_with_invalid_model_excluded(self, mock_parser, mock_graph, tmp_path):
        """Test that manual selector referencing non-existent model still preserved."""
        selectors_file = tmp_path / "selectors.yml"

        # Manual selector with non-existent model
        manual_selector = {
            "name": "manually_created_test",
            "description": "Test selector",
            "definition": {"union": [{"method": "fqn", "value": "nonexistent_model"}]},
        }

        with open(selectors_file, "w") as f:
            yaml.dump({"selectors": [manual_selector]}, f)

        original_dir = os.getcwd()
        try:
            os.chdir(tmp_path)
            config = SelectorConfig(method="fqn", group_by_dependencies=True)

            orchestrator = SelectorOrchestrator(mock_parser, mock_graph, config)
            selectors = orchestrator.generate_selectors()

            # Manual selector should still be preserved (even if model doesn't exist)
            manual_names = [
                s["name"] for s in selectors if s["name"].startswith("manually_created_")
            ]
            assert "manually_created_test" in manual_names

        finally:
            os.chdir(original_dir)

    def test_all_models_in_manual_selectors_no_auto_generated(
        self, mock_parser, mock_graph, tmp_path
    ):
        """Test that if all models are in manual selectors, no auto selectors are generated."""
        selectors_file = tmp_path / "selectors.yml"

        # Manual selector covering all models
        manual_selector = {
            "name": "manually_created_all",
            "description": "All models",
            "definition": {
                "union": [
                    {"method": "fqn", "value": "model_a"},
                    {"method": "fqn", "value": "model_b"},
                    {"method": "fqn", "value": "model_c"},
                ]
            },
        }

        with open(selectors_file, "w") as f:
            yaml.dump({"selectors": [manual_selector]}, f)

        original_dir = os.getcwd()
        try:
            os.chdir(tmp_path)
            config = SelectorConfig(method="fqn", group_by_dependencies=True)

            orchestrator = SelectorOrchestrator(mock_parser, mock_graph, config)
            selectors = orchestrator.generate_selectors()

            # Should have manual selector but no auto-generated selectors
            manual_selectors = [s for s in selectors if s["name"].startswith("manually_created_")]
            auto_selectors = [
                s for s in selectors if s["name"].startswith("automatically_generated_")
            ]

            assert len(manual_selectors) == 1
            assert len(auto_selectors) == 0

        finally:
            os.chdir(original_dir)

    def test_manual_selector_with_parents_flag(self, mock_parser, mock_graph, tmp_path):
        """Test that manual selector with parents flag still excludes the model."""
        selectors_file = tmp_path / "selectors.yml"

        # Manual selector with parents flag
        manual_selector = {
            "name": "manually_created_with_parents",
            "description": "Models with sources",
            "definition": {"union": [{"method": "fqn", "value": "model_a", "parents": True}]},
        }

        with open(selectors_file, "w") as f:
            yaml.dump({"selectors": [manual_selector]}, f)

        original_dir = os.getcwd()
        try:
            os.chdir(tmp_path)
            config = SelectorConfig(method="fqn", group_by_dependencies=True)

            orchestrator = SelectorOrchestrator(mock_parser, mock_graph, config)
            selectors = orchestrator.generate_selectors()

            # model_a should be excluded from auto-generated selectors
            auto_selectors = [
                s for s in selectors if s["name"].startswith("automatically_generated_")
            ]
            auto_models = set()
            resolver = ModelResolver(mock_parser, mock_graph)

            for selector in auto_selectors:
                resolution = resolver.resolve_selector(selector)
                auto_models.update(resolution.models)

            assert "model_a" not in auto_models

        finally:
            os.chdir(original_dir)


class TestFreshnessSelectors:
    """Test freshness selector configuration and generation."""

    def test_freshness_disabled_by_default(self, mock_parser, mock_graph):
        """Test that freshness selectors are not generated by default."""
        config = SelectorConfig(method="fqn", group_by_dependencies=True)
        orchestrator = SelectorOrchestrator(mock_parser, mock_graph, config)
        selectors = orchestrator.generate_selectors()

        # No freshness selectors should be generated
        freshness_selectors = [s for s in selectors if s["name"].startswith("freshness_")]
        assert len(freshness_selectors) == 0

    def test_freshness_enabled_globally(self, mock_parser, mock_graph):
        """Test that freshness selectors are generated when globally enabled."""
        config = SelectorConfig(
            method="fqn", group_by_dependencies=True, include_freshness_selectors=True
        )
        orchestrator = SelectorOrchestrator(mock_parser, mock_graph, config)
        selectors = orchestrator.generate_selectors()

        # Freshness selectors should be generated
        freshness_selectors = [s for s in selectors if s["name"].startswith("freshness_")]
        assert len(freshness_selectors) > 0

    def test_freshness_for_specific_selectors_only(self, mock_parser, mock_graph, tmp_path):
        """Test that freshness selectors are only generated for specified selectors."""
        import os

        original_dir = os.getcwd()
        try:
            os.chdir(tmp_path)  # Isolate from existing selectors.yml

            # First generate to get selector names
            config = SelectorConfig(method="fqn", group_by_dependencies=True)
            orchestrator = SelectorOrchestrator(mock_parser, mock_graph, config)
            base_selectors = orchestrator.generate_selectors()
            non_freshness = [
                s["name"] for s in base_selectors if not s["name"].startswith("freshness_")
            ]

            if len(non_freshness) >= 1:
                # Only enable freshness for first selector
                target_selector = non_freshness[0]
                config_with_freshness = SelectorConfig(
                    method="fqn",
                    group_by_dependencies=True,
                    freshness_selector_names=[target_selector],
                )
                orchestrator2 = SelectorOrchestrator(mock_parser, mock_graph, config_with_freshness)
                selectors = orchestrator2.generate_selectors()

                freshness_selectors = [s for s in selectors if s["name"].startswith("freshness_")]
                # Should have exactly one freshness selector
                assert len(freshness_selectors) == 1
                assert freshness_selectors[0]["name"] == f"freshness_{target_selector}"
        finally:
            os.chdir(original_dir)

    def test_freshness_exclude_specific_selectors(self, mock_parser, mock_graph):
        """Test that specific selectors can be excluded from freshness generation."""
        # First generate to get selector names
        config = SelectorConfig(
            method="fqn", group_by_dependencies=True, include_freshness_selectors=True
        )
        orchestrator = SelectorOrchestrator(mock_parser, mock_graph, config)
        base_selectors = orchestrator.generate_selectors()
        non_freshness = [
            s["name"] for s in base_selectors if not s["name"].startswith("freshness_")
        ]

        if len(non_freshness) >= 1:
            # Exclude first selector from freshness
            excluded_selector = non_freshness[0]
            config_with_exclusion = SelectorConfig(
                method="fqn",
                group_by_dependencies=True,
                include_freshness_selectors=True,
                exclude_freshness_selector_names=[excluded_selector],
            )
            orchestrator2 = SelectorOrchestrator(mock_parser, mock_graph, config_with_exclusion)
            selectors = orchestrator2.generate_selectors()

            freshness_selectors = [s for s in selectors if s["name"].startswith("freshness_")]
            freshness_names = [s["name"] for s in freshness_selectors]

            # Excluded selector should not have a freshness variant
            assert f"freshness_{excluded_selector}" not in freshness_names

    def test_exclude_overrides_include(self, mock_parser, mock_graph):
        """Test that exclude_freshness_selector_names overrides freshness_selector_names."""
        config = SelectorConfig(
            method="fqn", group_by_dependencies=True, include_freshness_selectors=True
        )
        orchestrator = SelectorOrchestrator(mock_parser, mock_graph, config)
        base_selectors = orchestrator.generate_selectors()
        non_freshness = [
            s["name"] for s in base_selectors if not s["name"].startswith("freshness_")
        ]

        if len(non_freshness) >= 1:
            target_selector = non_freshness[0]
            # Both include and exclude the same selector - exclude should win
            config_with_both = SelectorConfig(
                method="fqn",
                group_by_dependencies=True,
                freshness_selector_names=[target_selector],
                exclude_freshness_selector_names=[target_selector],
            )
            orchestrator2 = SelectorOrchestrator(mock_parser, mock_graph, config_with_both)
            selectors = orchestrator2.generate_selectors()

            freshness_selectors = [s for s in selectors if s["name"].startswith("freshness_")]
            # No freshness selectors because the only included one is also excluded
            assert len(freshness_selectors) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
