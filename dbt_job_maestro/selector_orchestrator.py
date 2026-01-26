"""Orchestrate selector generation across all types."""

import os
import yaml
import logging
from typing import List, Dict, Any, Set

from dbt_job_maestro.selector_types import SelectorPriority
from dbt_job_maestro.model_resolver import ModelResolver
from dbt_job_maestro.overlap_detector import OverlapDetector
from dbt_job_maestro.selectors import ManualSelector, FQNSelector

logger = logging.getLogger(__name__)


class SelectorOrchestrator:
    """Orchestrates selector generation using different strategies.

    Supports three methods:
    - fqn: Groups models by dependencies (allows group_by_dependencies)
    - path: One selector per path (no dependency grouping)
    - tag: One selector per tag (no dependency grouping)

    Manual selectors (those not starting with the configured prefix) are always
    preserved and their models excluded from auto-generation to prevent duplicates.
    """

    def __init__(self, manifest_parser, graph_builder, config):
        """Initialize the selector orchestrator.

        Args:
            manifest_parser: ManifestParser instance
            graph_builder: GraphBuilder instance
            config: SelectorConfig instance
        """
        self.parser = manifest_parser
        self.graph = graph_builder
        self.config = config
        self.models = manifest_parser.get_models()

        # Initialize model resolver
        self.resolver = ModelResolver(manifest_parser, graph_builder)

        # Initialize overlap detector with selector prefix from config
        self.overlap_detector = OverlapDetector(
            self.resolver, selector_prefix=config.selector_prefix
        )

        # Initialize selector generators
        self.generators = {
            SelectorPriority.MANUAL: ManualSelector(manifest_parser, graph_builder, config),
            SelectorPriority.AUTO_FQN: FQNSelector(manifest_parser, graph_builder, config),
        }

    def generate_selectors(self) -> List[Dict[str, Any]]:
        """Generate selectors based on configuration.

        Manual selectors are always preserved and their models excluded from
        auto-generation to prevent duplicates.

        Returns:
            List of selector definitions
        """
        method = self.config.method

        if method == "fqn":
            selectors = self._generate_fqn_mode()
        elif method == "path":
            selectors = self._generate_path_mode()
        elif method == "tag":
            selectors = self._generate_tag_mode()
        else:
            raise ValueError(f"Unknown selector method: {method}")

        # Check for overlaps if configured
        if self.config.warn_on_manual_overlaps:
            warnings = self.overlap_detector.detect_overlaps(selectors)
            self.overlap_detector.report_overlaps(warnings)

        return selectors

    def _get_config_excluded_models(self) -> Set[str]:
        """Get models to exclude based on config's exclude_paths and exclude_models.

        Returns:
            Set of model names to exclude from selector generation
        """
        excluded = set()

        # Exclude models matching exclude_paths
        if self.config.exclude_paths:
            path_excluded = self.graph.get_models_in_paths(self.config.exclude_paths)
            if path_excluded:
                logger.info(
                    f"Excluding {len(path_excluded)} models based on exclude_paths: "
                    f"{', '.join(self.config.exclude_paths)}"
                )
            excluded.update(path_excluded)

        # Exclude models by name
        if self.config.exclude_models:
            model_excluded = self.graph.get_models_by_names(self.config.exclude_models)
            if model_excluded:
                logger.info(
                    f"Excluding {len(model_excluded)} models based on exclude_models: "
                    f"{', '.join(model_excluded)}"
                )
            excluded.update(model_excluded)

        return excluded

    def _get_manual_selectors_and_excluded_models(self) -> tuple:
        """Get manual selectors and the models they cover.

        Manual selectors are always preserved. Their covered models are excluded
        from auto-generation to prevent duplicates.

        Returns:
            Tuple of (manual_selectors list, excluded_models set)
        """
        manual_gen = self.generators[SelectorPriority.MANUAL]
        manual_selectors = manual_gen.generate(excluded_models=set())
        excluded_models = set()

        if manual_selectors:
            logger.info(f"Preserved {len(manual_selectors)} manual selectors")

            for selector in manual_selectors:
                metadata = manual_gen.extract_metadata(selector)

                # Warn about invalid FQN references
                if metadata.invalid_fqns:
                    logger.warning(
                        f"  Manual selector '{selector['name']}' references "
                        f"{len(metadata.invalid_fqns)} models that no longer exist:"
                    )
                    for invalid_fqn in sorted(metadata.invalid_fqns)[:5]:
                        logger.warning(f"      - {invalid_fqn}")
                    if len(metadata.invalid_fqns) > 5:
                        logger.warning(f"      ... and {len(metadata.invalid_fqns) - 5} more")

                # Exclude models from auto-generation
                if metadata.models_covered:
                    logger.info(
                        f"  Manual selector '{selector['name']}' covers "
                        f"{len(metadata.models_covered)} models"
                    )
                    excluded_models.update(metadata.models_covered)

        return manual_selectors, excluded_models

    def _generate_fqn_mode(self) -> List[Dict[str, Any]]:
        """Generate FQN-based selectors with manual selector preservation.

        Manual selectors are always preserved. Auto-generated FQN selectors
        exclude models already covered by manual selectors.

        Returns:
            List of selector definitions (manual + auto-generated)
        """
        all_selectors = []

        # Start with models excluded by config
        excluded_models = self._get_config_excluded_models()

        # Get manual selectors and their covered models
        manual_selectors, manual_excluded = self._get_manual_selectors_and_excluded_models()
        excluded_models.update(manual_excluded)
        all_selectors.extend(manual_selectors)

        # Generate FQN-based selectors for remaining models
        fqn_gen = self.generators[SelectorPriority.AUTO_FQN]
        fqn_selectors = fqn_gen.generate(excluded_models=excluded_models)

        logger.info(f"Generated {len(fqn_selectors)} FQN-based selectors")
        if excluded_models:
            logger.info(f"  ({len(excluded_models)} models excluded from auto-generation)")

        all_selectors.extend(fqn_selectors)

        return all_selectors

    def _generate_path_mode(self) -> List[Dict[str, Any]]:
        """Generate path-based selectors (one per path).

        Creates selectors based on directory structure. Manual selectors are
        preserved and their models excluded from path selectors.

        Returns:
            List of path-based selector definitions
        """
        all_selectors = []

        # Start with models excluded by config
        excluded_models = self._get_config_excluded_models()

        # Get manual selectors and their covered models
        manual_selectors, manual_excluded = self._get_manual_selectors_and_excluded_models()
        excluded_models.update(manual_excluded)
        all_selectors.extend(manual_selectors)

        # Get path prefixes at configured level
        path_prefixes = self.parser.get_path_prefixes(self.config.path_grouping_level)

        for path_prefix in sorted(path_prefixes):
            models = self.graph.group_by_path(path_prefix)
            # Filter out excluded models
            models = [m for m in models if m not in excluded_models]

            if len(models) >= self.config.min_models_per_selector:
                selector_name = self._path_to_selector_name(path_prefix)
                selector = {
                    "name": f"{self.config.selector_prefix}_path_{selector_name}",
                    "description": f"Selector for models in {path_prefix}",
                    "definition": {"union": [{"method": "path", "value": path_prefix}]},
                }

                # Add tag exclusions if configured
                if self.config.exclude_tags:
                    selector["definition"]["union"].append(self._create_tag_exclusion())

                all_selectors.append(selector)

                # Create freshness selector if configured
                if self._should_create_freshness(selector["name"]):
                    freshness = self._create_freshness_selector(selector["name"])
                    all_selectors.append(freshness)

        logger.info(f"Generated {len(all_selectors) - len(manual_selectors)} path-based selectors")

        return all_selectors

    def _generate_tag_mode(self) -> List[Dict[str, Any]]:
        """Generate tag-based selectors (one per tag).

        Creates selectors based on dbt tags. Warns about models without tags.
        Manual selectors are preserved and their models excluded.

        Returns:
            List of tag-based selector definitions
        """
        all_selectors = []

        # Start with models excluded by config
        excluded_models = self._get_config_excluded_models()

        # Get manual selectors and their covered models
        manual_selectors, manual_excluded = self._get_manual_selectors_and_excluded_models()
        excluded_models.update(manual_excluded)
        all_selectors.extend(manual_selectors)

        # Get all unique tags
        all_tags = self.parser.get_all_tags()

        # Filter out excluded tags
        included_tags = all_tags - set(self.config.exclude_tags)

        # Track tagged models for warning
        all_model_names = set(self.models.keys()) - excluded_models
        tagged_models = set()

        for tag in sorted(included_tags):
            models = self.graph.group_by_tag(tag)
            # Filter out excluded models
            models = [m for m in models if m not in excluded_models]
            tagged_models.update(models)

            if len(models) >= self.config.min_models_per_selector:
                selector = {
                    "name": f"{self.config.selector_prefix}_tag_{tag}",
                    "description": f"Selector for models tagged with {tag}",
                    "definition": {"union": [{"method": "tag", "value": tag}]},
                }

                all_selectors.append(selector)

                # Create freshness selector if configured
                if self._should_create_freshness(selector["name"]):
                    freshness = self._create_freshness_selector(selector["name"])
                    all_selectors.append(freshness)

        # Warn about untagged models
        untagged_models = all_model_names - tagged_models
        if untagged_models:
            logger.warning(
                f"\n⚠️  WARNING: {len(untagged_models)} model(s) have no tags and will NOT be "
                f"included in any selector when using method='tag':"
            )
            for model in sorted(untagged_models)[:10]:
                logger.warning(f"    - {model}")
            if len(untagged_models) > 10:
                logger.warning(f"    ... and {len(untagged_models) - 10} more")
            logger.warning(
                "\n💡 RECOMMENDATION: Use method='fqn' to ensure all models are included "
                "in selectors. The 'fqn' method groups models by dependencies for complete coverage."
            )

        logger.info(f"Generated {len(all_selectors) - len(manual_selectors)} tag-based selectors")

        return all_selectors

    def _path_to_selector_name(self, path: str) -> str:
        """Convert a path to a valid selector name.

        Args:
            path: Directory path (e.g., 'models/staging/core')

        Returns:
            Sanitized selector name (e.g., 'staging_core')
        """
        # Remove common prefixes and convert to underscore-separated
        parts = path.replace("models/", "").replace("/", "_").strip("_")
        return parts or "root"

    def _create_tag_exclusion(self) -> Dict[str, Any]:
        """Create tag exclusion definition.

        Returns:
            Exclusion definition dictionary
        """
        return {
            "exclude": {
                "union": [{"method": "tag", "value": tag} for tag in self.config.exclude_tags]
            }
        }

    def _create_freshness_selector(self, base_name: str) -> Dict[str, Any]:
        """Create freshness selector.

        Args:
            base_name: Base selector name

        Returns:
            Freshness selector definition dictionary
        """
        return {
            "name": f"freshness_{base_name}",
            "description": f"Freshness selector for {base_name}",
            "definition": {
                "union": [
                    {
                        "intersection": [
                            {"method": "selector", "value": base_name},
                            {"method": "source_status", "value": "fresher", "children": True},
                        ]
                    }
                ]
            },
        }

    def _should_create_freshness(self, selector_name: str) -> bool:
        """Determine if a freshness selector should be created for this selector.

        Args:
            selector_name: Name of the selector to check

        Returns:
            True if freshness selector should be created, False otherwise
        """
        if self.config.freshness_selector_names:
            return selector_name in self.config.freshness_selector_names
        return self.config.include_freshness_selectors

    def write_selectors(self, selectors: List[Dict[str, Any]], output_path: str) -> None:
        """Write selectors to YAML file with blank lines between selectors.

        Args:
            selectors: List of selector definitions
            output_path: Path to output file
        """
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

        with open(output_path, "w") as f:
            # Write the selectors key
            f.write("selectors:\n")

            # Write each selector with a blank line after it
            for i, selector in enumerate(selectors):
                # Convert selector to YAML
                selector_yaml = yaml.dump(
                    [selector], default_flow_style=False, sort_keys=False, indent=2
                )

                # Remove the leading "- " from the first line and adjust indentation
                lines = selector_yaml.split("\n")
                if lines and lines[0].startswith("- "):
                    lines[0] = "  - " + lines[0][2:]  # Add proper indentation
                    for j in range(1, len(lines)):
                        if lines[j]:  # Only add indentation to non-empty lines
                            lines[j] = "  " + lines[j]

                # Write the selector
                f.write("\n".join(lines))

                # Add blank line between selectors (but not after the last one)
                if i < len(selectors) - 1:
                    f.write("\n")
