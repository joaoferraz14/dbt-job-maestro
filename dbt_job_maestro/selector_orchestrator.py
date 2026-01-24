"""Orchestrate selector generation across all types with priority handling."""

import os
import yaml
import logging
from typing import List, Dict, Any

from dbt_job_maestro.selector_types import SelectorPriority
from dbt_job_maestro.model_resolver import ModelResolver
from dbt_job_maestro.overlap_detector import OverlapDetector
from dbt_job_maestro.selectors import ManualSelector, FQNSelector

logger = logging.getLogger(__name__)


class SelectorOrchestrator:
    """Orchestrates selector generation across all types with priority handling.

    This class coordinates the generation of selectors using different strategies
    (manual, FQN) with a priority-based system to prevent duplicates.
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

        # Initialize model resolver
        self.resolver = ModelResolver(manifest_parser, graph_builder)

        # Initialize overlap detector
        self.overlap_detector = OverlapDetector(self.resolver)

        # Initialize selector generators (in priority order)
        self.generators = {
            SelectorPriority.MANUAL: ManualSelector(
                manifest_parser, graph_builder, config
            ),
            SelectorPriority.AUTO_FQN: FQNSelector(
                manifest_parser, graph_builder, config
            ),
        }

    def generate_selectors(self) -> List[Dict[str, Any]]:
        """Generate selectors based on configuration.

        Returns:
            List of selector definitions
        """
        method = self.config.method

        if method == "fqn":
            return self._generate_fqn_only()
        elif method == "mixed":
            return self._generate_mixed()
        else:
            raise ValueError(f"Unknown selector method: {method}")

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

    def _generate_fqn_only(self) -> List[Dict[str, Any]]:
        """Generate only FQN-based selectors.

        Returns:
            List of FQN selector definitions
        """
        # Get models excluded by config (exclude_paths and exclude_models)
        config_excluded = self._get_config_excluded_models()

        generator = self.generators[SelectorPriority.AUTO_FQN]
        return generator.generate(excluded_models=config_excluded)

    def _generate_mixed(self) -> List[Dict[str, Any]]:
        """Generate selectors using mixed mode with priority system.

        Priority order (highest to lowest):
        1. Manual selectors (preserved from existing file)
        2. FQN-based selectors (auto-generated)

        Returns:
            List of all selectors with no duplicates
        """
        all_selectors = []
        selector_metadata = {}

        # Start with models excluded by config (exclude_paths and exclude_models)
        excluded_models = self._get_config_excluded_models()

        # Priority 1: Manual selectors (HIGHEST)
        if self.config.preserve_manual_selectors:
            manual_gen = self.generators[SelectorPriority.MANUAL]
            manual_selectors = manual_gen.generate(excluded_models=set())

            logger.info(f"Preserved {len(manual_selectors)} manual selectors")

            # Extract metadata and track exclusions
            for selector in manual_selectors:
                metadata = manual_gen.extract_metadata(selector)
                selector_metadata[selector["name"]] = metadata

                # Warn about invalid FQN references
                if metadata.invalid_fqns:
                    logger.warning(
                        f"  ⚠️  Manual selector '{selector['name']}' references {len(metadata.invalid_fqns)} "
                        f"models that no longer exist in the manifest:"
                    )
                    for invalid_fqn in sorted(metadata.invalid_fqns):
                        logger.warning(f"      - {invalid_fqn}")

                # Log which models are being excluded by this manual selector
                if metadata.models_covered:
                    logger.info(
                        f"  Manual selector '{selector['name']}' covers {len(metadata.models_covered)} models, "
                        f"excluding them from auto-generation"
                    )
                    # Log first few models as examples
                    example_models = sorted(list(metadata.models_covered))[:5]
                    logger.info(f"    Example models: {', '.join(example_models)}")
                    if len(metadata.models_covered) > 5:
                        logger.info(f"    ... and {len(metadata.models_covered) - 5} more")

                # Exclude models from future generation
                excluded_models.update(metadata.models_covered)

            all_selectors.extend(manual_selectors)

        # Priority 2: FQN-based selectors (auto-generated)
        if self.config.group_by_dependencies:
            fqn_gen = self.generators[SelectorPriority.AUTO_FQN]
            fqn_selectors = fqn_gen.generate(excluded_models=excluded_models)

            logger.info(
                f"Generated {len(fqn_selectors)} FQN-based selectors "
                f"(excluding {len(excluded_models)} models from auto-generation due to manual selectors)"
            )

            if excluded_models:
                logger.info(
                    f"  The following {len(excluded_models)} models were excluded from auto-generation "
                    f"because they are covered by manual selectors:"
                )
                example_excluded = sorted(list(excluded_models))[:10]
                logger.info(f"    {', '.join(example_excluded)}")
                if len(excluded_models) > 10:
                    logger.info(f"    ... and {len(excluded_models) - 10} more")

            # Extract metadata and update exclusions
            for selector in fqn_selectors:
                metadata = fqn_gen.extract_metadata(selector)
                selector_metadata[selector["name"]] = metadata
                excluded_models.update(metadata.models_covered)

            all_selectors.extend(fqn_selectors)

        # Detect and report overlaps
        overlap_warnings = self.overlap_detector.detect_overlaps(
            all_selectors,
            selector_metadata
        )
        self.overlap_detector.report_overlaps(overlap_warnings)

        # Report summary of invalid FQNs
        total_invalid_fqns = sum(
            len(meta.invalid_fqns) for meta in selector_metadata.values()
        )
        if total_invalid_fqns > 0:
            logger.warning(
                f"\n⚠️  Found {total_invalid_fqns} invalid FQN references across all selectors. "
                f"These models no longer exist in the manifest."
            )

        return all_selectors

    def write_selectors(
        self,
        selectors: List[Dict[str, Any]],
        output_path: str
    ) -> None:
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
                    [selector],
                    default_flow_style=False,
                    sort_keys=False,
                    indent=2
                )

                # Remove the leading "- " from the first line and adjust indentation
                lines = selector_yaml.split('\n')
                if lines and lines[0].startswith('- '):
                    lines[0] = '  - ' + lines[0][2:]  # Add proper indentation
                    for j in range(1, len(lines)):
                        if lines[j]:  # Only add indentation to non-empty lines
                            lines[j] = '  ' + lines[j]

                # Write the selector
                f.write('\n'.join(lines))

                # Add blank line between selectors (but not after the last one)
                if i < len(selectors) - 1:
                    f.write('\n')
