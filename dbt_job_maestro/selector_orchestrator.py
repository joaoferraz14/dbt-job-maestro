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

    def _generate_fqn_only(self) -> List[Dict[str, Any]]:
        """Generate only FQN-based selectors.

        Returns:
            List of FQN selector definitions
        """
        generator = self.generators[SelectorPriority.AUTO_FQN]
        return generator.generate(excluded_models=set())

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
        excluded_models = set()

        # Priority 1: Manual selectors (HIGHEST)
        if self.config.preserve_manual_selectors:
            manual_gen = self.generators[SelectorPriority.MANUAL]
            manual_selectors = manual_gen.generate(excluded_models=set())

            logger.info(f"Preserved {len(manual_selectors)} manual selectors")

            # Extract metadata and track exclusions
            for selector in manual_selectors:
                metadata = manual_gen.extract_metadata(selector)
                selector_metadata[selector["name"]] = metadata

                # Exclude models from future generation
                excluded_models.update(metadata.models_covered)

            all_selectors.extend(manual_selectors)

        # Priority 2: FQN-based selectors (auto-generated)
        if self.config.group_by_dependencies:
            fqn_gen = self.generators[SelectorPriority.AUTO_FQN]
            fqn_selectors = fqn_gen.generate(excluded_models=excluded_models)

            logger.info(
                f"Generated {len(fqn_selectors)} FQN-based selectors "
                f"(excluding {len(excluded_models)} models)"
            )

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
