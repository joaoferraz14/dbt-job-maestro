"""Manual selector handler."""

import os
import yaml
from typing import Dict, List, Any, Set
from pathlib import Path

from dbt_job_maestro.base_selector import BaseSelector
from dbt_job_maestro.selector_types import SelectorPriority, SelectorMetadata
from dbt_job_maestro.model_resolver import ModelResolver


class ManualSelector(BaseSelector):
    """Handles manually created selectors.

    Manual selectors are read from existing selectors.yml files and preserved
    exactly as-is. They are never regenerated or modified.
    """

    def get_priority(self) -> SelectorPriority:
        """Return the priority level for manual selectors.

        Returns:
            SelectorPriority.MANUAL (highest priority)
        """
        return SelectorPriority.MANUAL

    def generate(self, excluded_models: Set[str]) -> List[Dict[str, Any]]:
        """Read and preserve manual selectors from existing file.

        Manual selectors are NEVER regenerated or modified. This method simply
        reads the existing selectors.yml file and filters for manual selectors.

        Args:
            excluded_models: Not used for manual selectors

        Returns:
            List of manual selector definitions
        """
        manual_selectors = []

        # Try to find existing selectors file
        possible_paths = [
            Path("selectors.yml"),
            Path("dbt_project/selectors.yml"),
            Path("./selectors.yml"),
        ]

        for path in possible_paths:
            if path.exists():
                existing = self._read_selectors_file(str(path))

                for selector in existing:
                    if self.is_manually_created(selector):
                        manual_selectors.append(selector)

                break

        return manual_selectors

    def extract_metadata(self, selector_def: Dict[str, Any]) -> SelectorMetadata:
        """Extract metadata from a manual selector.

        Uses ModelResolver to resolve which models are covered by the selector
        definition, regardless of whether it uses fqn, tag, or path methods.

        Args:
            selector_def: Selector definition dictionary

        Returns:
            SelectorMetadata with resolved models and metadata
        """
        resolver = ModelResolver(self.parser, self.graph)
        resolution = resolver.resolve_selector(selector_def)

        return SelectorMetadata(
            name=selector_def.get("name", ""),
            priority=SelectorPriority.MANUAL,
            manually_created=True,
            models_covered=resolution.models,
            paths_used=resolution.paths,
            tags_used=resolution.tags,
            fqns_used=resolution.fqns,
            invalid_fqns=resolution.invalid_fqns,
        )

    def _read_selectors_file(self, file_path: str) -> List[Dict[str, Any]]:
        """Read selectors from YAML file.

        Args:
            file_path: Path to selectors.yml file

        Returns:
            List of selector definitions, or empty list if file doesn't exist
        """
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            with open(file_path, "r") as f:
                content = yaml.safe_load(f)
                if content is None:
                    return []
                return content.get("selectors", [])
        return []
