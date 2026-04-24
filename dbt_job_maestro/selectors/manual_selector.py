"""Manual selector handler."""

import os
import re
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

    def __init__(self, manifest_parser, graph_builder, config):
        """Initialize ManualSelector.

        Args:
            manifest_parser: ManifestParser instance
            graph_builder: GraphBuilder instance
            config: SelectorConfig instance
        """
        super().__init__(manifest_parser, graph_builder, config)
        # Raw YAML text for each manual selector (populated by generate())
        self.raw_manual_blocks: List[str] = []

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
        Also populates raw_manual_blocks with the original YAML text for each
        manual selector (used when reformat_manual_selectors is False).

        Args:
            excluded_models: Not used for manual selectors

        Returns:
            List of manual selector definitions
        """
        manual_selectors = []
        self.raw_manual_blocks = []

        # Try to find existing selectors file
        possible_paths = [
            Path("selectors.yml"),
            Path("dbt_project/selectors.yml"),
            Path("./selectors.yml"),
        ]

        for path in possible_paths:
            if path.exists():
                existing = self._read_selectors_file(str(path))
                raw_blocks = self._read_raw_selector_blocks(str(path))

                for selector in existing:
                    if self.is_manually_created(selector):
                        manual_selectors.append(selector)
                        # Find matching raw block by name
                        name = selector.get("name", "")
                        raw = raw_blocks.get(name)
                        if raw is not None:
                            self.raw_manual_blocks.append(raw)

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

    def _read_raw_selector_blocks(self, file_path: str) -> Dict[str, str]:
        """Read raw YAML text blocks for each selector from a file.

        Splits the file on selector boundaries (lines matching '  - name:')
        and returns a dict mapping selector name → raw YAML text block.

        Args:
            file_path: Path to selectors.yml file

        Returns:
            Dict mapping selector name to its raw YAML text (without leading
            'selectors:' header). Each block starts with '  - name: ...'
        """
        blocks: Dict[str, str] = {}
        if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
            return blocks

        with open(file_path, "r") as f:
            content = f.read()

        # Split into individual selector blocks.
        # Each selector starts with "  - name:" at the beginning of a line.
        # We use a regex that splits on the boundary just before "  - name:".
        parts = re.split(r"(?=^  - name: )", content, flags=re.MULTILINE)

        for part in parts:
            part = part.rstrip("\n")
            if not part.strip():
                continue
            # Skip the 'selectors:' header line
            if part.strip() == "selectors:":
                continue
            # Extract the name from the first line
            match = re.match(r"  - name: (.+)", part)
            if match:
                name = match.group(1).strip()
                # Remove surrounding quotes if present
                if (name.startswith("'") and name.endswith("'")) or (
                    name.startswith('"') and name.endswith('"')
                ):
                    name = name[1:-1]
                blocks[name] = part

        return blocks
