"""Abstract base class for selector generators."""

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Set
from dbt_job_maestro.selector_types import SelectorPriority, SelectorMetadata


class BaseSelector(ABC):
    """Abstract base class for all selector types.

    This class defines the interface that all selector generators must implement.
    It provides common functionality for manual selector detection while requiring
    subclasses to implement their specific generation logic.
    """

    def __init__(self, manifest_parser, graph_builder, config):
        """Initialize the selector generator.

        Args:
            manifest_parser: ManifestParser instance
            graph_builder: GraphBuilder instance
            config: SelectorConfig instance
        """
        self.parser = manifest_parser
        self.graph = graph_builder
        self.config = config
        self.models = manifest_parser.get_models()

    @abstractmethod
    def get_priority(self) -> SelectorPriority:
        """Return the priority level of this selector type.

        Returns:
            SelectorPriority enum value
        """
        pass

    @abstractmethod
    def generate(self, excluded_models: Set[str]) -> List[Dict[str, Any]]:
        """Generate selectors of this type.

        Args:
            excluded_models: Models already assigned to higher-priority selectors

        Returns:
            List of selector definitions
        """
        pass

    @abstractmethod
    def extract_metadata(self, selector_def: Dict[str, Any]) -> SelectorMetadata:
        """Extract metadata from a selector definition.

        Args:
            selector_def: Selector definition dictionary

        Returns:
            SelectorMetadata with information about the selector
        """
        pass

    def is_manually_created(self, selector_def: Dict[str, Any]) -> bool:
        """Determine if a selector is manually created.

        Checks in priority order:
        1. Name starts with "manually_created_"
        2. Has metadata field "manually_created: true"
        3. Fallback: description contains "manually_created"

        Args:
            selector_def: Selector definition dictionary

        Returns:
            True if manually created, False otherwise
        """
        name = selector_def.get("name", "")

        # Primary: Check name prefix
        if name.startswith("manually_created_"):
            return True

        # Secondary: Check metadata field
        metadata = selector_def.get("metadata", {})
        if metadata.get("manually_created", False):
            return True

        # Fallback: Check description (deprecated, for backward compatibility)
        description = selector_def.get("description", "")
        if "manually_created" in description.lower():
            return True

        return False
