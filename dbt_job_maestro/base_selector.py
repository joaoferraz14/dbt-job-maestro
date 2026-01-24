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

        Any selector whose name does NOT start with the configured selector_prefix
        (default: "maestro_") is considered manual. This is the simple convention -
        all auto-generated selectors use the configured prefix.

        Args:
            selector_def: Selector definition dictionary

        Returns:
            True if manually created (name doesn't start with selector_prefix), False otherwise
        """
        name = selector_def.get("name", "")
        prefix = f"{self.config.selector_prefix}_"

        # If name doesn't start with the configured prefix, it's manual
        if not name.startswith(prefix):
            return True

        return False
