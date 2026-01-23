"""Core data structures for selector generation system."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Set, List, Tuple


class SelectorPriority(Enum):
    """Priority levels for selector generation.

    Lower numeric values indicate higher priority.
    Manual selectors always have the highest priority.
    """
    MANUAL = 1      # Highest priority - never modified
    AUTO_FQN = 2    # Auto-generated FQN-based selectors


@dataclass
class SelectorMetadata:
    """Metadata for a selector definition.

    Attributes:
        name: Selector name
        priority: Priority level
        manually_created: Whether this is a manually created selector
        models_covered: Set of model names covered by this selector
        paths_used: Set of paths referenced in the selector definition
        tags_used: Set of tags referenced in the selector definition
        fqns_used: Set of FQN values referenced in the selector definition
        invalid_fqns: Set of FQN references that don't exist in the manifest
    """
    name: str
    priority: SelectorPriority
    manually_created: bool
    models_covered: Set[str] = field(default_factory=set)
    paths_used: Set[str] = field(default_factory=set)
    tags_used: Set[str] = field(default_factory=set)
    fqns_used: Set[str] = field(default_factory=set)
    invalid_fqns: Set[str] = field(default_factory=set)


@dataclass
class ModelResolution:
    """Result of resolving models from a selector definition.

    Attributes:
        models: Set of resolved model names
        paths: Set of paths referenced
        tags: Set of tags referenced
        fqns: Set of FQN values referenced
        invalid_fqns: Set of FQN references that don't exist in the manifest
    """
    models: Set[str] = field(default_factory=set)
    paths: Set[str] = field(default_factory=set)
    tags: Set[str] = field(default_factory=set)
    fqns: Set[str] = field(default_factory=set)
    invalid_fqns: Set[str] = field(default_factory=set)


@dataclass
class OverlapWarning:
    """Warning about overlapping models between selectors.

    Attributes:
        model_name: Name of the model that appears in multiple selectors
        selectors: List of (selector_name, priority) tuples
        severity: Severity level ("ERROR", "WARNING", "INFO")
        message: Human-readable warning message
    """
    model_name: str
    selectors: List[Tuple[str, SelectorPriority]]
    severity: str  # "ERROR", "WARNING", "INFO"
    message: str
