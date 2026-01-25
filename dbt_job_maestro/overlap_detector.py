"""Detect and report overlaps between selectors."""

import logging
from typing import Dict, List, Any
from dbt_job_maestro.selector_types import SelectorPriority, SelectorMetadata, OverlapWarning
from dbt_job_maestro.model_resolver import ModelResolver

logger = logging.getLogger(__name__)


class OverlapDetector:
    """Detects and reports overlaps between selectors.

    Analyzes selector definitions to identify models that appear in multiple
    selectors and reports them with appropriate severity levels.
    """

    def __init__(self, model_resolver: ModelResolver, selector_prefix: str = "maestro"):
        """Initialize the overlap detector.

        Args:
            model_resolver: ModelResolver instance for extracting models
            selector_prefix: Prefix used for auto-generated selectors (default: "maestro")
        """
        self.resolver = model_resolver
        self.selector_prefix = selector_prefix

    def detect_overlaps(
        self, selectors: List[Dict[str, Any]], selector_metadata: Dict[str, SelectorMetadata] = None
    ) -> List[OverlapWarning]:
        """Detect overlapping models between selectors.

        Args:
            selectors: List of all selector definitions
            selector_metadata: Optional pre-computed metadata for selectors

        Returns:
            List of overlap warnings
        """
        if selector_metadata is None:
            selector_metadata = {}

        warnings = []

        # Build a map of model -> list of selectors containing it
        model_to_selectors = {}

        for selector in selectors:
            selector_name = selector.get("name", "")

            # Get metadata or extract it
            if selector_name in selector_metadata:
                metadata = selector_metadata[selector_name]
            else:
                # Extract on the fly
                resolution = self.resolver.resolve_selector(selector)
                metadata = SelectorMetadata(
                    name=selector_name,
                    priority=self._infer_priority(selector),
                    manually_created=self._is_manual(selector),
                    models_covered=resolution.models,
                    paths_used=resolution.paths,
                    tags_used=resolution.tags,
                    fqns_used=resolution.fqns,
                )

            # Track which selectors contain each model
            for model in metadata.models_covered:
                if model not in model_to_selectors:
                    model_to_selectors[model] = []
                model_to_selectors[model].append((selector_name, metadata.priority))

        # Identify overlaps
        for model, selector_list in model_to_selectors.items():
            if len(selector_list) > 1:
                warning = self._create_overlap_warning(model, selector_list)
                warnings.append(warning)

        return warnings

    def report_overlaps(self, warnings: List[OverlapWarning]) -> None:
        """Report overlap warnings to the user.

        Args:
            warnings: List of overlap warnings to report
        """
        if not warnings:
            logger.info("✓ No overlaps detected between selectors")
            return

        # Group by severity
        errors = [w for w in warnings if w.severity == "ERROR"]
        warnings_list = [w for w in warnings if w.severity == "WARNING"]

        if errors:
            logger.error(f"Found {len(errors)} ERRORS in selector overlap:")
            for warning in errors:
                logger.error(f"  ❌ {warning.message}")

        if warnings_list:
            logger.warning(f"Found {len(warnings_list)} WARNINGS in selector overlap:")
            for warning in warnings_list:
                logger.warning(f"  ⚠️  {warning.message}")

        # Summary
        logger.info(f"\nOverlap Summary: {len(errors)} errors, " f"{len(warnings_list)} warnings")

    def _create_overlap_warning(self, model_name: str, selectors: List[tuple]) -> OverlapWarning:
        """Create an overlap warning with appropriate severity.

        Args:
            model_name: Name of the overlapping model
            selectors: List of (selector_name, priority) tuples

        Returns:
            OverlapWarning instance
        """
        # Check if any manual selectors are involved
        has_manual = any(priority == SelectorPriority.MANUAL for _, priority in selectors)

        # Determine severity
        if has_manual and len(selectors) > 1:
            # Manual selector overlap with others
            manual_count = sum(1 for _, p in selectors if p == SelectorPriority.MANUAL)

            if manual_count > 1:
                # Multiple manual selectors - WARNING
                severity = "WARNING"
                manual_selectors = [s for s, p in selectors if p == SelectorPriority.MANUAL]
                message = (
                    f"Model '{model_name}' appears in {manual_count} manual "
                    f"selectors: {', '.join(manual_selectors)}. "
                    f"This is allowed but should be intentional."
                )
            else:
                # Manual + auto overlap - should not happen with proper deduplication
                severity = "ERROR"
                auto_selectors = [s for s, p in selectors if p != SelectorPriority.MANUAL]
                message = (
                    f"Model '{model_name}' in manual selector but also found in "
                    f"auto-generated selectors: {', '.join(auto_selectors)}. "
                    f"This indicates a bug in the deduplication logic."
                )
        else:
            # Multiple auto selectors - ERROR (should never happen)
            severity = "ERROR"
            message = (
                f"Model '{model_name}' appears in multiple auto-generated "
                f"selectors: {', '.join(s for s, _ in selectors)}. "
                f"This indicates a bug in the priority system."
            )

        return OverlapWarning(
            model_name=model_name, selectors=selectors, severity=severity, message=message
        )

    def _infer_priority(self, selector_def: Dict[str, Any]) -> SelectorPriority:
        """Infer priority from selector definition.

        Args:
            selector_def: Selector definition dictionary

        Returns:
            SelectorPriority enum value
        """
        if self._is_manual(selector_def):
            return SelectorPriority.MANUAL
        else:
            # Selector starts with auto-generated prefix (e.g., "maestro_")
            return SelectorPriority.AUTO_FQN

    def _is_manual(self, selector_def: Dict[str, Any]) -> bool:
        """Check if selector is manually created.

        A selector is considered manual if it does NOT start with the
        auto-generated selector prefix (e.g., "maestro_").

        Args:
            selector_def: Selector definition dictionary

        Returns:
            True if manually created, False otherwise
        """
        name = selector_def.get("name", "")
        auto_prefix = f"{self.selector_prefix}_"

        # Selector is manual if it does NOT start with the auto-generated prefix
        return not name.startswith(auto_prefix)
