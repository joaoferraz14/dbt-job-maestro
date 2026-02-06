"""Orchestrate selector generation across all types."""

import os
import yaml
import logging
from typing import List, Dict, Any, Set, Tuple, Optional

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

        # Generate seeds selectors if configured
        if self.config.include_seeds_selectors:
            seeds_selectors = self._generate_seeds_selectors()
            selectors.extend(seeds_selectors)

        # Generate snapshots selectors if configured
        if self.config.include_snapshots_selectors:
            snapshots_selectors = self._generate_snapshots_selectors()
            selectors.extend(snapshots_selectors)

        # Generate full refresh selector if configured
        if self.config.include_full_refresh_selector:
            full_refresh_selector = self._generate_full_refresh_selector()
            selectors.append(full_refresh_selector)

        # Check for overlaps if configured
        if self.config.warn_on_manual_overlaps:
            warnings = self.overlap_detector.detect_overlaps(selectors)
            self.overlap_detector.report_overlaps(warnings)

        # Warn about models not covered by any selector
        self._warn_uncovered_models(selectors)

        return selectors

    def _warn_uncovered_models(self, selectors: List[Dict[str, Any]]) -> None:
        """Warn if any models won't be run by any selector.

        Checks all models in the manifest against what's covered by all selectors
        (both manual and auto-generated) and warns about any gaps, including
        models that were excluded by config but not covered by manual selectors.

        Args:
            selectors: List of all selector definitions
        """
        # Get all models in manifest
        all_models = set(self.models.keys())

        # Get models excluded by config
        config_excluded = self._get_config_excluded_models()

        # Get models covered by all selectors (manual + auto-generated)
        covered_models: Set[str] = set()
        for selector in selectors:
            resolution = self.resolver.resolve_selector(selector)
            covered_models.update(resolution.models)

        # Find ALL models that aren't covered by any selector
        all_uncovered = all_models - covered_models

        if all_uncovered:
            # Separate into config-excluded vs other uncovered
            excluded_and_uncovered = all_uncovered & config_excluded
            other_uncovered = all_uncovered - config_excluded

            # Warn about config-excluded models not in any manual selector
            if excluded_and_uncovered:
                logger.warning(
                    f"\n⚠️  WARNING: {len(excluded_and_uncovered)} model(s) are excluded from "
                    f"auto-generation and NOT in any manual selector (they will NOT run):"
                )
                for model in sorted(excluded_and_uncovered)[:10]:
                    logger.warning(f"    - {model}")
                if len(excluded_and_uncovered) > 10:
                    logger.warning(f"    ... and {len(excluded_and_uncovered) - 10} more")
                logger.warning(
                    "\n💡 TIP: If these models should run, add them to a manual selector "
                    "or remove them from exclude_tags/exclude_paths/exclude_models."
                )

            # Warn about other uncovered models (not excluded by config)
            if other_uncovered:
                logger.warning(
                    f"\n⚠️  WARNING: {len(other_uncovered)} model(s) "
                    "will NOT be run by any selector:"
                )
                for model in sorted(other_uncovered)[:10]:
                    logger.warning(f"    - {model}")
                if len(other_uncovered) > 10:
                    logger.warning(f"    ... and {len(other_uncovered) - 10} more")
                logger.warning(
                    "\n💡 RECOMMENDATION: Check if these models should be added to a "
                    "manual selector or if they need proper tags/paths for auto-generation."
                )

    def _get_config_excluded_models(self) -> Set[str]:
        """Get models to exclude based on config's exclude_tags, exclude_paths, and exclude_models.

        Returns:
            Set of model names to exclude from selector generation
        """
        excluded = set()

        # Exclude models matching exclude_tags
        if self.config.exclude_tags:
            tag_excluded = self.graph.get_models_with_tags(self.config.exclude_tags)
            if tag_excluded:
                logger.info(
                    f"Excluding {len(tag_excluded)} models based on exclude_tags: "
                    f"{', '.join(self.config.exclude_tags)}"
                )
            excluded.update(tag_excluded)

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

                # Warn about exclusions in manual selector not in config
                self._warn_manual_selector_exclusions(selector)

                # Exclude models from auto-generation
                if metadata.models_covered:
                    logger.info(
                        f"  Manual selector '{selector['name']}' covers "
                        f"{len(metadata.models_covered)} models"
                    )
                    excluded_models.update(metadata.models_covered)

        return manual_selectors, excluded_models

    def _warn_manual_selector_exclusions(self, selector: Dict[str, Any]) -> None:
        """Warn if manual selector has exclusions not in config.

        If a manual selector excludes tags/paths that aren't in the config's
        exclude_tags/exclude_paths, models with those tags/paths will still
        appear in auto-generated selectors.

        Args:
            selector: Manual selector definition
        """
        selector_name = selector.get("name", "unknown")
        definition = selector.get("definition", {})

        # Extract exclusions from the selector definition
        manual_excluded_tags, manual_excluded_paths = self._extract_exclusions_from_definition(
            definition
        )

        # Check for tags excluded in manual but not in config
        config_excluded_tags = set(self.config.exclude_tags)
        tags_not_in_config = manual_excluded_tags - config_excluded_tags
        if tags_not_in_config:
            logger.warning(
                f"  Manual selector '{selector_name}' excludes tag(s) {sorted(tags_not_in_config)} "
                f"but these are NOT in config exclude_tags. "
                f"Models with these tags will still appear in auto-generated selectors."
            )

        # Check for paths excluded in manual but not in config
        config_excluded_paths = set(self.config.exclude_paths)
        paths_not_in_config = manual_excluded_paths - config_excluded_paths
        if paths_not_in_config:
            logger.warning(
                f"  Manual selector '{selector_name}' excludes path(s) "
                f"{sorted(paths_not_in_config)} but these are NOT in config "
                "exclude_paths. Models in these paths will still appear "
                "in auto-generated selectors."
            )

    def _extract_exclusions_from_definition(
        self, definition: Dict[str, Any]
    ) -> Tuple[Set[str], Set[str]]:
        """Extract excluded tags and paths from a selector definition.

        Recursively searches the definition for exclude clauses.

        Args:
            definition: Selector definition dictionary

        Returns:
            Tuple of (excluded_tags set, excluded_paths set)
        """
        excluded_tags: Set[str] = set()
        excluded_paths: Set[str] = set()

        def extract_from_item(item: Any) -> None:
            if not isinstance(item, dict):
                return

            # Check for exclude clause
            if "exclude" in item:
                exclude_def = item["exclude"]
                if isinstance(exclude_def, dict):
                    # Handle union of exclusions
                    for exc_item in exclude_def.get("union", []):
                        if isinstance(exc_item, dict):
                            method = exc_item.get("method")
                            value = exc_item.get("value")
                            if method == "tag" and value:
                                excluded_tags.add(value)
                            elif method == "path" and value:
                                excluded_paths.add(value)

            # Recursively check union items
            if "union" in item:
                for union_item in item["union"]:
                    extract_from_item(union_item)

            # Recursively check intersection items
            if "intersection" in item:
                for inter_item in item["intersection"]:
                    extract_from_item(inter_item)

        extract_from_item(definition)
        return excluded_tags, excluded_paths

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

        Models are only added to their shortest matching path to avoid duplicates.
        For example, a model in 'models/staging/sap/snpglue' will only appear in
        the 'staging_sap' selector (at level 1), not in 'staging_sap_snpglue'.

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

        # Sort paths by length (shortest first) so higher-level paths take priority
        sorted_paths = sorted(path_prefixes, key=lambda p: len(p.split("/")))

        # Track models already assigned to a path selector
        models_already_covered: Set[str] = set()

        for path_prefix in sorted_paths:
            models = self.graph.group_by_path(path_prefix)
            # Filter out excluded models AND models already covered by another path selector
            models = [
                m for m in models if m not in excluded_models and m not in models_already_covered
            ]

            if models:
                selector_name = self._path_to_selector_name(path_prefix)
                selector = {
                    "name": f"{self.config.selector_prefix}_path_{selector_name}",
                    "description": f"Selector for models in {path_prefix}",
                    "definition": {"union": [{"method": "path", "value": path_prefix}]},
                }

                # Add combined exclusion clause if any exclusions are configured
                exclusion = self._create_exclusion()
                if exclusion:
                    selector["definition"]["union"].append(exclusion)

                all_selectors.append(selector)

                # Mark these models as covered so they won't appear in other path selectors
                models_already_covered.update(models)

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

            if models:
                selector = {
                    "name": f"{self.config.selector_prefix}_tag_{tag}",
                    "description": f"Selector for models tagged with {tag}",
                    "definition": {"union": [{"method": "tag", "value": tag}]},
                }

                # Add combined exclusion clause if any exclusions are configured
                exclusion = self._create_exclusion()
                if exclusion:
                    selector["definition"]["union"].append(exclusion)

                all_selectors.append(selector)

                # Create freshness selector if configured
                if self._should_create_freshness(selector["name"]):
                    freshness = self._create_freshness_selector(selector["name"])
                    all_selectors.append(freshness)

        # Warn about untagged models
        untagged_models = all_model_names - tagged_models
        if untagged_models:
            logger.warning(
                f"\n⚠️  WARNING: {len(untagged_models)} model(s) have no tags and "
                "will NOT be included in any selector when using method='tag':"
            )
            for model in sorted(untagged_models)[:10]:
                logger.warning(f"    - {model}")
            if len(untagged_models) > 10:
                logger.warning(f"    ... and {len(untagged_models) - 10} more")
            logger.warning(
                "\n💡 RECOMMENDATION: Use method='fqn' to ensure all models are "
                "included in selectors. The 'fqn' method groups models by "
                "dependencies for complete coverage."
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

    def _create_exclusion(self) -> Optional[Dict[str, Any]]:
        """Create combined exclusion definition for tags and paths.

        Combines all exclusions (tags and paths) into a single exclude block.
        Uses 'union' mode (exclude if ANY criteria matches) or 'intersection'
        mode (exclude only if ALL criteria match) based on config.exclusion_mode.

        Returns:
            Exclusion definition dictionary, or None if no exclusions configured
        """
        exclusion_items = []

        # Add tag exclusions
        if self.config.exclude_tags:
            for tag in self.config.exclude_tags:
                exclusion_items.append({"method": "tag", "value": tag})

        # Add path exclusions
        if self.config.exclude_paths:
            for path in self.config.exclude_paths:
                exclusion_items.append({"method": "path", "value": path})

        if not exclusion_items:
            return None

        # Use configured exclusion mode (union or intersection)
        mode = self.config.exclusion_mode
        return {"exclude": {mode: exclusion_items}}

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

        Logic:
        1. If selector is in exclude_freshness_selector_names, return False
        2. If freshness_selector_names is provided, only create for those selectors
        3. Otherwise, use the global include_freshness_selectors flag

        Args:
            selector_name: Name of the selector to check

        Returns:
            True if freshness selector should be created, False otherwise
        """
        # Check exclusion list first
        if selector_name in self.config.exclude_freshness_selector_names:
            return False

        # If include list is provided, only create for those selectors
        if self.config.freshness_selector_names:
            return selector_name in self.config.freshness_selector_names

        # Otherwise use global flag
        return self.config.include_freshness_selectors

    def _generate_seeds_selectors(self) -> List[Dict[str, Any]]:
        """Generate selectors for seed files.

        Creates selectors for seeds based on the configured method (path or fqn).
        Seeds already covered by manual selectors are excluded.

        Returns:
            List of seed selector definitions
        """
        selectors = []
        seeds = self.parser.get_seeds()

        if not seeds:
            logger.info("No seeds found in manifest")
            return selectors

        # Get seeds covered by manual selectors
        excluded_seeds = self._get_seeds_in_manual_selectors()

        if self.config.seeds_selector_method == "path":
            # Create a single selector for all seeds by path
            seeds_path = self.config.seeds_path
            if not seeds_path:
                # Auto-detect from manifest
                path_prefixes = self.parser.get_seeds_path_prefixes(level=0)
                if path_prefixes:
                    seeds_path = sorted(path_prefixes)[0]

            if seeds_path:
                # Filter out excluded seeds
                included_seeds = [s for s in seeds.keys() if s not in excluded_seeds]
                if included_seeds:
                    selector = {
                        "name": f"{self.config.selector_prefix}_seeds",
                        "description": f"Selector for all seeds in {seeds_path}",
                        "definition": {"union": [{"method": "path", "value": seeds_path}]},
                    }
                    selectors.append(selector)
                    logger.info(
                        f"Generated seeds selector covering {len(included_seeds)} seeds "
                        f"({len(excluded_seeds)} excluded by manual selectors)"
                    )
        else:
            # Create ONE combined selector for all seeds (fqn method)
            included_seeds = [s for s in sorted(seeds.keys()) if s not in excluded_seeds]
            if included_seeds:
                union_items = [
                    {"method": "fqn", "value": seed_name} for seed_name in included_seeds
                ]
                selector = {
                    "name": f"{self.config.selector_prefix}_seeds",
                    "description": f"Selector for all seeds ({len(included_seeds)} seeds)",
                    "definition": {"union": union_items},
                }
                selectors.append(selector)
                logger.info(
                    f"Generated seeds selector covering {len(included_seeds)} seeds "
                    f"({len(excluded_seeds)} excluded by manual selectors)"
                )

        return selectors

    def _generate_snapshots_selectors(self) -> List[Dict[str, Any]]:
        """Generate selectors for snapshot files.

        Creates selectors for snapshots based on the configured method (path or fqn).
        Snapshots already covered by manual selectors are excluded.

        Returns:
            List of snapshot selector definitions
        """
        selectors = []
        snapshots = self.parser.get_snapshots()

        if not snapshots:
            logger.info("No snapshots found in manifest")
            return selectors

        # Get snapshots covered by manual selectors
        excluded_snapshots = self._get_snapshots_in_manual_selectors()

        if self.config.snapshots_selector_method == "path":
            # Create a single selector for all snapshots by path
            snapshots_path = self.config.snapshots_path
            if not snapshots_path:
                # Auto-detect from manifest
                path_prefixes = self.parser.get_snapshots_path_prefixes(level=0)
                if path_prefixes:
                    snapshots_path = sorted(path_prefixes)[0]

            if snapshots_path:
                # Filter out excluded snapshots
                included_snapshots = [s for s in snapshots.keys() if s not in excluded_snapshots]
                if included_snapshots:
                    selector = {
                        "name": f"{self.config.selector_prefix}_snapshots",
                        "description": f"Selector for all snapshots in {snapshots_path}",
                        "definition": {"union": [{"method": "path", "value": snapshots_path}]},
                    }
                    selectors.append(selector)
                    logger.info(
                        f"Generated snapshots selector covering {len(included_snapshots)} snapshots "
                        f"({len(excluded_snapshots)} excluded by manual selectors)"
                    )
        else:
            # Create ONE combined selector for all snapshots (fqn method)
            included_snapshots = [
                s for s in sorted(snapshots.keys()) if s not in excluded_snapshots
            ]
            if included_snapshots:
                union_items = [
                    {"method": "fqn", "value": snapshot_name}
                    for snapshot_name in included_snapshots
                ]
                selector = {
                    "name": f"{self.config.selector_prefix}_snapshots",
                    "description": f"Selector for all snapshots ({len(included_snapshots)} snapshots)",
                    "definition": {"union": union_items},
                }
                selectors.append(selector)
                logger.info(
                    f"Generated snapshots selector covering {len(included_snapshots)} snapshots "
                    f"({len(excluded_snapshots)} excluded by manual selectors)"
                )

        return selectors

    def _get_seeds_in_manual_selectors(self) -> Set[str]:
        """Get seeds that are covered by manual selectors.

        Analyzes manual selectors to find any that reference seeds,
        either by FQN or path.

        Returns:
            Set of seed names covered by manual selectors
        """
        covered_seeds = set()
        seeds = self.parser.get_seeds()
        seed_names = set(seeds.keys())

        # Get all manual selectors
        manual_gen = self.generators[SelectorPriority.MANUAL]
        manual_selectors = manual_gen.generate(excluded_models=set())

        for selector in manual_selectors:
            definition = selector.get("definition", {})
            covered = self._extract_covered_resources_from_definition(definition, seed_names)
            covered_seeds.update(covered)

        return covered_seeds

    def _get_snapshots_in_manual_selectors(self) -> Set[str]:
        """Get snapshots that are covered by manual selectors.

        Analyzes manual selectors to find any that reference snapshots,
        either by FQN or path.

        Returns:
            Set of snapshot names covered by manual selectors
        """
        covered_snapshots = set()
        snapshots = self.parser.get_snapshots()
        snapshot_names = set(snapshots.keys())

        # Get all manual selectors
        manual_gen = self.generators[SelectorPriority.MANUAL]
        manual_selectors = manual_gen.generate(excluded_models=set())

        for selector in manual_selectors:
            definition = selector.get("definition", {})
            covered = self._extract_covered_resources_from_definition(definition, snapshot_names)
            covered_snapshots.update(covered)

        return covered_snapshots

    def _extract_covered_resources_from_definition(
        self, definition: Dict[str, Any], resource_names: Set[str]
    ) -> Set[str]:
        """Extract resources covered by a selector definition.

        Recursively searches the definition for FQN or path references
        that match the provided resource names.

        Args:
            definition: Selector definition dictionary
            resource_names: Set of resource names to check against

        Returns:
            Set of covered resource names
        """
        covered = set()

        def extract_from_item(item: Any) -> None:
            if not isinstance(item, dict):
                return

            method = item.get("method")
            value = item.get("value")

            if method == "fqn" and value in resource_names:
                covered.add(value)
            elif method == "path" and value:
                # Check if any resources are in this path
                for resource_name in resource_names:
                    # Simple path matching - could be enhanced
                    if value in resource_name or resource_name.startswith(value.rstrip("/")):
                        covered.add(resource_name)

            # Recursively check union items
            if "union" in item:
                for union_item in item["union"]:
                    extract_from_item(union_item)

            # Recursively check intersection items
            if "intersection" in item:
                for inter_item in item["intersection"]:
                    extract_from_item(inter_item)

        extract_from_item(definition)
        return covered

    def _generate_full_refresh_selector(self) -> Dict[str, Any]:
        """Generate a selector for full refresh of incremental models.

        Creates a selector using intersection of fqn:* and config.materialized:incremental
        with optional exclusions and indirect_selection setting.

        Returns:
            Full refresh selector definition
        """
        # Build the intersection clause
        intersection_clause = [
            {"method": "fqn", "value": "*"},
            {"method": "config.materialized", "value": "incremental"},
        ]

        # Build the selector definition
        definition: Dict[str, Any] = {"union": [{"intersection": intersection_clause}]}

        # Add exclusions if configured
        exclusion_items = []

        for tag in self.config.full_refresh_exclude_tags:
            exclusion_items.append({"method": "tag", "value": tag})

        for path in self.config.full_refresh_exclude_paths:
            exclusion_items.append({"method": "path", "value": path})

        for model in self.config.full_refresh_exclude_models:
            exclusion_items.append({"method": "fqn", "value": model})

        if exclusion_items:
            definition["union"].append({"exclude": {"union": exclusion_items}})

        selector = {
            "name": f"{self.config.selector_prefix}_full_refresh_incremental",
            "description": "Selector for full refresh of all incremental models",
            "definition": definition,
        }

        # Add indirect_selection if not the default
        if self.config.indirect_selection != "eager":
            selector["default"] = {"indirect_selection": self.config.indirect_selection}

        logger.info(
            f"Generated full refresh selector for incremental models "
            f"(indirect_selection: {self.config.indirect_selection})"
        )

        return selector

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
