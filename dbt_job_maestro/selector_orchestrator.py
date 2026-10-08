"""Orchestrate selector generation across all types."""

import os
import io
import yaml
import logging
from typing import List, Dict, Any, Set, Tuple

from dbt_job_maestro.selector_types import ModelCoverage, SelectorPriority
from dbt_job_maestro.model_resolver import ModelResolver
from dbt_job_maestro.overlap_detector import OverlapDetector
from dbt_job_maestro.selectors import ManualSelector, FQNSelector

logger = logging.getLogger(__name__)


class SelectorOrchestrator:
    """Orchestrates selector generation using FQN-based strategy.

    Groups models by their dependency graph. Manual selectors (those not starting
    with the configured prefix) are always preserved and their models excluded
    from auto-generation to prevent duplicates.
    """

    def __init__(self, manifest_parser, graph_builder, config, existing_selectors_path=None):
        """Initialize the selector orchestrator.

        Args:
            manifest_parser: ManifestParser instance
            graph_builder: GraphBuilder instance
            config: SelectorConfig instance
            existing_selectors_path: Path to the selectors file whose manual
                selectors should be preserved. Pass the configured output path
                (output_dir + selectors_output_file) so a custom filename still
                preserves hand-written selectors. Falls back to cwd lookup if None.
        """
        self.parser = manifest_parser
        self.graph = graph_builder
        self.config = config
        self.existing_selectors_path = existing_selectors_path
        self.models = manifest_parser.get_models()

        # Initialize model resolver
        self.resolver = ModelResolver(manifest_parser, graph_builder)

        # Initialize overlap detector with selector prefix from config
        self.overlap_detector = OverlapDetector(
            self.resolver, selector_prefix=config.selector_prefix
        )

        # Initialize selector generators
        self.generators = {
            SelectorPriority.MANUAL: ManualSelector(
                manifest_parser,
                graph_builder,
                config,
                existing_selectors_path=existing_selectors_path,
            ),
            SelectorPriority.AUTO_FQN: FQNSelector(manifest_parser, graph_builder, config),
        }

        # Raw YAML text blocks for manual selectors (populated during generation)
        self._raw_manual_blocks: List[str] = []
        self.model_coverage = ModelCoverage()

    def generate_selectors(self) -> List[Dict[str, Any]]:
        """Generate FQN-based selectors.

        Manual selectors are preserved and their models are excluded from
        automated selectors after complete automated model groups are built.

        Returns:
            List of selector definitions
        """
        model_selectors = self._generate_fqn_mode()
        manual_selectors, manual_excluded = self._get_manual_selectors_and_excluded_models()
        manual_gen = self.generators[SelectorPriority.MANUAL]
        self._raw_manual_blocks = list(manual_gen.raw_manual_blocks)
        self._apply_manual_model_exclusions(model_selectors, manual_excluded)
        selectors = manual_selectors + self._remove_empty_model_selectors(model_selectors)

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

        # Guarantee unique selector names. dbt treats two selectors with the same
        # name as an error, and downstream job/DAG generation keys by name. Names
        # can collide when a model is literally named like a reserved selector
        # (e.g. the dbt_artifacts package ships models named 'seeds'/'snapshots',
        # so maestro_seeds from that model collides with the dedicated seeds
        # selector). Disambiguate deterministically and warn.
        self._ensure_unique_names(selectors)

        # Check for overlaps if configured
        if self.config.warn_on_manual_overlaps:
            warnings = self.overlap_detector.detect_overlaps(selectors)
            self.overlap_detector.report_overlaps(warnings)

        # Apply indirect_selection to all auto-generated selectors when not default
        if self.config.indirect_selection != "eager":
            manual_gen = self.generators[SelectorPriority.MANUAL]
            for selector in selectors:
                if not manual_gen.is_manually_created(selector):
                    self._apply_indirect_selection(
                        selector.get("definition", {}), self.config.indirect_selection
                    )

        # Warn about models not covered by any selector
        self._warn_uncovered_models(selectors)

        return selectors

    def _apply_indirect_selection(self, definition: Any, mode: str) -> None:
        """Set ``indirect_selection`` on every selection method in a definition.

        dbt accepts ``indirect_selection`` as a key on an individual selector
        *method* inside ``definition``. It is NOT a root-level selector key: a
        selector whose root carries ``default: {indirect_selection: ...}`` makes
        dbt reject the whole selectors.yml with "Could not parse selector file
        data ... Valid root-level selector definitions: union, intersection,
        string, dictionary", which breaks every selector in the file, not just
        that one.

        Only positive selection clauses (``union`` / ``intersection``) are
        touched; ``exclude`` blocks are left alone since indirect selection has
        no meaning for exclusions.

        Args:
            definition: Selector definition subtree (mutated in place).
            mode: One of eager, cautious, buildable, empty.
        """
        if isinstance(definition, dict):
            if "method" in definition:
                definition["indirect_selection"] = mode
            for key in ("union", "intersection"):
                for item in definition.get(key, []) or []:
                    self._apply_indirect_selection(item, mode)
        elif isinstance(definition, list):
            for item in definition:
                self._apply_indirect_selection(item, mode)

    def _ensure_unique_names(self, selectors: List[Dict[str, Any]]) -> None:
        """Rename any selectors that share a name so every name is unique.

        Mutates ``selectors`` in place. The first occurrence of a name keeps it;
        later occurrences get a numeric suffix (``_2``, ``_3``, ...). A warning is
        logged for each rename so the collision is visible.

        Args:
            selectors: List of selector definitions (modified in place).
        """
        seen: Set[str] = set()
        for selector in selectors:
            name = selector.get("name", "")
            if name not in seen:
                seen.add(name)
                continue
            suffix = 2
            new_name = f"{name}_{suffix}"
            while new_name in seen:
                suffix += 1
                new_name = f"{name}_{suffix}"
            logger.warning(
                f"Duplicate selector name '{name}' detected; renaming one occurrence "
                f"to '{new_name}'. This usually means a model is named like a reserved "
                f"selector (e.g. a model literally called 'seeds' or 'snapshots')."
            )
            selector["name"] = new_name
            seen.add(new_name)

    def _warn_uncovered_models(self, selectors: List[Dict[str, Any]]) -> None:
        """Warn if any models won't be run by any selector.

        Checks all models in the manifest against what's covered by all selectors
        (both manual and auto-generated) and warns about any gaps, including
        models that were excluded by config but not covered by manual selectors.

        Args:
            selectors: List of all selector definitions
        """
        self.model_coverage = self.get_model_coverage(selectors)
        config_excluded = self.model_coverage.fully_excluded
        all_uncovered = self.model_coverage.fully_excluded | self.model_coverage.unexplained

        if all_uncovered:
            # Separate into explicitly excluded vs unexplained uncovered models
            excluded_and_uncovered = all_uncovered & config_excluded
            other_uncovered = all_uncovered - config_excluded

            # Warn about excluded models not in any selector
            if excluded_and_uncovered:
                logger.warning(
                    f"\n⚠️  WARNING: {len(excluded_and_uncovered)} model(s) are excluded from "
                    f"all selectors (they will NOT run):"
                )
                for model in sorted(excluded_and_uncovered)[:10]:
                    logger.warning(f"    - {model}")
                if len(excluded_and_uncovered) > 10:
                    logger.warning(f"    ... and {len(excluded_and_uncovered) - 10} more")
                logger.warning(
                    "\n💡 TIP: If these models should run, adjust selector exclusions "
                    "or remove them from exclude_tags/exclude_paths/exclude_models."
                )

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

    def get_model_coverage(self, selectors: List[Dict[str, Any]]) -> ModelCoverage:
        """Account for selected, explicitly removed, and unexplained manifest models."""
        all_models = set(self.models)
        selected: Set[str] = set()
        excluded = self._get_config_excluded_models()
        unsupported_methods: Set[str] = set()

        def without_exclusions(definition: Any) -> Any:
            if isinstance(definition, list):
                return [without_exclusions(item) for item in definition]
            if not isinstance(definition, dict):
                if isinstance(definition, str):
                    unsupported_methods.add("string selector expressions")
                return definition
            method = definition.get("method")
            if method and method not in {"fqn", "tag", "path"}:
                unsupported_methods.add(str(method))
            value = definition.get("value", "")
            if (
                method == "fqn"
                and isinstance(value, str)
                and value not in self.models
                and any(character in value for character in ".*?")
            ):
                unsupported_methods.add("qualified or wildcard fqn")
            if "exclude" in definition:
                without_exclusions(definition["exclude"])
            return {
                key: without_exclusions(value) if key in {"union", "intersection"} else value
                for key, value in definition.items()
                if key != "exclude"
            }

        for selector in selectors:
            # Freshness selects sources, not executable model coverage.
            if selector.get("name", "").startswith("freshness_"):
                continue
            resolution = self.resolver.resolve_selector(selector)
            selected.update(resolution.models & all_models)
            definition = selector.get("definition", {})
            positive_definition = without_exclusions(definition)
            if positive_definition != definition:
                positive = self.resolver.resolve_selector({"definition": positive_definition})
                excluded.update(positive.models - resolution.models)

        fully_excluded = (excluded & all_models) - selected
        return ModelCoverage(
            total=len(all_models),
            selected=selected,
            fully_excluded=fully_excluded,
            unexplained=all_models - selected - fully_excluded,
            unsupported_methods=unsupported_methods,
        )

    def _get_config_excluded_models(self) -> Set[str]:
        """Get models to exclude based on config's exclude_tags, exclude_paths, and exclude_models.

        Returns:
            Set of model names to exclude from selector generation
        """
        excluded = set()
        criteria: List[Set[str]] = []

        # Exclude models matching exclude_tags
        if self.config.exclude_tags:
            tag_excluded = self.graph.get_models_with_tags(self.config.exclude_tags)
            if tag_excluded:
                logger.info(
                    f"Excluding {len(tag_excluded)} models based on exclude_tags: "
                    f"{', '.join(self.config.exclude_tags)}"
                )
            excluded.update(tag_excluded)
            criteria.extend(set(self.graph.group_by_tag(tag)) for tag in self.config.exclude_tags)

        # Exclude models matching exclude_paths
        if self.config.exclude_paths:
            path_excluded = self.graph.get_models_in_paths(self.config.exclude_paths)
            if path_excluded:
                logger.info(
                    f"Excluding {len(path_excluded)} models based on exclude_paths: "
                    f"{', '.join(self.config.exclude_paths)}"
                )
            excluded.update(path_excluded)
            criteria.extend(
                set(self.graph.group_by_path(path)) for path in self.config.exclude_paths
            )

        if self.config.exclusion_mode == "intersection" and criteria:
            excluded = set.intersection(*criteria)

        # Exclude models by name
        if self.config.exclude_models:
            model_excluded = self.graph.get_models_by_names(self.config.exclude_models)
            if model_excluded:
                logger.info(
                    f"Excluding {len(model_excluded)} models based on exclude_models: "
                    f"{', '.join(model_excluded)}"
                )
            excluded.update(model_excluded)
            unmatched = set(self.config.exclude_models) - model_excluded
            if unmatched:
                logger.warning(
                    "exclude_models entries did not match manifest model names: "
                    + ", ".join(sorted(unmatched))
                    + ". Use bare model names, not file paths or qualified node IDs."
                )

        return excluded

    def _get_manual_selectors_and_excluded_models(self) -> tuple:
        """Get manual selectors and the models they cover.

        Manual selectors are preserved. Their covered models are returned so
        automated groups can be formed first, then emitted without duplicate
        positive FQN entries.

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

            # Check for exclude clause. dbt requires a list here; older maestro
            # output wrote a {union: [...]} dict, so accept both shapes.
            if "exclude" in item:
                exclude_def = item["exclude"]
                if isinstance(exclude_def, dict):
                    exc_items = []
                    for key in ("union", "intersection"):
                        exc_items.extend(exclude_def.get(key, []) or [])
                elif isinstance(exclude_def, list):
                    exc_items = exclude_def
                else:
                    exc_items = []

                for exc_item in exc_items:
                    if not isinstance(exc_item, dict):
                        continue
                    # Unwrap a nested union/intersection (intersection mode).
                    nested = []
                    for key in ("union", "intersection"):
                        nested.extend(exc_item.get(key, []) or [])
                    for candidate in nested or [exc_item]:
                        if not isinstance(candidate, dict):
                            continue
                        method = candidate.get("method")
                        value = candidate.get("value")
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
        """Build complete dependency selectors, then remove configuration matches.

        Manual-selector exclusions are applied after all automated selectors
        have been generated, so they cannot alter connected-component grouping.

        Returns:
            List of automated FQN-based selector definitions
        """
        excluded_models = self._get_config_excluded_models()
        fqn_gen = self.generators[SelectorPriority.AUTO_FQN]
        fqn_selectors = fqn_gen.generate(excluded_models=set())
        self._apply_manual_model_exclusions(fqn_selectors, excluded_models)
        fqn_selectors = self._remove_empty_model_selectors(fqn_selectors)

        logger.info(f"Generated {len(fqn_selectors)} FQN-based selectors")
        if excluded_models:
            logger.info(f"  ({len(excluded_models)} models excluded from auto-generation)")

        return fqn_selectors

    def _apply_manual_model_exclusions(
        self, selectors: List[Dict[str, Any]], manual_models: Set[str]
    ) -> None:
        """Exclude manual-owned models without changing generated group membership."""
        if not manual_models:
            return

        model_names = set(self.models)
        excluded_names = sorted(manual_models & model_names)
        for selector in selectors:
            name = selector.get("name", "")
            if name.startswith("freshness_"):
                continue
            self._remove_fqn_inclusions(selector, excluded_names)

    @staticmethod
    def _remove_fqn_inclusions(selector: Dict[str, Any], names: List[str]) -> None:
        """Remove manually owned models from an automated selector's positive FQNs."""
        definition = selector.get("definition", {})
        if not isinstance(definition, dict):
            return
        union = definition.get("union", [])
        if not isinstance(union, list):
            return

        excluded_names = set(names)
        definition["union"] = [
            item
            for item in union
            if not (
                isinstance(item, dict)
                and item.get("method") == "fqn"
                and item.get("value") in excluded_names
            )
        ]

    @staticmethod
    def _remove_empty_model_selectors(
        selectors: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Drop emptied model selectors and freshness selectors that reference them."""
        model_selector_names = {
            selector.get("name", "")
            for selector in selectors
            if not selector.get("name", "").startswith("freshness_")
            and any(
                isinstance(item, dict) and item.get("method") == "fqn"
                for item in selector.get("definition", {}).get("union", [])
            )
        }

        return [
            selector
            for selector in selectors
            if (
                selector.get("name", "")[len("freshness_") :] in model_selector_names
                if selector.get("name", "").startswith("freshness_")
                else selector.get("name", "") in model_selector_names
            )
        ]

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
                included_snapshots = [s for s in snapshots.keys() if s not in excluded_snapshots]
                if included_snapshots:
                    selector = {
                        "name": f"{self.config.selector_prefix}_snapshots",
                        "description": f"Selector for all snapshots in {snapshots_path}",
                        "definition": {"union": [{"method": "path", "value": snapshots_path}]},
                    }
                    selectors.append(selector)
                    logger.info(
                        f"Generated snapshots selector covering "
                        f"{len(included_snapshots)} snapshots "
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
                    "description": (
                        f"Selector for all snapshots ({len(included_snapshots)} snapshots)"
                    ),
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
                for resource_name in resource_names:
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
            # dbt requires "exclude" to hold a list, not a {union: [...]} dict.
            definition["union"].append({"exclude": exclusion_items})

        selector = {
            "name": f"{self.config.selector_prefix}_full_refresh_incremental",
            "description": "Selector for full refresh of all incremental models",
            "definition": definition,
        }

        logger.info(
            f"Generated full refresh selector for incremental models "
            f"(indirect_selection: {self.config.indirect_selection})"
        )

        return selector

    def write_selectors(self, selectors: List[Dict[str, Any]], output_path: str) -> None:
        """Write selectors to YAML file with blank lines between selectors.

        When config.reformat_manual_selectors is False, manual selectors are
        written using their original raw YAML text (preserving indentation,
        quoting, comments). Auto-generated selectors are always formatted
        through yaml.dump.

        Args:
            selectors: List of selector definitions
            output_path: Path to output file
        """
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

        # Separate manual and auto-generated selectors
        manual_gen = self.generators[SelectorPriority.MANUAL]
        manual_selectors = []
        auto_selectors = []
        for s in selectors:
            if manual_gen.is_manually_created(s):
                manual_selectors.append(s)
            else:
                auto_selectors.append(s)

        with io.StringIO() as f:
            # Write the selectors key
            f.write("selectors:\n")

            # Determine which selectors to dump vs preserve raw
            if not self.config.reformat_manual_selectors and self._raw_manual_blocks:
                # Write manual selectors as raw text (preserving original formatting)
                for raw_block in self._raw_manual_blocks:
                    f.write(raw_block)
                    f.write("\n\n")

                # Write auto-generated selectors through yaml.dump
                for i, selector in enumerate(auto_selectors):
                    self._write_single_selector(f, selector)
                    if i < len(auto_selectors) - 1:
                        f.write("\n")
            else:
                # Default: dump all selectors through yaml.dump (consistent formatting)
                all_selectors = manual_selectors + auto_selectors
                for i, selector in enumerate(all_selectors):
                    self._write_single_selector(f, selector)
                    if i < len(all_selectors) - 1:
                        f.write("\n")
            content = f.getvalue().encode("utf-8")

        if os.path.exists(output_path):
            with open(output_path, "rb") as existing:
                if existing.read() == content:
                    logger.info(f"Selectors unchanged; leaving {output_path} untouched")
                    return

        with open(output_path, "wb") as destination:
            destination.write(content)

    def _write_single_selector(self, f, selector: Dict[str, Any]) -> None:
        """Write a single selector to file using yaml.dump formatting.

        Args:
            f: File handle to write to
            selector: Selector definition dictionary
        """
        selector_yaml = yaml.dump([selector], default_flow_style=False, sort_keys=False, indent=2)

        # Remove the leading "- " from the first line and adjust indentation
        lines = selector_yaml.split("\n")
        if lines and lines[0].startswith("- "):
            lines[0] = "  - " + lines[0][2:]  # Add proper indentation
            for j in range(1, len(lines)):
                if lines[j]:  # Only add indentation to non-empty lines
                    lines[j] = "  " + lines[j]

        # Write the selector
        f.write("\n".join(lines))
