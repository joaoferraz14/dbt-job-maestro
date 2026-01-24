"""Generate dbt selectors from model dependencies"""

import os
import yaml
from typing import Any, Dict, List, Set, Tuple
from pathlib import Path

from dbt_job_maestro.config import SelectorConfig
from dbt_job_maestro.manifest_parser import ManifestParser
from dbt_job_maestro.graph_builder import GraphBuilder


class SelectorGenerator:
    """Generate dbt selector definitions"""

    def __init__(
        self,
        manifest_parser: ManifestParser,
        graph_builder: GraphBuilder,
        config: SelectorConfig,
    ):
        """
        Initialize selector generator

        Args:
            manifest_parser: ManifestParser instance
            graph_builder: GraphBuilder instance
            config: SelectorConfig instance
        """
        self.parser = manifest_parser
        self.graph = graph_builder
        self.config = config
        self.models = manifest_parser.get_models()

    def generate_selectors(self) -> List[Dict[str, Any]]:
        """
        Generate selectors based on configuration

        Returns:
            List of selector definitions
        """
        if self.config.method == "fqn":
            return self._generate_fqn_selectors()
        elif self.config.method == "path":
            return self._generate_path_selectors()
        elif self.config.method == "tag":
            return self._generate_tag_selectors()
        elif self.config.method == "mixed":
            return self._generate_mixed_selectors()
        else:
            raise ValueError(f"Unknown selector method: {self.config.method}")

    def _get_excluded_models(self) -> Set[str]:
        """
        Get models to exclude based on config's exclude_paths and exclude_models.

        Returns:
            Set of model names to exclude from selector generation
        """
        excluded = set()

        # Exclude models matching exclude_paths
        if self.config.exclude_paths:
            for path_prefix in self.config.exclude_paths:
                excluded.update(self.graph.group_by_path(path_prefix))

        # Exclude models by name
        if self.config.exclude_models:
            for model_name in self.config.exclude_models:
                if model_name in self.models:
                    excluded.add(model_name)

        return excluded

    def _should_include_model(self, model_name: str, excluded_models: Set[str] = None) -> bool:
        """
        Check if a model should be included in selector generation.

        Args:
            model_name: Name of the model to check
            excluded_models: Optional set of already excluded models

        Returns:
            True if model should be included, False otherwise
        """
        if excluded_models is None:
            excluded_models = self._get_excluded_models()
        return model_name not in excluded_models

    def _generate_fqn_selectors(self) -> List[Dict[str, Any]]:
        """Generate selectors using FQN (fully qualified names)"""
        selectors = []

        # Get models to exclude from config (exclude_paths and exclude_models)
        excluded_models = self._get_excluded_models()

        # Get manually generated models to exclude
        manually_generated_models = set()
        manually_generated_models.update(excluded_models)

        if self.config.group_by_dependencies:
            # Find connected components
            independent_models = set(self.graph.find_independent_models())
            components = self.graph.find_connected_components(
                exclude_models=manually_generated_models
            )

            # Generate selector for each component
            for component in components:
                filtered_component = [m for m in component if m not in manually_generated_models]

                if filtered_component:
                    selector = self._create_fqn_selector_for_component(filtered_component)
                    selectors.append(selector)

                    if self.config.include_freshness_selectors:
                        freshness_selector = self._create_freshness_selector(
                            selector["name"], filtered_component
                        )
                        selectors.append(freshness_selector)

            # Handle independent models
            filtered_independent = [
                m for m in independent_models if m not in manually_generated_models
            ]

            if filtered_independent:
                independent_selector = self._create_independent_selector(list(filtered_independent))
                selectors.append(independent_selector)

                if self.config.include_freshness_selectors:
                    freshness_selector = self._create_freshness_selector(
                        "selector_independent", list(filtered_independent)
                    )
                    selectors.append(freshness_selector)
        else:
            # Generate one selector per model
            for model_name in self.models:
                if model_name not in manually_generated_models:
                    selector = self._create_single_model_selector(model_name)
                    selectors.append(selector)

        return selectors

    def _generate_path_selectors(self) -> List[Dict[str, Any]]:
        """Generate selectors based on directory paths"""
        selectors = []
        excluded_models = self._get_excluded_models()

        # Get path prefixes at configured level
        path_prefixes = self.parser.get_path_prefixes(self.config.path_grouping_level)

        for path_prefix in sorted(path_prefixes):
            models = self.graph.group_by_path(path_prefix)
            # Filter out excluded models
            models = [m for m in models if m not in excluded_models]

            if len(models) >= self.config.min_models_per_selector:
                selector_name = self._path_to_selector_name(path_prefix)
                selector = {
                    "name": f"path_{selector_name}",
                    "description": f"Selector for models in {path_prefix}",
                    "definition": {"union": [{"method": "path", "value": path_prefix}]},
                }

                # Add exclusions if configured
                if self.config.exclude_tags:
                    selector["definition"]["union"].append(self._create_tag_exclusion())

                selectors.append(selector)

                if self.config.include_freshness_selectors:
                    freshness_selector = self._create_freshness_selector(
                        f"path_{selector_name}", models
                    )
                    selectors.append(freshness_selector)

        return selectors

    def _generate_tag_selectors(self) -> List[Dict[str, Any]]:
        """Generate selectors based on tags"""
        selectors = []
        excluded_models = self._get_excluded_models()

        # Get all unique tags
        all_tags = self.parser.get_all_tags()

        # Filter out excluded tags
        included_tags = all_tags - set(self.config.exclude_tags)

        for tag in sorted(included_tags):
            models = self.graph.group_by_tag(tag)
            # Filter out excluded models
            models = [m for m in models if m not in excluded_models]

            if len(models) >= self.config.min_models_per_selector:
                selector = {
                    "name": f"tag_{tag}",
                    "description": f"Selector for models tagged with {tag}",
                    "definition": {"union": [{"method": "tag", "value": tag}]},
                }

                selectors.append(selector)

                if self.config.include_freshness_selectors:
                    freshness_selector = self._create_freshness_selector(f"tag_{tag}", models)
                    selectors.append(freshness_selector)

        return selectors

    def _generate_mixed_selectors(self) -> List[Dict[str, Any]]:
        """
        Generate selectors using a mix of methods with priority system.

        Priority order for automated selectors (NO duplicates):
        1. FQN-based selectors (highest priority - dependency grouping)
        2. Path-based selectors (from include_path_groups config)
        3. Tag-based selectors (lowest priority - remaining tagged models)

        Manual selectors are preserved and can have duplicates.
        Overlaps are detected and reported as warnings.
        """
        selectors = []
        # Start with models excluded by config (exclude_paths and exclude_models)
        assigned_models = (
            self._get_excluded_models()
        )  # Track models assigned in automated selectors

        # Stage 0: Preserve manually created selectors (can have duplicates)
        manual_selectors = []
        manual_model_assignments = {}  # Track which models are in which manual selectors

        if self.config.preserve_manual_selectors:
            manual_selectors, manual_models, _, _ = self._read_manual_selectors()
            selectors.extend(manual_selectors)

            # Track manual selector assignments for overlap detection
            for selector in manual_selectors:
                models = self._extract_models_from_selector(selector)
                for model in models:
                    if model not in manual_model_assignments:
                        manual_model_assignments[model] = []
                    manual_model_assignments[model].append((selector["name"], "manual"))

        # Stage 1: Create FQN-based selectors (HIGHEST PRIORITY)
        if self.config.group_by_dependencies:
            components = self.graph.find_connected_components(exclude_models=set())

            for component in components:
                if component and len(component) >= self.config.min_models_per_selector:
                    selector = self._create_fqn_selector_for_component(component)
                    selectors.append(selector)
                    assigned_models.update(component)

                    if self.config.include_freshness_selectors:
                        freshness_selector = self._create_freshness_selector(
                            selector["name"], component
                        )
                        selectors.append(freshness_selector)

        # Stage 2: Create path-based selectors (SECOND PRIORITY - exclude FQN models)
        if self.config.include_path_groups:
            path_selectors, path_models = self._create_path_group_selectors(
                assigned_models, self.config.include_path_groups
            )
            selectors.extend(path_selectors)
            assigned_models.update(path_models)

        # Stage 3: Create tag-based selectors (LOWEST PRIORITY - exclude FQN and path models)
        all_tags = self.parser.get_all_tags() - set(self.config.exclude_tags)

        for tag in sorted(all_tags):
            tag_models = self.graph.group_by_tag(tag)
            # Only include models not already assigned
            unassigned_tag_models = [m for m in tag_models if m not in assigned_models]

            if len(unassigned_tag_models) >= self.config.min_models_per_selector:
                selector = {
                    "name": f"tag_{tag}",
                    "description": f"Selector for models tagged with {tag}",
                    "definition": {"union": [{"method": "tag", "value": tag}]},
                }
                selectors.append(selector)
                assigned_models.update(unassigned_tag_models)

                if self.config.include_freshness_selectors:
                    freshness_selector = self._create_freshness_selector(
                        f"tag_{tag}", unassigned_tag_models
                    )
                    selectors.append(freshness_selector)

        # Detect and report overlaps
        self._detect_and_report_overlaps(selectors, manual_model_assignments)

        return selectors

    def _create_fqn_selector_for_component(self, models: List[str]) -> Dict[str, Any]:
        """Create a selector for a component using FQN method"""
        sorted_models = self._custom_sort(models)
        first_model = sorted_models[0]

        selector = {
            "name": f"automatically_generated_selector_{first_model}",
            "description": f"Selector for models in component starting with {first_model}",
            "definition": {"union": []},
        }

        # Add models to selector
        models_with_sources = self.graph.get_models_with_sources()

        for model in sorted_models:
            if model in models_with_sources and self.config.include_parent_sources:
                selector["definition"]["union"].append(
                    {"method": "fqn", "value": model, "parents": True}
                )
            else:
                selector["definition"]["union"].append({"method": "fqn", "value": model})

        # Add tag exclusions
        if self.config.exclude_tags:
            selector["definition"]["union"].append(self._create_tag_exclusion())

        return selector

    def _create_single_model_selector(self, model_name: str) -> Dict[str, Any]:
        """Create a selector for a single model"""
        models_with_sources = self.graph.get_models_with_sources()

        selector = {
            "name": f"model_{model_name}",
            "description": f"Selector for model {model_name}",
            "definition": {"union": []},
        }

        if model_name in models_with_sources and self.config.include_parent_sources:
            selector["definition"]["union"].append(
                {"method": "fqn", "value": model_name, "parents": True}
            )
        else:
            selector["definition"]["union"].append({"method": "fqn", "value": model_name})

        if self.config.exclude_tags:
            selector["definition"]["union"].append(self._create_tag_exclusion())

        return selector

    def _create_independent_selector(self, models: List[str]) -> Dict[str, Any]:
        """Create selector for independent models"""
        sorted_models = self._custom_sort(models)
        models_with_sources = self.graph.get_models_with_sources()

        selector = {
            "name": "selector_independent",
            "description": "Selector for independent models",
            "definition": {"union": []},
        }

        for model in sorted_models:
            if model in models_with_sources and self.config.include_parent_sources:
                selector["definition"]["union"].append(
                    {"method": "fqn", "value": model, "parents": True}
                )
            else:
                selector["definition"]["union"].append({"method": "fqn", "value": model})

        if self.config.exclude_tags:
            selector["definition"]["union"].append(self._create_tag_exclusion())

        return selector

    def _create_freshness_selector(
        self, base_selector_name: str, models: List[str]
    ) -> Dict[str, Any]:
        """Create a freshness selector for a base selector"""
        return {
            "name": f"freshness_{base_selector_name}",
            "description": f"Freshness selector for {base_selector_name}",
            "definition": {
                "union": [
                    {
                        "intersection": [
                            {"method": "selector", "value": base_selector_name},
                            {
                                "method": "source_status",
                                "value": "fresher",
                                "children": True,
                            },
                        ]
                    }
                ]
            },
        }

    def _create_tag_exclusion(self) -> Dict[str, Any]:
        """Create tag exclusion definition"""
        return {
            "exclude": {
                "union": [{"method": "tag", "value": tag} for tag in self.config.exclude_tags]
            }
        }

    def _custom_sort(self, models: List[str]) -> List[str]:
        """
        Sort models based on prefix order configuration

        If prefix_order is empty, models are sorted alphabetically (reverse).
        If prefix_order is specified, models with those prefixes are prioritized.
        """
        # If no prefix order specified, just sort alphabetically
        if not self.config.prefix_order:
            return sorted(models, reverse=True)

        # Otherwise, prioritize models with specified prefixes
        sorted_with_prefix = sorted(
            [
                model
                for model in models
                if any(model.startswith(prefix) for prefix in self.config.prefix_order)
            ],
            reverse=True,
        )
        sorted_without_prefix = sorted(
            [
                model
                for model in models
                if not any(model.startswith(prefix) for prefix in self.config.prefix_order)
            ],
            reverse=True,
        )
        return sorted_with_prefix + sorted_without_prefix

    def _path_to_selector_name(self, path: str) -> str:
        """Convert path to valid selector name"""
        return path.replace("/", "_").replace("\\", "_").replace(".", "_")

    def _read_manual_selectors(self) -> Tuple[List[Dict[str, Any]], Set[str], Set[str], Set[str]]:
        """
        Read existing manually created selectors from file.

        Returns:
            Tuple of (list of manual selectors, set of models, set of paths used, set of tags used)
        """
        manual_selectors = []
        manual_models = set()
        manual_paths = set()
        manual_tags = set()

        # Try common locations for selectors file
        possible_paths = [
            Path("selectors.yml"),
            Path("dbt_project/selectors.yml"),
            Path("./selectors.yml"),
        ]

        existing = []
        for path in possible_paths:
            if path.exists():
                existing = self.read_existing_selectors(str(path))
                break

        if not existing:
            return manual_selectors, manual_models, manual_paths, manual_tags

        for selector in existing:
            description = selector.get("description", "")
            # Check if selector is manually created
            if "manually_created" in description.lower() or not description.startswith(
                "Selector for"
            ):
                manual_selectors.append(selector)
                # Extract models, paths, and tags from this selector
                models, paths, tags = self._extract_selector_components(selector)
                manual_models.update(models)
                manual_paths.update(paths)
                manual_tags.update(tags)

        return manual_selectors, manual_models, manual_paths, manual_tags

    def _extract_models_from_selector(self, selector: Dict[str, Any]) -> Set[str]:
        """
        Extract model names covered by a selector definition.

        Args:
            selector: Selector definition

        Returns:
            Set of model names covered by the selector
        """
        models, _, _ = self._extract_selector_components(selector)
        return models

    def _extract_selector_components(
        self, selector: Dict[str, Any]
    ) -> Tuple[Set[str], Set[str], Set[str]]:
        """
        Extract models, paths, and tags from a selector definition.

        Args:
            selector: Selector definition

        Returns:
            Tuple of (models set, paths set, tags set)
        """
        models = set()
        paths = set()
        tags = set()
        definition = selector.get("definition", {})

        # Handle union definitions
        if "union" in definition:
            for item in definition["union"]:
                if "method" in item:
                    method = item["method"]
                    value = item.get("value", "")

                    if method == "fqn":
                        # Direct model reference
                        if value in self.models:
                            models.add(value)
                    elif method == "tag":
                        # All models with this tag
                        models.update(self.graph.group_by_tag(value))
                        tags.add(value)  # Track the tag itself
                    elif method == "path":
                        # All models in this path
                        models.update(self.graph.group_by_path(value))
                        paths.add(value)  # Track the path itself

        return models, paths, tags

    def _create_path_group_selectors(
        self, assigned_models: Set[str], path_groups: List[str] = None
    ) -> Tuple[List[Dict[str, Any]], Set[str]]:
        """
        Create selectors for specified path groups.

        Args:
            assigned_models: Models already assigned to other selectors
            path_groups: List of paths to create selectors for (defaults to config.include_path_groups)

        Returns:
            Tuple of (list of path selectors, set of models covered by them)
        """
        selectors = []
        path_models = set()

        # Use provided path_groups or fall back to config
        paths_to_process = (
            path_groups if path_groups is not None else self.config.include_path_groups
        )

        for path_group in paths_to_process:
            models = self.graph.group_by_path(path_group)
            # Only include models not already assigned
            unassigned_models = [m for m in models if m not in assigned_models]

            if len(unassigned_models) >= self.config.min_models_per_selector:
                selector_name = self._path_to_selector_name(path_group)
                selector = {
                    "name": f"path_{selector_name}",
                    "description": f"Selector for models in {path_group}",
                    "definition": {"union": [{"method": "path", "value": path_group}]},
                }

                # Add exclusions if configured
                if self.config.exclude_tags:
                    selector["definition"]["union"].append(self._create_tag_exclusion())

                selectors.append(selector)
                path_models.update(unassigned_models)

                if self.config.include_freshness_selectors:
                    freshness_selector = self._create_freshness_selector(
                        f"path_{selector_name}", unassigned_models
                    )
                    selectors.append(freshness_selector)

        return selectors, path_models

    def write_selectors(self, selectors: List[Dict[str, Any]], output_path: str) -> None:
        """
        Write selectors to YAML file

        Args:
            selectors: List of selector definitions
            output_path: Path to output file
        """
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

        with open(output_path, "w") as outfile:
            yaml.dump(
                {"selectors": selectors},
                outfile,
                default_flow_style=False,
                sort_keys=False,
                indent=2,
            )

    def read_existing_selectors(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Read existing selectors from file

        Args:
            file_path: Path to selectors file

        Returns:
            List of existing selector definitions
        """
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            with open(file_path, "r") as infile:
                content = yaml.safe_load(infile)
                if content is None:
                    return []
                return content.get("selectors", [])
        return []
