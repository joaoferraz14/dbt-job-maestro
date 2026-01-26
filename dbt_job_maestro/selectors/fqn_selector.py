"""FQN-based selector generator."""

from typing import Dict, List, Any, Set

from dbt_job_maestro.base_selector import BaseSelector
from dbt_job_maestro.selector_types import SelectorPriority, SelectorMetadata
from dbt_job_maestro.model_resolver import ModelResolver


class FQNSelector(BaseSelector):
    """Generates FQN-based selectors using dependency analysis.

    Creates selectors based on connected components in the dependency graph,
    grouping related models together.
    """

    def get_priority(self) -> SelectorPriority:
        """Return the priority level for FQN selectors.

        Returns:
            SelectorPriority.AUTO_FQN
        """
        return SelectorPriority.AUTO_FQN

    def generate(self, excluded_models: Set[str]) -> List[Dict[str, Any]]:
        """Generate FQN-based selectors for unassigned models.

        Args:
            excluded_models: Models already assigned to higher-priority selectors

        Returns:
            List of FQN-based selector definitions
        """
        selectors = []

        if not self.config.group_by_dependencies:
            # Generate one selector per model
            for model_name in self.models:
                if model_name not in excluded_models:
                    selector = self._create_single_model_selector(model_name)
                    selectors.append(selector)
        else:
            # Find connected components (excluding already assigned models)
            components = self.graph.find_connected_components(exclude_models=excluded_models)

            # Track models added to component selectors
            component_models = set()

            # Generate selector for each component
            for component in components:
                filtered_component = [m for m in component if m not in excluded_models]

                if len(filtered_component) >= self.config.min_models_per_selector:
                    selector = self._create_component_selector(filtered_component)
                    selectors.append(selector)

                    # Track these models to exclude from independent selector
                    component_models.update(filtered_component)

                    # Optionally create freshness selector
                    if self._should_create_freshness(selector["name"]):
                        freshness = self._create_freshness_selector(
                            selector["name"], filtered_component
                        )
                        selectors.append(freshness)

            # Handle independent models (exclude models already in components)
            independent = set(self.graph.find_independent_models())
            filtered_independent = [
                m for m in independent if m not in excluded_models and m not in component_models
            ]

            if filtered_independent:
                selector = self._create_independent_selector(filtered_independent)
                selectors.append(selector)

                if self._should_create_freshness("selector_independent"):
                    freshness = self._create_freshness_selector(
                        "selector_independent", filtered_independent
                    )
                    selectors.append(freshness)

        return selectors

    def extract_metadata(self, selector_def: Dict[str, Any]) -> SelectorMetadata:
        """Extract metadata from an FQN selector.

        Args:
            selector_def: Selector definition dictionary

        Returns:
            SelectorMetadata with resolved models and metadata
        """
        resolver = ModelResolver(self.parser, self.graph)
        resolution = resolver.resolve_selector(selector_def)

        return SelectorMetadata(
            name=selector_def.get("name", ""),
            priority=SelectorPriority.AUTO_FQN,
            manually_created=False,
            models_covered=resolution.models,
            paths_used=resolution.paths,
            tags_used=resolution.tags,
            fqns_used=resolution.fqns,
            invalid_fqns=resolution.invalid_fqns,
        )

    def _create_component_selector(self, models: List[str]) -> Dict[str, Any]:
        """Create selector for a dependency component.

        Args:
            models: List of model names in the component

        Returns:
            Selector definition dictionary
        """
        sorted_models = self._custom_sort(models)
        first_model = sorted_models[0]

        selector = {
            "name": f"{self.config.selector_prefix}_{first_model}",
            "description": f"Selector for models in component starting with {first_model}",
            "definition": {"union": []},
        }

        models_with_sources = self.graph.get_models_with_sources()

        for model in sorted_models:
            if model in models_with_sources and self.config.include_parent_sources:
                selector["definition"]["union"].append(
                    {"method": "fqn", "value": model, "parents": True}
                )
            else:
                selector["definition"]["union"].append({"method": "fqn", "value": model})

        if self.config.exclude_tags:
            selector["definition"]["union"].append(self._create_tag_exclusion())
        if self.config.exclude_paths:
            selector["definition"]["union"].append(self._create_path_exclusion())

        return selector

    def _create_single_model_selector(self, model_name: str) -> Dict[str, Any]:
        """Create selector for a single model.

        Args:
            model_name: Name of the model

        Returns:
            Selector definition dictionary
        """
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
        if self.config.exclude_paths:
            selector["definition"]["union"].append(self._create_path_exclusion())

        return selector

    def _create_independent_selector(self, models: List[str]) -> Dict[str, Any]:
        """Create selector for independent models.

        Args:
            models: List of independent model names

        Returns:
            Selector definition dictionary
        """
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
        if self.config.exclude_paths:
            selector["definition"]["union"].append(self._create_path_exclusion())

        return selector

    def _create_freshness_selector(self, base_name: str, models: List[str]) -> Dict[str, Any]:
        """Create freshness selector.

        Args:
            base_name: Base selector name
            models: List of models covered by base selector

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

    def _create_tag_exclusion(self) -> Dict[str, Any]:
        """Create tag exclusion definition.

        Returns:
            Exclusion definition dictionary
        """
        return {
            "exclude": {
                "union": [{"method": "tag", "value": tag} for tag in self.config.exclude_tags]
            }
        }

    def _create_path_exclusion(self) -> Dict[str, Any]:
        """Create path exclusion definition for runtime enforcement by dbt.

        Returns:
            Exclusion definition dictionary
        """
        return {
            "exclude": {
                "union": [{"method": "path", "value": path} for path in self.config.exclude_paths]
            }
        }

    def _custom_sort(self, models: List[str]) -> List[str]:
        """Sort models based on prefix_order config.

        If prefix_order is empty, models are sorted alphabetically (reverse).
        If prefix_order is specified, models with those prefixes are prioritized.

        Args:
            models: List of model names to sort

        Returns:
            Sorted list of model names
        """
        if not self.config.prefix_order:
            return sorted(models, reverse=True)

        sorted_with_prefix = sorted(
            [m for m in models if any(m.startswith(p) for p in self.config.prefix_order)],
            reverse=True,
        )

        sorted_without_prefix = sorted(
            [m for m in models if not any(m.startswith(p) for p in self.config.prefix_order)],
            reverse=True,
        )

        return sorted_with_prefix + sorted_without_prefix

    def _should_create_freshness(self, selector_name: str) -> bool:
        """Determine if a freshness selector should be created for this selector.

        Logic:
        - If freshness_selector_names is provided, only create freshness for those selectors
        - Otherwise, use the global include_freshness_selectors flag

        Args:
            selector_name: Name of the selector to check

        Returns:
            True if freshness selector should be created, False otherwise
        """
        if self.config.freshness_selector_names:
            return selector_name in self.config.freshness_selector_names
        return self.config.include_freshness_selectors
