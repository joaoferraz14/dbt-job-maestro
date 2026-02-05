"""Generate dbt selectors from model dependencies"""

import logging
import os
import yaml
from typing import Any, Dict, List, Set, Optional

from dbt_job_maestro.config import SelectorConfig
from dbt_job_maestro.manifest_parser import ManifestParser
from dbt_job_maestro.graph_builder import GraphBuilder

logger = logging.getLogger(__name__)


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

    def _should_include_model(
        self, model_name: str, excluded_models: Optional[Set[str]] = None
    ) -> bool:
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

                    if self._should_create_freshness(selector["name"]):
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

                if self._should_create_freshness("selector_independent"):
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

            if models:
                selector_name = self._path_to_selector_name(path_prefix)
                selector = {
                    "name": f"path_{selector_name}",
                    "description": f"Selector for models in {path_prefix}",
                    "definition": {"union": [{"method": "path", "value": path_prefix}]},
                }

                # Add exclusions if configured
                if self.config.exclude_tags:
                    selector["definition"]["union"].append(self._create_tag_exclusion())
                if self.config.exclude_paths:
                    selector["definition"]["union"].append(self._create_path_exclusion())

                selectors.append(selector)

                if self._should_create_freshness(f"path_{selector_name}"):
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

        # Find models without any tags and warn about them
        all_model_names = set(self.models.keys()) - excluded_models
        tagged_models = set()

        for tag in sorted(included_tags):
            models = self.graph.group_by_tag(tag)
            # Filter out excluded models
            models = [m for m in models if m not in excluded_models]
            tagged_models.update(models)

            if models:
                selector = {
                    "name": f"tag_{tag}",
                    "description": f"Selector for models tagged with {tag}",
                    "definition": {"union": [{"method": "tag", "value": tag}]},
                }

                # Add path exclusions for runtime enforcement
                if self.config.exclude_paths:
                    selector["definition"]["union"].append(self._create_path_exclusion())

                selectors.append(selector)

                if self._should_create_freshness(f"tag_{tag}"):
                    freshness_selector = self._create_freshness_selector(f"tag_{tag}", models)
                    selectors.append(freshness_selector)

        # Warn about untagged models
        untagged_models = all_model_names - tagged_models
        if untagged_models:
            logger.warning(
                f"\n⚠️  WARNING: {len(untagged_models)} model(s) have no tags and will NOT be "
                f"included in any selector when using method='tag':"
            )
            for model in sorted(untagged_models)[:10]:
                logger.warning(f"    - {model}")
            if len(untagged_models) > 10:
                logger.warning(f"    ... and {len(untagged_models) - 10} more")
            logger.warning(
                "\n💡 RECOMMENDATION: Use method='fqn' to ensure all models are included "
                "in selectors. The 'fqn' method groups models by dependencies "
                "for complete coverage."
            )

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
        if self.config.exclude_paths:
            selector["definition"]["union"].append(self._create_path_exclusion())

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
        if self.config.exclude_paths:
            selector["definition"]["union"].append(self._create_path_exclusion())

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
        if self.config.exclude_paths:
            selector["definition"]["union"].append(self._create_path_exclusion())

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

    def _create_path_exclusion(self) -> Dict[str, Any]:
        """Create path exclusion definition for runtime enforcement by dbt"""
        return {
            "exclude": {
                "union": [{"method": "path", "value": path} for path in self.config.exclude_paths]
            }
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
