"""Resolve models from selector definitions."""

from typing import Dict, Any, Set
from dbt_job_maestro.selector_types import ModelResolution


class ModelResolver:
    """Resolves which models are covered by a selector definition.

    This class can handle complex selector definitions with union, intersection,
    and exclude operations, and supports fqn, tag, and path methods.
    """

    def __init__(self, manifest_parser, graph_builder):
        """Initialize the model resolver.

        Args:
            manifest_parser: ManifestParser instance
            graph_builder: GraphBuilder instance
        """
        self.parser = manifest_parser
        self.graph = graph_builder
        self.models = manifest_parser.get_models()

    def resolve_selector(self, selector_def: Dict[str, Any]) -> ModelResolution:
        """Resolve all models, paths, tags, and fqns from a selector definition.

        Args:
            selector_def: Selector definition dictionary

        Returns:
            ModelResolution with all extracted information
        """
        models = set()
        paths = set()
        tags = set()
        fqns = set()

        definition = selector_def.get("definition", {})

        # Process union definitions
        if "union" in definition:
            for item in definition["union"]:
                resolved = self._resolve_item(item)
                models.update(resolved.models)
                paths.update(resolved.paths)
                tags.update(resolved.tags)
                fqns.update(resolved.fqns)

        # Process intersection definitions
        if "intersection" in definition:
            all_sets = []
            for item in definition["intersection"]:
                resolved = self._resolve_item(item)
                all_sets.append(resolved.models)
                paths.update(resolved.paths)
                tags.update(resolved.tags)
                fqns.update(resolved.fqns)

            if all_sets:
                models = set.intersection(*all_sets)

        # Handle exclusions
        if "exclude" in definition:
            excluded = self._resolve_exclusions(definition["exclude"])
            models = models - excluded.models

        return ModelResolution(
            models=models,
            paths=paths,
            tags=tags,
            fqns=fqns
        )

    def _resolve_item(self, item: Dict[str, Any]) -> ModelResolution:
        """Resolve a single definition item.

        Args:
            item: Definition item dictionary

        Returns:
            ModelResolution for this item
        """
        models = set()
        paths = set()
        tags = set()
        fqns = set()

        if not isinstance(item, dict):
            return ModelResolution(models=models, paths=paths, tags=tags, fqns=fqns)

        method = item.get("method", "")
        value = item.get("value", "")
        include_parents = item.get("parents", False)
        include_children = item.get("children", False)

        if method == "fqn":
            # Direct FQN reference
            if value in self.models:
                models.add(value)
                fqns.add(value)

                # Handle parents/children
                if include_parents:
                    models.update(self._get_parents(value))
                if include_children:
                    models.update(self._get_children(value))

        elif method == "tag":
            # All models with this tag
            tag_models = self.graph.group_by_tag(value)
            models.update(tag_models)
            tags.add(value)

        elif method == "path":
            # All models in this path
            path_models = self.graph.group_by_path(value)
            models.update(path_models)
            paths.add(value)

        elif method == "selector":
            # Reference to another selector - skip to avoid circular dependencies
            # In a full implementation, would need a selector registry
            pass

        # Recursive handling for nested definitions
        if "union" in item:
            for sub_item in item["union"]:
                resolved = self._resolve_item(sub_item)
                models.update(resolved.models)
                paths.update(resolved.paths)
                tags.update(resolved.tags)
                fqns.update(resolved.fqns)

        if "intersection" in item:
            all_sets = []
            for sub_item in item["intersection"]:
                resolved = self._resolve_item(sub_item)
                all_sets.append(resolved.models)
                paths.update(resolved.paths)
                tags.update(resolved.tags)
                fqns.update(resolved.fqns)

            if all_sets:
                models = set.intersection(*all_sets)

        if "exclude" in item:
            excluded = self._resolve_exclusions(item["exclude"])
            models = models - excluded.models

        return ModelResolution(
            models=models,
            paths=paths,
            tags=tags,
            fqns=fqns
        )

    def _resolve_exclusions(self, exclude_def: Dict[str, Any]) -> ModelResolution:
        """Resolve exclusion definitions.

        Args:
            exclude_def: Exclusion definition dictionary

        Returns:
            ModelResolution for excluded models
        """
        models = set()

        if "union" in exclude_def:
            for item in exclude_def["union"]:
                resolved = self._resolve_item(item)
                models.update(resolved.models)

        return ModelResolution(models=models, paths=set(), tags=set(), fqns=set())

    def _get_parents(self, model_name: str) -> Set[str]:
        """Get all parent models (dependencies).

        Args:
            model_name: Name of the model

        Returns:
            Set of parent model names
        """
        if model_name not in self.models:
            return set()

        return set(self.models[model_name].get("dependencies", []))

    def _get_children(self, model_name: str) -> Set[str]:
        """Get all child models (dependents).

        Args:
            model_name: Name of the model

        Returns:
            Set of child model names
        """
        children = set()
        for name, data in self.models.items():
            if model_name in data.get("dependencies", []):
                children.add(name)
        return children
