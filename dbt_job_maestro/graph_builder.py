"""Build dependency graphs from model dependencies"""

from typing import Dict, List, Set, Any
from collections import defaultdict


class GraphBuilder:
    """Build and analyze dependency graphs"""

    def __init__(self, models: Dict[str, Dict[str, Any]]):
        """
        Initialize the graph builder

        Args:
            models: Dictionary of model data from ManifestParser
        """
        self.models = models
        self.graph = self._build_graph()

    def _build_graph(self) -> Dict[str, Set[str]]:
        """
        Build undirected graph from model dependencies

        Returns:
            Dictionary mapping model names to their connected models
        """
        graph = defaultdict(set)

        for model_name, model_data in self.models.items():
            # Ensure model exists in graph even if it has no connections
            graph[model_name]

            # Add edges for dependencies
            for dep in model_data["dependencies"]:
                if dep in self.models:
                    graph[model_name].add(dep)
                    graph[dep].add(model_name)

        return dict(graph)

    def find_connected_components(self, exclude_models: Set[str] = None) -> List[List[str]]:
        """
        Find connected components in the dependency graph

        Args:
            exclude_models: Set of models to exclude from grouping

        Returns:
            List of connected components (each component is a list of model names)
        """
        if exclude_models is None:
            exclude_models = set()

        visited = set()
        components = []

        def dfs(node: str, component: List[str]) -> None:
            """Depth-first search to find connected component"""
            stack = [node]
            while stack:
                current_node = stack.pop()
                if current_node not in visited:
                    visited.add(current_node)
                    component.append(current_node)
                    for neighbor in self.graph.get(current_node, []):
                        if neighbor not in visited and neighbor not in exclude_models:
                            stack.append(neighbor)

        for node in self.graph:
            if node not in visited and node not in exclude_models:
                component = []
                dfs(node, component)
                if component:
                    components.append(sorted(component))

        return components

    def find_independent_models(self) -> List[str]:
        """
        Find models that have no dependencies and are not dependencies of others

        Returns:
            List of independent model names
        """
        independent_models = []
        dependent_models = set()

        # Find all models that are dependencies of others
        for model_data in self.models.values():
            for dep in model_data["dependencies"]:
                dependent_models.add(dep)

        # Find models with no dependencies that aren't dependencies of others
        for model_name, model_data in self.models.items():
            if not model_data["dependencies"] and model_name not in dependent_models:
                independent_models.append(model_name)

        return sorted(independent_models)

    def get_component_size(self, component: List[str]) -> int:
        """
        Get the size of a component

        Args:
            component: List of model names in the component

        Returns:
            Number of models in the component
        """
        return len(component)

    def get_models_with_sources(self) -> Set[str]:
        """
        Get models that depend on sources

        Returns:
            Set of model names that have source dependencies
        """
        return {name for name, data in self.models.items() if data.get("sources", [])}

    def group_by_path(self, path_prefix: str) -> List[str]:
        """
        Group models by path prefix

        Args:
            path_prefix: Path prefix to filter by (e.g., "models/staging" or "staging")

        Returns:
            List of model names under the path prefix
        """
        # Normalize the path prefix by removing common prefixes like "models/"
        normalized_prefix = path_prefix
        for common_prefix in ["models/", "model/"]:
            if normalized_prefix.startswith(common_prefix):
                normalized_prefix = normalized_prefix[len(common_prefix) :]
                break

        # Match against both the manifest path and original_file_path
        matched_models = []
        for name, data in self.models.items():
            model_path = data.get("path", "")
            original_path = data.get("original_file_path", "")

            # Check if either path starts with the normalized prefix
            if (
                model_path.startswith(normalized_prefix)
                or model_path.startswith(path_prefix)
                or original_path.startswith(normalized_prefix)
                or original_path.startswith(path_prefix)
            ):
                matched_models.append(name)

        return sorted(matched_models)

    def group_by_tag(self, tag: str) -> List[str]:
        """
        Group models by tag

        Args:
            tag: Tag to filter by

        Returns:
            List of model names with the tag
        """
        return sorted([name for name, data in self.models.items() if tag in data["tags"]])

    def get_models_in_paths(self, paths: List[str]) -> Set[str]:
        """
        Get all models that match any of the given path prefixes.

        Args:
            paths: List of path prefixes to match

        Returns:
            Set of model names that match any of the paths
        """
        matched_models = set()
        for path_prefix in paths:
            matched_models.update(self.group_by_path(path_prefix))
        return matched_models

    def get_models_by_names(self, names: List[str]) -> Set[str]:
        """
        Get models that match the given names (exact match or pattern).

        Args:
            names: List of model names to match

        Returns:
            Set of model names that exist in the manifest
        """
        return {name for name in names if name in self.models}
