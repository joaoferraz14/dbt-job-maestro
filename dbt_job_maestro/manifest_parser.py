"""Parse dbt manifest.json file and extract model information"""

import json
from typing import Any, Dict, List, Set
from pathlib import Path


class ManifestParser:
    """Parser for dbt manifest.json files"""

    def __init__(self, manifest_path: str):
        """
        Initialize the manifest parser

        Args:
            manifest_path: Path to the manifest.json file
        """
        self.manifest_path = Path(manifest_path)
        self.manifest_data = self._load_manifest()

    def _load_manifest(self) -> Dict[str, Any]:
        """Load manifest.json file"""
        if not self.manifest_path.exists():
            raise FileNotFoundError(f"Manifest file not found: {self.manifest_path}")

        with open(self.manifest_path, "r") as file:
            return json.load(file)

    def get_models(self) -> Dict[str, Dict[str, Any]]:
        """
        Extract model information from manifest

        Returns:
            Dictionary mapping model names to their metadata including:
            - name: model name
            - fqn: fully qualified name
            - path: file path
            - tags: list of tags
            - dependencies: list of model dependencies
            - sources: list of source dependencies
        """
        models = {}

        for node_id, node_data in self.manifest_data.get("nodes", {}).items():
            if not node_id.startswith("model."):
                continue

            model_name = node_id.split(".")[-1]

            # Extract dependencies
            dependencies = []
            sources = []

            if node_data.get("depends_on", {}).get("nodes"):
                for dep_id in set(node_data["depends_on"]["nodes"]):
                    if dep_id.startswith("model."):
                        dep_name = dep_id.split(".")[-1]
                        dependencies.append(dep_name)
                    elif dep_id.startswith("source."):
                        source_name = ".".join(dep_id.rsplit(".", 2)[-2:])
                        sources.append(source_name)

            models[model_name] = {
                "name": model_name,
                "fqn": node_data.get("fqn", []),
                "path": node_data.get("path", ""),
                "original_file_path": node_data.get("original_file_path", ""),
                "tags": node_data.get("tags", []),
                "dependencies": dependencies,
                "sources": sources,
                "resource_type": node_data.get("resource_type", "model"),
            }

        return models

    def get_model_paths(self) -> Dict[str, str]:
        """
        Get mapping of model names to their file paths

        Returns:
            Dictionary mapping model name to file path
        """
        models = self.get_models()
        return {name: data["path"] for name, data in models.items()}

    def get_model_tags(self) -> Dict[str, List[str]]:
        """
        Get mapping of model names to their tags

        Returns:
            Dictionary mapping model name to list of tags
        """
        models = self.get_models()
        return {name: data["tags"] for name, data in models.items()}

    def get_models_by_tag(self, tag: str) -> List[str]:
        """
        Get all models with a specific tag

        Args:
            tag: Tag to filter by

        Returns:
            List of model names
        """
        models = self.get_models()
        return [name for name, data in models.items() if tag in data["tags"]]

    def get_models_by_path(self, path_prefix: str) -> List[str]:
        """
        Get all models under a specific path

        Args:
            path_prefix: Path prefix to filter by

        Returns:
            List of model names
        """
        models = self.get_models()
        return [name for name, data in models.items() if data["path"].startswith(path_prefix)]

    def get_path_prefixes(self, level: int = 1) -> Set[str]:
        """
        Get unique path prefixes at a specific directory level

        Args:
            level: Directory level (0 = root, 1 = first subdirectory, etc.)

        Returns:
            Set of unique path prefixes
        """
        models = self.get_models()
        prefixes = set()

        for data in models.values():
            path_parts = Path(data["path"]).parts
            if len(path_parts) > level:
                prefix = str(Path(*path_parts[: level + 1]))
                prefixes.add(prefix)

        return prefixes

    def get_all_tags(self) -> Set[str]:
        """
        Get all unique tags from the manifest

        Returns:
            Set of unique tags
        """
        models = self.get_models()
        tags = set()

        for data in models.values():
            tags.update(data["tags"])

        return tags

    def get_seeds(self) -> Dict[str, Dict[str, Any]]:
        """
        Extract seed information from manifest

        Returns:
            Dictionary mapping seed names to their metadata including:
            - name: seed name
            - fqn: fully qualified name
            - path: file path
            - tags: list of tags
        """
        seeds = {}

        for node_id, node_data in self.manifest_data.get("nodes", {}).items():
            if not node_id.startswith("seed."):
                continue

            seed_name = node_id.split(".")[-1]

            seeds[seed_name] = {
                "name": seed_name,
                "fqn": node_data.get("fqn", []),
                "path": node_data.get("path", ""),
                "original_file_path": node_data.get("original_file_path", ""),
                "tags": node_data.get("tags", []),
                "resource_type": "seed",
            }

        return seeds

    def get_snapshots(self) -> Dict[str, Dict[str, Any]]:
        """
        Extract snapshot information from manifest

        Returns:
            Dictionary mapping snapshot names to their metadata including:
            - name: snapshot name
            - fqn: fully qualified name
            - path: file path
            - tags: list of tags
        """
        snapshots = {}

        for node_id, node_data in self.manifest_data.get("nodes", {}).items():
            if not node_id.startswith("snapshot."):
                continue

            snapshot_name = node_id.split(".")[-1]

            snapshots[snapshot_name] = {
                "name": snapshot_name,
                "fqn": node_data.get("fqn", []),
                "path": node_data.get("path", ""),
                "original_file_path": node_data.get("original_file_path", ""),
                "tags": node_data.get("tags", []),
                "resource_type": "snapshot",
            }

        return snapshots

    def get_seeds_path_prefixes(self, level: int = 0) -> Set[str]:
        """
        Get unique path prefixes for seeds at a specific directory level

        Args:
            level: Directory level (0 = root level, 1 = first subdirectory, etc.)

        Returns:
            Set of unique path prefixes for seeds
        """
        seeds = self.get_seeds()
        prefixes = set()

        for data in seeds.values():
            path_parts = Path(data["path"]).parts
            if len(path_parts) > level:
                prefix = str(Path(*path_parts[: level + 1]))
                prefixes.add(prefix)

        return prefixes

    def get_snapshots_path_prefixes(self, level: int = 0) -> Set[str]:
        """
        Get unique path prefixes for snapshots at a specific directory level

        Args:
            level: Directory level (0 = root level, 1 = first subdirectory, etc.)

        Returns:
            Set of unique path prefixes for snapshots
        """
        snapshots = self.get_snapshots()
        prefixes = set()

        for data in snapshots.values():
            path_parts = Path(data["path"]).parts
            if len(path_parts) > level:
                prefix = str(Path(*path_parts[: level + 1]))
                prefixes.add(prefix)

        return prefixes
