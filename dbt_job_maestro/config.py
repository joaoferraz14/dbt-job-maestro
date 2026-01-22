"""Configuration options for dbt-job-maestro"""

import os
import yaml
from typing import List, Optional
from dataclasses import dataclass, field


@dataclass
class SelectorConfig:
    """Configuration for selector generation"""

    # Selector generation method: 'fqn', 'path', 'tag', or 'mixed'
    method: str = "fqn"

    # Whether to group models by shared dependencies
    group_by_dependencies: bool = True

    # Tags to exclude from selectors
    exclude_tags: List[str] = field(default_factory=list)

    # Models to exclude from selectors (by name or pattern)
    exclude_models: List[str] = field(default_factory=list)

    # Paths to exclude from selectors
    exclude_paths: List[str] = field(default_factory=list)

    # Specific paths to create path-based selectors for (used in 'mixed' mode)
    # Models in these paths get FIRST PRIORITY - their own dedicated selectors
    # They are then excluded from tag-based and FQN-based grouping to prevent duplicates
    # This allows you to isolate specific model groups (e.g., legacy, critical, experimental)
    # Example: ['models/staging/legacy', 'models/marts/finance', 'models/marts/critical']
    include_path_groups: List[str] = field(default_factory=list)

    # Whether to include source freshness selectors
    include_freshness_selectors: bool = True

    # Whether to include parent sources when a model depends on them
    include_parent_sources: bool = True

    # Custom model prefix order for sorting (optional)
    # Leave empty to use alphabetical sorting
    # Example: ['raw', 'staging', 'marts'] or ['bronze', 'silver', 'gold']
    prefix_order: List[str] = field(default_factory=list)

    # Path grouping level (for path-based selectors)
    # 0 = root level, 1 = first subdirectory, etc.
    path_grouping_level: int = 1

    # Minimum models per selector (selectors with fewer models will be merged)
    min_models_per_selector: int = 1

    # Selector name prefix for auto-generated selectors
    selector_prefix: str = "automatically_generated_selector"

    # Whether to preserve manually created selectors
    preserve_manual_selectors: bool = True


@dataclass
class JobConfig:
    """Configuration for dbt Cloud job generation"""

    # DBT Cloud account ID
    account_id: Optional[int] = None

    # DBT Cloud project ID
    project_id: Optional[int] = None

    # DBT Cloud environment ID
    environment_id: Optional[int] = None

    # DBT version to use (empty string means use environment default)
    dbt_version: str = ""

    # Number of threads for DBT execution
    threads: int = 8

    # Target name for DBT execution
    target_name: str = "prod"

    # Timeout in seconds (0 = no timeout)
    timeout_seconds: int = 0

    # Cron schedule for scheduled jobs
    cron_schedule: str = "0 */6 * * *"

    # Whether to generate docs
    generate_docs: bool = False

    # Whether to run source freshness
    run_generate_sources: bool = False

    # Job name prefix
    job_name_prefix: str = "dbt"


@dataclass
class DeploymentConfig:
    """Configuration for deploying jobs to dbt Cloud"""

    # Branch that triggers job deployment (e.g., 'main', 'production')
    deploy_branch: str = "main"

    # Whether to validate dbt-jobs-as-code is installed
    require_dbt_jobs_as_code: bool = True

    # Path to dbt_project.yml (to check packages.yml)
    dbt_project_path: str = "."


@dataclass
class Config:
    """Main configuration class"""

    selector: SelectorConfig = field(default_factory=SelectorConfig)
    job: JobConfig = field(default_factory=JobConfig)
    deployment: DeploymentConfig = field(default_factory=DeploymentConfig)

    # Path to manifest.json
    manifest_path: str = "target/manifest.json"

    # Output directory for generated files
    output_dir: str = "."

    # Output file name for selectors
    selectors_output_file: str = "selectors.yml"

    # Output file name for jobs
    jobs_output_file: str = "jobs.yml"

    @classmethod
    def from_yaml(cls, yaml_path: str) -> "Config":
        """
        Load configuration from YAML file

        Args:
            yaml_path: Path to configuration YAML file

        Returns:
            Config instance
        """
        if not os.path.exists(yaml_path):
            raise FileNotFoundError(f"Configuration file not found: {yaml_path}")

        with open(yaml_path, "r") as f:
            data = yaml.safe_load(f) or {}

        # Create selector config
        selector_data = data.get("selector", {})
        selector_config = SelectorConfig(
            method=selector_data.get("method", "fqn"),
            group_by_dependencies=selector_data.get("group_by_dependencies", True),
            exclude_tags=selector_data.get("exclude_tags", []),
            exclude_models=selector_data.get("exclude_models", []),
            exclude_paths=selector_data.get("exclude_paths", []),
            include_path_groups=selector_data.get("include_path_groups", []),
            include_freshness_selectors=selector_data.get("include_freshness_selectors", True),
            include_parent_sources=selector_data.get("include_parent_sources", True),
            prefix_order=selector_data.get("prefix_order", []),
            path_grouping_level=selector_data.get("path_grouping_level", 1),
            min_models_per_selector=selector_data.get("min_models_per_selector", 1),
            selector_prefix=selector_data.get("selector_prefix", "automatically_generated_selector"),
            preserve_manual_selectors=selector_data.get("preserve_manual_selectors", True),
        )

        # Create job config
        job_data = data.get("job", {})
        job_config = JobConfig(
            account_id=job_data.get("account_id"),
            project_id=job_data.get("project_id"),
            environment_id=job_data.get("environment_id"),
            dbt_version=job_data.get("dbt_version", ""),
            threads=job_data.get("threads", 8),
            target_name=job_data.get("target_name", "prod"),
            timeout_seconds=job_data.get("timeout_seconds", 0),
            cron_schedule=job_data.get("cron_schedule", "0 */6 * * *"),
            generate_docs=job_data.get("generate_docs", False),
            run_generate_sources=job_data.get("run_generate_sources", False),
            job_name_prefix=job_data.get("job_name_prefix", "dbt"),
        )

        # Create deployment config
        deployment_data = data.get("deployment", {})
        deployment_config = DeploymentConfig(
            deploy_branch=deployment_data.get("deploy_branch", "main"),
            require_dbt_jobs_as_code=deployment_data.get("require_dbt_jobs_as_code", True),
            dbt_project_path=deployment_data.get("dbt_project_path", "."),
        )

        # Create main config
        return cls(
            selector=selector_config,
            job=job_config,
            deployment=deployment_config,
            manifest_path=data.get("manifest_path", "target/manifest.json"),
            output_dir=data.get("output_dir", "."),
            selectors_output_file=data.get("selectors_output_file", "selectors.yml"),
            jobs_output_file=data.get("jobs_output_file", "jobs.yml"),
        )

    def to_yaml(self, yaml_path: str) -> None:
        """
        Save configuration to YAML file

        Args:
            yaml_path: Path to save configuration
        """
        data = {
            "manifest_path": self.manifest_path,
            "output_dir": self.output_dir,
            "selectors_output_file": self.selectors_output_file,
            "jobs_output_file": self.jobs_output_file,
            "selector": {
                "method": self.selector.method,
                "group_by_dependencies": self.selector.group_by_dependencies,
                "exclude_tags": self.selector.exclude_tags,
                "exclude_models": self.selector.exclude_models,
                "exclude_paths": self.selector.exclude_paths,
                "include_path_groups": self.selector.include_path_groups,
                "include_freshness_selectors": self.selector.include_freshness_selectors,
                "include_parent_sources": self.selector.include_parent_sources,
                "prefix_order": self.selector.prefix_order,
                "path_grouping_level": self.selector.path_grouping_level,
                "min_models_per_selector": self.selector.min_models_per_selector,
                "selector_prefix": self.selector.selector_prefix,
                "preserve_manual_selectors": self.selector.preserve_manual_selectors,
            },
            "job": {
                "account_id": self.job.account_id,
                "project_id": self.job.project_id,
                "environment_id": self.job.environment_id,
                "dbt_version": self.job.dbt_version,
                "threads": self.job.threads,
                "target_name": self.job.target_name,
                "timeout_seconds": self.job.timeout_seconds,
                "cron_schedule": self.job.cron_schedule,
                "generate_docs": self.job.generate_docs,
                "run_generate_sources": self.job.run_generate_sources,
                "job_name_prefix": self.job.job_name_prefix,
            },
            "deployment": {
                "deploy_branch": self.deployment.deploy_branch,
                "require_dbt_jobs_as_code": self.deployment.require_dbt_jobs_as_code,
                "dbt_project_path": self.deployment.dbt_project_path,
            },
        }

        os.makedirs(os.path.dirname(yaml_path) or ".", exist_ok=True)
        with open(yaml_path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False, indent=2)
