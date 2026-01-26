"""Configuration options for dbt-job-maestro"""

import os
import yaml
from typing import Dict, List, Optional
from dataclasses import dataclass, field


@dataclass
class SelectorConfig:
    """Configuration for selector generation"""

    # Selector generation method: 'fqn', 'path', or 'tag'
    # - fqn: Group models by dependencies (allows group_by_dependencies)
    # - path: One selector per path (no dependency grouping)
    # - tag: One selector per tag (no dependency grouping)
    method: str = "fqn"

    # Whether to group models by shared dependencies (only valid for 'fqn' method)
    group_by_dependencies: bool = True

    # Tags to exclude from selectors
    exclude_tags: List[str] = field(default_factory=list)

    # Models to exclude from selectors (by name or pattern)
    exclude_models: List[str] = field(default_factory=list)

    # Paths to exclude from selectors
    exclude_paths: List[str] = field(default_factory=list)

    # Whether to include source freshness selectors (disabled by default)
    # Set to True to generate freshness selectors for all selectors
    # Or use freshness_selector_names to only generate for specific selectors
    include_freshness_selectors: bool = False

    # Specific selector names to generate freshness for (optional)
    # If empty and include_freshness_selectors is True, all selectors get freshness
    # If provided, only these selectors get freshness variants
    # Example: ['selector_critical', 'automatically_generated_selector_staging']
    freshness_selector_names: List[str] = field(default_factory=list)

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
    # Note: Not allowed when method='fqn' and group_by_dependencies=True
    min_models_per_selector: int = 1

    # Selector name prefix for auto-generated selectors
    # Selectors starting with "{selector_prefix}_" are auto-generated
    # Selectors NOT starting with this prefix are considered manual (always preserved)
    selector_prefix: str = "maestro"

    # Whether to warn about overlapping manual selectors
    # When True, detects and logs warnings when multiple manual selectors cover the same model
    warn_on_manual_overlaps: bool = True

    def validate(self) -> None:
        """Validate configuration options for compatibility.

        Raises:
            ValueError: If incompatible options are set
        """
        # Validate method
        valid_methods = ["fqn", "path", "tag"]
        if self.method not in valid_methods:
            raise ValueError(
                f"Invalid method '{self.method}'. Must be one of: {', '.join(valid_methods)}"
            )

        # group_by_dependencies is only allowed for 'fqn' method
        if self.method in ["path", "tag"] and self.group_by_dependencies:
            raise ValueError(
                f"group_by_dependencies is not allowed with method='{self.method}'. "
                f"Only the 'fqn' method supports dependency grouping."
            )

        # min_models_per_selector conflicts with group_by_dependencies in fqn mode
        if self.method == "fqn" and self.group_by_dependencies and self.min_models_per_selector > 1:
            raise ValueError(
                "min_models_per_selector > 1 conflicts with group_by_dependencies=True. "
                "When grouping by dependencies, all connected models are grouped together "
                "regardless of count. Set group_by_dependencies=False to use min_models_per_selector."
            )


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

    # Cron schedule for scheduled jobs (used when orchestration_mode is "simple")
    cron_schedule: str = "0 */6 * * *"

    # Whether to generate docs
    generate_docs: bool = False

    # Whether to run source freshness
    run_generate_sources: bool = False

    # Job name prefix
    job_name_prefix: str = "dbt"

    # Job orchestration mode: "simple", "cron_incremental", or "cascade"
    # - simple: All jobs use the same cron_schedule
    # - cron_incremental: Stagger jobs with time increments (e.g., 6:00, 6:05, 6:10)
    # - cascade: Chain jobs so each triggers after the previous completes
    orchestration_mode: str = "simple"

    # For cascade mode: use two-phase deployment
    # Phase 1: Set to True to generate jobs without cascade triggers (for initial deployment)
    # Phase 2: Set to False to generate jobs with cascade triggers using job_id_mapping
    cascade_initial_deployment: bool = True

    # Mapping of job names to dbt Cloud job IDs (for cascade mode phase 2)
    # After deploying jobs, populate this with actual job IDs from dbt Cloud API
    # Example: {"dbt_revenue_critical": 12345, "dbt_customer_analytics": 12346}
    job_id_mapping: Dict[str, int] = field(default_factory=dict)

    # Whether to automatically create jobs for maestro_ selectors (auto-generated)
    # When True: maestro_ selectors automatically become dbt Cloud jobs
    # When False: teams manage job creation manually in dbt Cloud
    include_maestro_selectors_in_jobs: bool = True

    # Whether to create jobs for manual selectors (selectors without maestro_ prefix)
    # When True: manual selectors are included in jobs.yml
    # When False: manual selectors are ignored (must be created manually in dbt Cloud)
    include_manual_selectors_in_jobs: bool = True

    # Selector prefix for identifying auto-generated selectors
    # Must match selector.selector_prefix for consistent behavior
    # Selectors starting with "{selector_prefix}_" are auto-generated
    # Selectors NOT starting with this prefix are manual
    selector_prefix: str = "maestro"

    # Starting hour for first job (0-23) when using cron_incremental or cascade modes
    start_hour: int = 6

    # Starting minute for first job (0-59) when using cron_incremental or cascade modes
    start_minute: int = 0

    # Time increment in minutes between jobs for cron_incremental mode
    cron_increment_minutes: int = 5

    # Days of week for cron jobs (used in cron_incremental mode)
    # Empty list means every day, or specify: ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"]
    cron_days_of_week: List[str] = field(default_factory=list)


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
        method = selector_data.get("method", "fqn")
        # Default group_by_dependencies to False for path/tag methods
        default_group_by = method == "fqn"
        selector_config = SelectorConfig(
            method=method,
            group_by_dependencies=selector_data.get("group_by_dependencies", default_group_by),
            exclude_tags=selector_data.get("exclude_tags", []),
            exclude_models=selector_data.get("exclude_models", []),
            exclude_paths=selector_data.get("exclude_paths", []),
            include_freshness_selectors=selector_data.get("include_freshness_selectors", False),
            freshness_selector_names=selector_data.get("freshness_selector_names", []),
            include_parent_sources=selector_data.get("include_parent_sources", True),
            prefix_order=selector_data.get("prefix_order", []),
            path_grouping_level=selector_data.get("path_grouping_level", 1),
            min_models_per_selector=selector_data.get("min_models_per_selector", 1),
            selector_prefix=selector_data.get("selector_prefix", "maestro"),
            warn_on_manual_overlaps=selector_data.get("warn_on_manual_overlaps", True),
        )

        # Validate selector config
        selector_config.validate()

        # Create job config
        job_data = data.get("job", {})
        # Use selector prefix from selector config, fallback to job config, then default
        selector_prefix = selector_data.get(
            "selector_prefix", job_data.get("selector_prefix", "maestro")
        )
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
            orchestration_mode=job_data.get("orchestration_mode", "simple"),
            start_hour=job_data.get("start_hour", 6),
            start_minute=job_data.get("start_minute", 0),
            cron_increment_minutes=job_data.get("cron_increment_minutes", 5),
            cron_days_of_week=job_data.get("cron_days_of_week", []),
            cascade_initial_deployment=job_data.get("cascade_initial_deployment", True),
            job_id_mapping=job_data.get("job_id_mapping", {}),
            include_maestro_selectors_in_jobs=job_data.get(
                "include_maestro_selectors_in_jobs", True
            ),
            include_manual_selectors_in_jobs=job_data.get("include_manual_selectors_in_jobs", True),
            selector_prefix=selector_prefix,
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
                "include_freshness_selectors": self.selector.include_freshness_selectors,
                "freshness_selector_names": self.selector.freshness_selector_names,
                "include_parent_sources": self.selector.include_parent_sources,
                "prefix_order": self.selector.prefix_order,
                "path_grouping_level": self.selector.path_grouping_level,
                "min_models_per_selector": self.selector.min_models_per_selector,
                "selector_prefix": self.selector.selector_prefix,
                "warn_on_manual_overlaps": self.selector.warn_on_manual_overlaps,
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
                "orchestration_mode": self.job.orchestration_mode,
                "start_hour": self.job.start_hour,
                "start_minute": self.job.start_minute,
                "cron_increment_minutes": self.job.cron_increment_minutes,
                "cron_days_of_week": self.job.cron_days_of_week,
                "cascade_initial_deployment": self.job.cascade_initial_deployment,
                "job_id_mapping": self.job.job_id_mapping,
                "include_maestro_selectors_in_jobs": self.job.include_maestro_selectors_in_jobs,
                "include_manual_selectors_in_jobs": self.job.include_manual_selectors_in_jobs,
                "selector_prefix": self.job.selector_prefix,
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
