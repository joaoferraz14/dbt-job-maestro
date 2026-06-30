"""Configuration options for dbt-job-maestro"""

import os
import yaml
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field


@dataclass
class SelectorConfig:
    """Configuration for selector generation"""

    # Whether to group models by shared dependencies
    group_by_dependencies: bool = True

    # Tags to exclude from selectors
    exclude_tags: List[str] = field(default_factory=list)

    # Models to exclude from selectors (by name or pattern)
    exclude_models: List[str] = field(default_factory=list)

    # Paths to exclude from selectors
    exclude_paths: List[str] = field(default_factory=list)

    # How to combine exclusion criteria in the selector definition
    # - 'union': Exclude models matching ANY of the criteria (OR logic) - default
    # - 'intersection': Exclude models matching ALL of the criteria (AND logic)
    # Example with union: exclude models with tag 'alloy' OR in path 'staging'
    # Example with intersection: exclude models with tag 'alloy' AND in path 'staging'
    exclusion_mode: str = "union"

    # Whether to include source freshness selectors (disabled by default)
    # Set to True to generate freshness selectors for all selectors
    # Or use freshness_selector_names to only generate for specific selectors
    include_freshness_selectors: bool = False

    # Specific selector names to generate freshness for (optional)
    # If empty and include_freshness_selectors is True, all selectors get freshness
    # If provided, only these selectors get freshness variants
    # Example: ['maestro_staging', 'maestro_marts']
    freshness_selector_names: List[str] = field(default_factory=list)

    # Selector names to EXCLUDE from freshness generation (optional)
    # If provided, these selectors will NOT get freshness variants even if
    # include_freshness_selectors is True or they're in freshness_selector_names
    # Example: ['maestro_debug', 'manual_testing']
    exclude_freshness_selector_names: List[str] = field(default_factory=list)

    # Whether to include parent sources when a model depends on them
    include_parent_sources: bool = True

    # Custom model prefix order for sorting (optional)
    # Leave empty to use alphabetical sorting
    # Example: ['raw', 'staging', 'marts'] or ['bronze', 'silver', 'gold']
    prefix_order: List[str] = field(default_factory=list)

    # Selector name prefix for auto-generated selectors
    # Selectors starting with "{selector_prefix}_" are auto-generated
    # Selectors NOT starting with this prefix are considered manual (always preserved)
    selector_prefix: str = "maestro"

    # Whether to warn about overlapping manual selectors
    # When True, detects and logs warnings when multiple manual selectors cover the same model
    warn_on_manual_overlaps: bool = True

    # Whether to reformat manual selectors when writing selectors.yml
    # When True (default), manual selectors are re-serialized through yaml.dump
    # for consistent formatting with auto-generated selectors.
    # When False, manual selectors are preserved exactly as written in the original
    # selectors.yml file - indentation, quoting, comments, and line breaks are untouched.
    # Use False if you maintain specific YAML formatting in your manual selectors.
    reformat_manual_selectors: bool = True

    # -------------------------------------------------------------------------
    # SEEDS SELECTOR OPTIONS
    # -------------------------------------------------------------------------

    # Whether to generate selectors for seed files
    include_seeds_selectors: bool = False

    # Method to group seeds: 'path' (by folder) or 'fqn' (individual seeds)
    seeds_selector_method: str = "path"

    # Path to seeds folder (used when seeds_selector_method='path')
    # If empty, auto-detects from manifest
    seeds_path: str = ""

    # -------------------------------------------------------------------------
    # SNAPSHOTS SELECTOR OPTIONS
    # -------------------------------------------------------------------------

    # Whether to generate selectors for snapshot files
    include_snapshots_selectors: bool = False

    # Method to group snapshots: 'path' (by folder) or 'fqn' (individual snapshots)
    snapshots_selector_method: str = "path"

    # Path to snapshots folder (used when snapshots_selector_method='path')
    # If empty, auto-detects from manifest
    snapshots_path: str = ""

    # -------------------------------------------------------------------------
    # FULL REFRESH SELECTOR OPTIONS
    # -------------------------------------------------------------------------

    # Whether to generate a full refresh selector for incremental models
    include_full_refresh_selector: bool = False

    # Tags to exclude from the full refresh selector
    full_refresh_exclude_tags: List[str] = field(default_factory=list)

    # Paths to exclude from the full refresh selector
    full_refresh_exclude_paths: List[str] = field(default_factory=list)

    # Specific models to exclude from the full refresh selector
    full_refresh_exclude_models: List[str] = field(default_factory=list)

    # Indirect selection mode for tests (applies to ALL selectors): eager, cautious, buildable, or empty
    # - eager: include all tests that touch selected models (default)
    # - cautious: only include tests whose parents are all selected
    # - buildable: include tests that can be built with selected models
    # - empty: exclude all tests
    indirect_selection: str = "eager"

    # -------------------------------------------------------------------------
    # SINGLE-MODEL SELECTOR GROUPING
    # -------------------------------------------------------------------------

    # When true, all single-model components (orphan models) are combined into
    # one selector instead of creating individual selectors for each.
    combine_single_model_selectors: bool = False

    # Name suffix for the combined single-model selector.
    # The full name will be: {selector_prefix}_{single_model_selector_name}
    # e.g., maestro_orphan_models
    single_model_selector_name: str = "orphan_models"

    def validate(self) -> None:
        """Validate configuration options for compatibility.

        Raises:
            ValueError: If incompatible options are set
        """
        # Validate exclusion_mode
        valid_exclusion_modes = ["union", "intersection"]
        if self.exclusion_mode not in valid_exclusion_modes:
            raise ValueError(
                f"Invalid exclusion_mode '{self.exclusion_mode}'. "
                f"Must be one of: {', '.join(valid_exclusion_modes)}"
            )

        # Validate seeds_selector_method
        valid_selector_methods = ["path", "fqn"]
        if self.seeds_selector_method not in valid_selector_methods:
            raise ValueError(
                f"Invalid seeds_selector_method '{self.seeds_selector_method}'. "
                f"Must be one of: {', '.join(valid_selector_methods)}"
            )

        # Validate snapshots_selector_method
        if self.snapshots_selector_method not in valid_selector_methods:
            raise ValueError(
                f"Invalid snapshots_selector_method '{self.snapshots_selector_method}'. "
                f"Must be one of: {', '.join(valid_selector_methods)}"
            )


@dataclass
class CustomFullRefreshSchedule:
    """Configuration for a custom full refresh schedule.

    Allows defining specific resources (selectors, tags, paths, or models)
    that need full refresh on a custom schedule.
    """

    # Name for this custom full refresh job
    name: str = ""

    # Cron schedule for this full refresh job (minute hour day_of_month month day_of_week)
    cron_schedule: str = "0 0 * * 0"

    # Selector name to full refresh (mutually exclusive with tags, paths, models)
    selector: str = ""

    # Tags to full refresh (mutually exclusive with selector)
    tags: List[str] = field(default_factory=list)

    # Paths to full refresh (mutually exclusive with selector)
    paths: List[str] = field(default_factory=list)

    # Specific models to full refresh (mutually exclusive with selector)
    models: List[str] = field(default_factory=list)


@dataclass
class SeedsFullRefreshConfig:
    """Configuration for seeds full refresh job.

    Creates a job that runs `dbt seed --full-refresh` to reload all seed data.
    """

    # Enable seeds full refresh job
    enabled: bool = False

    # Cron schedule for the seeds full refresh job (minute hour day_of_month month day_of_week)
    cron_schedule: str = "0 0 * * 0"


@dataclass
class FullRefreshConfig:
    """Configuration for full refresh jobs.

    Supports two modes:
    1. Auto-generated full refresh for all incremental models
    2. Custom full refresh schedules for specific resources

    Note: Exclusions and indirect_selection are configured in SelectorConfig,
    not here. This class only handles job scheduling.
    """

    # Enable auto-generated full refresh job for all incremental models
    enabled: bool = False

    # Cron schedule for the auto-generated full refresh job (minute hour day_of_month month day_of_week)
    cron_schedule: str = "0 0 * * 0"

    # Custom full refresh schedules for specific resources
    # Each entry creates a separate full refresh job with its own schedule
    custom_schedules: List[CustomFullRefreshSchedule] = field(default_factory=list)


@dataclass
class AirflowConfig:
    """Configuration for Airflow DAG generation."""

    # Airflow DAG identifier
    dag_id: str = "dbt_maestro_dag"

    # Cron schedule for the DAG (same format as dbt Cloud)
    schedule_interval: str = "0 6 * * *"

    # DAG start date (YYYY-MM-DD)
    start_date: str = "2024-01-01"

    # Owner shown in Airflow UI
    owner: str = "airflow"

    # Number of task retries on failure
    retries: int = 1

    # Minutes to wait before retrying a failed task
    retry_delay_minutes: int = 5

    # Absolute path to the dbt project root (passed via --project-dir)
    dbt_project_dir: str = ""

    # Absolute path to dbt profiles directory (passed via --profiles-dir)
    dbt_profiles_dir: str = ""

    # dbt target name (e.g. "prod")
    dbt_target: str = "prod"

    # dbt --threads value
    dbt_threads: int = 8

    # Airflow DAG tags shown in the UI
    tags: List[str] = field(default_factory=lambda: ["dbt", "maestro"])

    # Must match selector.selector_prefix so the generator can classify selectors
    selector_prefix: str = "maestro"

    # How to wire task dependencies in the generated DAG:
    # - parallel:    all tasks run concurrently (no dependencies set)
    # - sequential:  each task waits for the previous one (list order)
    # - dependency:  type-based ordering (seeds → snapshots → models) plus
    #                optional manifest-derived cross-selector dependencies
    orchestration_mode: str = "dependency"

    # Output file name for the generated Airflow DAG
    dag_output_file: str = "dbt_maestro_dag.py"


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

    # Job orchestration mode: "simple", "staggered", or "none"
    # - simple: All jobs use the same cron_schedule (parallel execution)
    # - staggered: Jobs staggered with time increments (e.g., 6:00, 6:30, 7:00)
    # - none: No schedule - jobs are created but must be triggered manually
    orchestration_mode: str = "simple"

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

    # Starting hour for first job (0-23) when using staggered mode
    start_hour: int = 6

    # Starting minute for first job (0-59) when using staggered mode
    start_minute: int = 0

    # Time increment in minutes between jobs for staggered mode
    cron_increment_minutes: int = 5

    # Days of week for cron jobs (used in staggered mode)
    # Empty list means every day, or specify: ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"]
    cron_days_of_week: List[str] = field(default_factory=list)

    # Minimum models per job (selectors with fewer models will be combined)
    # When set > 1, selectors with fewer models are combined
    # into a single job that runs multiple selectors (e.g., dbt build --selector A --selector B)
    min_models_per_job: int = 1

    # Execution order for different resource types during job creation
    # Jobs will be created/ordered in this sequence (first in list runs first)
    # Valid values: 'seeds', 'snapshots', 'models'
    # Example: ['seeds', 'snapshots', 'models'] means seeds run first, then snapshots, then models
    # Empty list means no specific ordering (alphabetical by selector name)
    execution_order: List[str] = field(default_factory=list)

    # Full refresh configuration for incremental models
    full_refresh: FullRefreshConfig = field(default_factory=FullRefreshConfig)

    # Seeds full refresh configuration
    seeds_full_refresh: SeedsFullRefreshConfig = field(default_factory=SeedsFullRefreshConfig)


@dataclass
class Config:
    """Main configuration class"""

    selector: SelectorConfig = field(default_factory=SelectorConfig)
    job: JobConfig = field(default_factory=JobConfig)
    airflow: AirflowConfig = field(default_factory=AirflowConfig)

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
            group_by_dependencies=selector_data.get("group_by_dependencies", True),
            exclude_tags=selector_data.get("exclude_tags", []),
            exclude_models=selector_data.get("exclude_models", []),
            exclude_paths=selector_data.get("exclude_paths", []),
            exclusion_mode=selector_data.get("exclusion_mode", "union"),
            include_freshness_selectors=selector_data.get("include_freshness_selectors", False),
            freshness_selector_names=selector_data.get("freshness_selector_names", []),
            exclude_freshness_selector_names=selector_data.get(
                "exclude_freshness_selector_names", []
            ),
            include_parent_sources=selector_data.get("include_parent_sources", True),
            prefix_order=selector_data.get("prefix_order", []),
            selector_prefix=selector_data.get("selector_prefix", "maestro"),
            warn_on_manual_overlaps=selector_data.get("warn_on_manual_overlaps", True),
            reformat_manual_selectors=selector_data.get("reformat_manual_selectors", True),
            include_seeds_selectors=selector_data.get("include_seeds_selectors", False),
            seeds_selector_method=selector_data.get("seeds_selector_method", "path"),
            seeds_path=selector_data.get("seeds_path", ""),
            include_snapshots_selectors=selector_data.get("include_snapshots_selectors", False),
            snapshots_selector_method=selector_data.get("snapshots_selector_method", "path"),
            snapshots_path=selector_data.get("snapshots_path", ""),
            include_full_refresh_selector=selector_data.get("include_full_refresh_selector", False),
            full_refresh_exclude_tags=selector_data.get("full_refresh_exclude_tags", []),
            full_refresh_exclude_paths=selector_data.get("full_refresh_exclude_paths", []),
            full_refresh_exclude_models=selector_data.get("full_refresh_exclude_models", []),
            indirect_selection=selector_data.get("indirect_selection", "eager"),
            combine_single_model_selectors=selector_data.get(
                "combine_single_model_selectors", False
            ),
            single_model_selector_name=selector_data.get(
                "single_model_selector_name", "orphan_models"
            ),
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
            orchestration_mode=cls._normalize_orchestration_mode(
                job_data.get("orchestration_mode", "simple")
            ),
            start_hour=job_data.get("start_hour", 6),
            start_minute=job_data.get("start_minute", 0),
            cron_increment_minutes=job_data.get("cron_increment_minutes", 5),
            cron_days_of_week=job_data.get("cron_days_of_week", []),
            min_models_per_job=job_data.get("min_models_per_job", 1),
            execution_order=job_data.get("execution_order", []),
            include_maestro_selectors_in_jobs=job_data.get(
                "include_maestro_selectors_in_jobs", True
            ),
            include_manual_selectors_in_jobs=job_data.get("include_manual_selectors_in_jobs", True),
            selector_prefix=selector_prefix,
            full_refresh=cls._parse_full_refresh_config(job_data.get("full_refresh", {})),
            seeds_full_refresh=cls._parse_seeds_full_refresh_config(
                job_data.get("seeds_full_refresh", {})
            ),
        )

        # Create airflow config
        airflow_data = data.get("airflow", {})
        airflow_config = AirflowConfig(
            dag_id=airflow_data.get("dag_id", "dbt_maestro_dag"),
            schedule_interval=airflow_data.get("schedule_interval", "0 6 * * *"),
            start_date=airflow_data.get("start_date", "2024-01-01"),
            owner=airflow_data.get("owner", "airflow"),
            retries=airflow_data.get("retries", 1),
            retry_delay_minutes=airflow_data.get("retry_delay_minutes", 5),
            dbt_project_dir=airflow_data.get("dbt_project_dir", ""),
            dbt_profiles_dir=airflow_data.get("dbt_profiles_dir", ""),
            dbt_target=airflow_data.get("dbt_target", "prod"),
            dbt_threads=airflow_data.get("dbt_threads", 8),
            tags=airflow_data.get("tags", ["dbt", "maestro"]),
            selector_prefix=selector_data.get("selector_prefix", "maestro"),
            orchestration_mode=airflow_data.get("orchestration_mode", "dependency"),
            dag_output_file=airflow_data.get("dag_output_file", "dbt_maestro_dag.py"),
        )

        # Create main config
        return cls(
            selector=selector_config,
            job=job_config,
            airflow=airflow_config,
            manifest_path=data.get("manifest_path", "target/manifest.json"),
            output_dir=data.get("output_dir", "."),
            selectors_output_file=data.get("selectors_output_file", "selectors.yml"),
            jobs_output_file=data.get("jobs_output_file", "jobs.yml"),
        )

    @classmethod
    def _normalize_orchestration_mode(cls, mode: str) -> str:
        """Normalize orchestration mode, handling backward-compatible aliases.

        Args:
            mode: Raw orchestration mode string from config

        Returns:
            Normalized mode string (simple, staggered, or none)
        """
        aliases = {
            "cron_incremental": "staggered",
        }
        normalized = aliases.get(mode, mode)
        valid_modes = {"simple", "staggered", "none"}
        if normalized not in valid_modes:
            raise ValueError(
                f"Invalid orchestration_mode '{mode}'. "
                f"Valid options: {', '.join(sorted(valid_modes))}"
            )
        return normalized

    def _format_custom_schedules(self) -> str:
        """Format custom full refresh schedules for YAML output.

        Returns:
            YAML-formatted string of custom schedules
        """
        if not self.job.full_refresh.custom_schedules:
            return "[]"

        lines = []
        for schedule in self.job.full_refresh.custom_schedules:
            schedule_dict = {"name": schedule.name, "cron_schedule": schedule.cron_schedule}
            if schedule.selector:
                schedule_dict["selector"] = schedule.selector
            if schedule.tags:
                schedule_dict["tags"] = schedule.tags
            if schedule.paths:
                schedule_dict["paths"] = schedule.paths
            if schedule.models:
                schedule_dict["models"] = schedule.models
            lines.append(schedule_dict)

        # Format as YAML list
        result = "\n"
        for item in lines:
            first_key = True
            for key, value in item.items():
                if first_key:
                    result += f"      - {key}: "
                    first_key = False
                else:
                    result += f"        {key}: "
                if isinstance(value, list):
                    result += f"{value}\n"
                else:
                    result += f"'{value}'\n" if key == "cron_schedule" else f"{value}\n"
        return result.rstrip()

    @classmethod
    def _parse_full_refresh_config(cls, data: Dict[str, Any]) -> FullRefreshConfig:
        """Parse full refresh configuration from YAML data.

        Args:
            data: Dictionary containing full refresh configuration

        Returns:
            FullRefreshConfig instance
        """
        if not data:
            return FullRefreshConfig()

        # Parse custom schedules
        custom_schedules = []
        for schedule_data in data.get("custom_schedules", []):
            schedule = CustomFullRefreshSchedule(
                name=schedule_data.get("name", ""),
                cron_schedule=schedule_data.get("cron_schedule", "0 0 * * 0"),
                selector=schedule_data.get("selector", ""),
                tags=schedule_data.get("tags", []),
                paths=schedule_data.get("paths", []),
                models=schedule_data.get("models", []),
            )
            custom_schedules.append(schedule)

        return FullRefreshConfig(
            enabled=data.get("enabled", False),
            cron_schedule=data.get("cron_schedule", "0 0 * * 0"),
            custom_schedules=custom_schedules,
        )

    @classmethod
    def _parse_seeds_full_refresh_config(cls, data: Dict[str, Any]) -> SeedsFullRefreshConfig:
        """Parse seeds full refresh configuration from YAML data.

        Args:
            data: Dictionary containing seeds full refresh configuration

        Returns:
            SeedsFullRefreshConfig instance
        """
        if not data:
            return SeedsFullRefreshConfig()

        return SeedsFullRefreshConfig(
            enabled=data.get("enabled", False),
            cron_schedule=data.get("cron_schedule", "0 0 * * 0"),
        )

    def to_yaml(self, yaml_path: str) -> None:
        """
        Save configuration to YAML file with comments

        Args:
            yaml_path: Path to save configuration
        """
        os.makedirs(os.path.dirname(yaml_path) or ".", exist_ok=True)

        config_template = f"""# =============================================================================
# dbt-job-maestro Configuration
# =============================================================================
# Generate selectors: maestro generate --config {os.path.basename(yaml_path)}
# Generate jobs:      maestro generate-jobs --config {os.path.basename(yaml_path)}
# =============================================================================

# -----------------------------------------------------------------------------
# FILE PATHS
# -----------------------------------------------------------------------------
# Path to dbt manifest.json (generated by `dbt compile`)
manifest_path: {self.manifest_path}

# Directory for output files
output_dir: {self.output_dir}

# Output file names
selectors_output_file: {self.selectors_output_file}
jobs_output_file: {self.jobs_output_file}

# -----------------------------------------------------------------------------
# SELECTOR GENERATION
# -----------------------------------------------------------------------------
selector:
  # Group models by shared dependencies
  # When true: models sharing dependencies are grouped together
  # When false: one selector per model
  group_by_dependencies: {str(self.selector.group_by_dependencies).lower()}

  # ---------------------------------------------------------------------------
  # EXCLUSIONS - Models to skip from auto-generation
  # ---------------------------------------------------------------------------
  # Tags to exclude (models with these tags won't be in auto-generated selectors)
  # Example: ['deprecated', 'archived', 'test']
  exclude_tags: {self.selector.exclude_tags}

  # Models to exclude by name
  # Example: ['temp_model', 'debug_model']
  exclude_models: {self.selector.exclude_models}

  # Paths to exclude
  # Example: ['models/staging/legacy', 'models/temp']
  exclude_paths: {self.selector.exclude_paths}

  # How to combine exclusion criteria: 'union' or 'intersection'
  # - union: Exclude if ANY criteria matches (OR logic) - default
  # - intersection: Exclude only if ALL criteria match (AND logic)
  exclusion_mode: {self.selector.exclusion_mode}

  # ---------------------------------------------------------------------------
  # FRESHNESS SELECTORS
  # ---------------------------------------------------------------------------
  # Generate source freshness selectors alongside regular selectors
  include_freshness_selectors: {str(self.selector.include_freshness_selectors).lower()}

  # Whitelist: Only create freshness selectors for these (empty = all)
  # Example: ['maestro_staging', 'maestro_marts']
  freshness_selector_names: {self.selector.freshness_selector_names}

  # Blacklist: Never create freshness selectors for these (takes priority)
  # Example: ['maestro_debug', 'manual_testing']
  exclude_freshness_selector_names: {self.selector.exclude_freshness_selector_names}

  # ---------------------------------------------------------------------------
  # SEEDS SELECTORS
  # ---------------------------------------------------------------------------
  # Generate a selector for seed files
  include_seeds_selectors: {str(self.selector.include_seeds_selectors).lower()}

  # Method to group seeds: 'path' or 'fqn' (both create ONE selector)
  seeds_selector_method: {self.selector.seeds_selector_method}

  # Path to seeds folder (auto-detected if empty)
  seeds_path: '{self.selector.seeds_path}'

  # ---------------------------------------------------------------------------
  # SNAPSHOTS SELECTORS
  # ---------------------------------------------------------------------------
  # Generate a selector for snapshot files
  include_snapshots_selectors: {str(self.selector.include_snapshots_selectors).lower()}

  # Method to group snapshots: 'path' or 'fqn' (both create ONE selector)
  snapshots_selector_method: {self.selector.snapshots_selector_method}

  # Path to snapshots folder (auto-detected if empty)
  snapshots_path: '{self.selector.snapshots_path}'

  # ---------------------------------------------------------------------------
  # FULL REFRESH SELECTOR
  # ---------------------------------------------------------------------------
  # Generate a selector for full refresh of incremental models
  # Uses intersection of fqn:* and config.materialized:incremental
  include_full_refresh_selector: {str(self.selector.include_full_refresh_selector).lower()}

  # Exclusions from the full refresh selector
  # Tags to exclude (incremental models with these tags won't be full refreshed)
  full_refresh_exclude_tags: {self.selector.full_refresh_exclude_tags}

  # Paths to exclude
  full_refresh_exclude_paths: {self.selector.full_refresh_exclude_paths}

  # Specific models to exclude
  full_refresh_exclude_models: {self.selector.full_refresh_exclude_models}

  # ---------------------------------------------------------------------------
  # INDIRECT SELECTION
  # ---------------------------------------------------------------------------
  # Indirect selection mode for tests: eager, cautious, buildable, or empty
  # - eager: include all tests that touch selected models (default)
  # - cautious: only include tests whose parents are all selected
  # - buildable: include tests that can be built with selected models
  # - empty: exclude all tests
  # This applies to ALL generated selectors
  indirect_selection: {self.selector.indirect_selection}

  # ---------------------------------------------------------------------------
  # ADVANCED OPTIONS
  # ---------------------------------------------------------------------------
  # Include parent sources when a model depends on them
  include_parent_sources: {str(self.selector.include_parent_sources).lower()}

  # Custom prefix order for sorting selectors (empty = alphabetical)
  # Example: ['stg', 'int', 'fct', 'dim'] or ['bronze', 'silver', 'gold']
  prefix_order: {self.selector.prefix_order}

  # Prefix for auto-generated selectors (selectors without this prefix are manual)
  selector_prefix: {self.selector.selector_prefix}

  # Warn when multiple manual selectors cover the same model
  warn_on_manual_overlaps: {str(self.selector.warn_on_manual_overlaps).lower()}

  # Reformat manual selectors when writing selectors.yml
  # When true (default): manual selectors are reformatted through yaml.dump for
  #   consistent style with auto-generated selectors
  # When false: manual selectors are preserved exactly as written in the original
  #   selectors.yml (indentation, quoting, comments, line breaks are untouched)
  reformat_manual_selectors: {str(self.selector.reformat_manual_selectors).lower()}

  # ---------------------------------------------------------------------------
  # SINGLE-MODEL SELECTOR GROUPING
  # ---------------------------------------------------------------------------
  # When true, all single-model components (orphan models with no dependency
  # connections to other auto-generated models) are combined into one selector
  # instead of creating individual selectors for each.
  combine_single_model_selectors: {str(self.selector.combine_single_model_selectors).lower()}

  # Name suffix for the combined single-model selector
  # Full name: {{selector_prefix}}_{{single_model_selector_name}}
  single_model_selector_name: {self.selector.single_model_selector_name}

# -----------------------------------------------------------------------------
# JOB GENERATION (for dbt-jobs-as-code)
# -----------------------------------------------------------------------------
job:
  # ---------------------------------------------------------------------------
  # DBT CLOUD CREDENTIALS (required for deployment)
  # ---------------------------------------------------------------------------
  # Get these from dbt Cloud: Account Settings > Projects
  account_id: {self.job.account_id or 'null'}     # Your dbt Cloud account ID
  project_id: {self.job.project_id or 'null'}     # Your dbt Cloud project ID
  environment_id: {self.job.environment_id or 'null'}  # Your production environment ID

  # ---------------------------------------------------------------------------
  # JOB SETTINGS
  # ---------------------------------------------------------------------------
  # dbt version (empty = use environment default)
  dbt_version: '{self.job.dbt_version}'

  # Number of threads for dbt execution
  threads: {self.job.threads}

  # Target name (usually 'prod' or 'production')
  target_name: {self.job.target_name}

  # Job timeout in seconds (0 = no timeout)
  timeout_seconds: {self.job.timeout_seconds}

  # Generate dbt docs after job run
  generate_docs: {str(self.job.generate_docs).lower()}

  # Run source freshness check
  run_generate_sources: {str(self.job.run_generate_sources).lower()}

  # Prefix for job names (job name = {{prefix}}_{{selector_name}})
  job_name_prefix: {self.job.job_name_prefix}

  # ---------------------------------------------------------------------------
  # SELECTOR INCLUSION
  # ---------------------------------------------------------------------------
  # Include auto-generated selectors (maestro_*) in jobs
  include_maestro_selectors_in_jobs: {str(self.job.include_maestro_selectors_in_jobs).lower()}

  # Include manual selectors (non-maestro_*) in jobs
  include_manual_selectors_in_jobs: {str(self.job.include_manual_selectors_in_jobs).lower()}

  # Selector prefix (should match selector.selector_prefix)
  selector_prefix: {self.job.selector_prefix}

  # ---------------------------------------------------------------------------
  # ORCHESTRATION MODE
  # ---------------------------------------------------------------------------
  # Job orchestration: 'simple', 'staggered', or 'none'
  # ('cron_incremental' is accepted as an alias for 'staggered')
  # - simple: All jobs use the same cron_schedule (parallel execution)
  # - staggered: Jobs staggered by cron_increment_minutes from start_hour:start_minute
  # - none: No schedule, jobs exist but must be triggered manually in dbt Cloud
  orchestration_mode: {self.job.orchestration_mode}

  # Cron schedule for simple mode (e.g., "0 */6 * * *" = every 6 hours)
  cron_schedule: {self.job.cron_schedule}

  # Starting hour for first job (0-23) - for staggered mode
  start_hour: {self.job.start_hour}

  # Starting minute for first job (0-59)
  start_minute: {self.job.start_minute}

  # Minutes between jobs (for staggered mode)
  cron_increment_minutes: {self.job.cron_increment_minutes}

  # Days of week for cron (empty = every day)
  # Example: ['MON', 'TUE', 'WED', 'THU', 'FRI']
  cron_days_of_week: {self.job.cron_days_of_week}

  # ---------------------------------------------------------------------------
  # ADVANCED JOB OPTIONS
  # ---------------------------------------------------------------------------
  # Minimum models per job - only applies to auto-generated (maestro) selectors.
  # Manual selectors always get their own individual job regardless of this setting.
  # Maestro selectors with fewer than this number of models are combined into one
  # job, with a separate dbt build step per selector.
  # Does NOT apply to seeds, snapshots, or full refresh jobs.
  # Set to 1 to disable combining.
  min_models_per_job: {self.job.min_models_per_job}

  # ---------------------------------------------------------------------------
  # EXECUTION ORDER
  # ---------------------------------------------------------------------------
  # Execution order for different resource types during job creation
  # Jobs will be created/ordered in this sequence (first in list runs first)
  # Valid values: 'seeds', 'snapshots', 'models'
  # Example: ['seeds', 'snapshots', 'models'] = seeds run first, then snapshots, then models
  # Empty list = no specific ordering (alphabetical by selector name)
  execution_order: {self.job.execution_order}

  # ---------------------------------------------------------------------------
  # FULL REFRESH JOBS
  # ---------------------------------------------------------------------------
  # Configuration for full refresh jobs (for incremental models)
  full_refresh:
    # Enable auto-generated full refresh job for all incremental models
    enabled: {str(self.job.full_refresh.enabled).lower()}

    # Cron schedule for the auto-generated full refresh job
    # Format: minute hour day_of_month month day_of_week
    # Example: "0 0 * * 0" = every Sunday at midnight
    cron_schedule: "{self.job.full_refresh.cron_schedule}"

    # Custom full refresh schedules for specific resources
    # Each entry creates a separate full refresh job with its own schedule
    # Cron format: minute hour day_of_month month day_of_week (0=Sunday, 6=Saturday)
    # Example:
    # custom_schedules:
    #   - name: weekly_customer_refresh
    #     cron_schedule: "0 0 * * 0"       # Every Sunday at midnight
    #     selector: maestro_customers      # Full refresh a specific selector
    #   - name: monthly_orders_refresh
    #     cron_schedule: "0 0 1 * *"       # First day of month at midnight
    #     tags: ['orders', 'billing']      # Full refresh models with these tags
    #   - name: daily_inventory_refresh
    #     cron_schedule: "0 3 * * *"       # Every day at 3:00 AM
    #     paths: ['models/staging/inventory']  # Full refresh models in these paths
    #   - name: specific_models_refresh
    #     cron_schedule: "0 6 * * 6"       # Every Saturday at 6:00 AM
    #     models: ['dim_product', 'fct_inventory']  # Full refresh specific models
    custom_schedules: {self._format_custom_schedules()}

  # ---------------------------------------------------------------------------
  # SEEDS FULL REFRESH JOB
  # ---------------------------------------------------------------------------
  # Configuration for seeds full refresh job (runs `dbt seed --full-refresh`)
  seeds_full_refresh:
    # Enable seeds full refresh job
    enabled: {str(self.job.seeds_full_refresh.enabled).lower()}

    # Cron schedule for the seeds full refresh job
    # Format: minute hour day_of_month month day_of_week
    # Example: "0 0 * * 0" = every Sunday at midnight
    cron_schedule: "{self.job.seeds_full_refresh.cron_schedule}"

# -----------------------------------------------------------------------------
# AIRFLOW DAG GENERATION
# -----------------------------------------------------------------------------
airflow:
  # Airflow DAG identifier (must be unique within your Airflow instance)
  dag_id: {self.airflow.dag_id}

  # Cron schedule for the DAG (same format as dbt Cloud job schedules)
  schedule_interval: "{self.airflow.schedule_interval}"

  # DAG start date in YYYY-MM-DD format
  start_date: "{self.airflow.start_date}"

  # Owner displayed in the Airflow UI
  owner: {self.airflow.owner}

  # Number of retries on task failure
  retries: {self.airflow.retries}

  # Minutes to wait between retry attempts
  retry_delay_minutes: {self.airflow.retry_delay_minutes}

  # ---------------------------------------------------------------------------
  # DBT RUNTIME PATHS
  # ---------------------------------------------------------------------------
  # Absolute path to your dbt project root (passed as --project-dir)
  # Leave empty to rely on Airflow worker's working directory
  dbt_project_dir: '{self.airflow.dbt_project_dir}'

  # Absolute path to dbt profiles directory (passed as --profiles-dir)
  # Leave empty to use the default ~/.dbt location
  dbt_profiles_dir: '{self.airflow.dbt_profiles_dir}'

  # dbt target name (e.g. "prod", "production")
  dbt_target: {self.airflow.dbt_target}

  # Number of threads passed to dbt via --threads
  dbt_threads: {self.airflow.dbt_threads}

  # ---------------------------------------------------------------------------
  # AIRFLOW UI OPTIONS
  # ---------------------------------------------------------------------------
  # Tags shown in the Airflow DAG list view
  tags: {self.airflow.tags}

  # ---------------------------------------------------------------------------
  # ORCHESTRATION
  # ---------------------------------------------------------------------------
  # How to wire task dependencies in the generated DAG:
  # - parallel:    all tasks run concurrently (no >> dependencies set)
  # - sequential:  each task waits for the previous one (list order)
  # - dependency:  seeds → snapshots → models ordering, plus any
  #                cross-selector model dependencies found in manifest.json
  orchestration_mode: {self.airflow.orchestration_mode}

  # Output file name for the generated Airflow DAG Python file
  dag_output_file: {self.airflow.dag_output_file}
"""

        with open(yaml_path, "w") as f:
            f.write(config_template)
