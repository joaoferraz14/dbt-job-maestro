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

    # Path grouping level (for path-based selectors)
    # 0 = root level, 1 = first subdirectory, etc.
    path_grouping_level: int = 1

    # Selector name prefix for auto-generated selectors
    # Selectors starting with "{selector_prefix}_" are auto-generated
    # Selectors NOT starting with this prefix are considered manual (always preserved)
    selector_prefix: str = "maestro"

    # Whether to warn about overlapping manual selectors
    # When True, detects and logs warnings when multiple manual selectors cover the same model
    warn_on_manual_overlaps: bool = True

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

    def validate(self) -> None:
        """Validate configuration options for compatibility.

        Raises:
            ValueError: If incompatible options are set
        """
        # Validate method is a single string, not a list
        if not isinstance(self.method, str):
            raise ValueError(
                f"method must be a single string value (e.g., 'fqn'), "
                f"got {type(self.method).__name__}: {self.method}. "
                f"Only one method can be used at a time."
            )

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

    # Minimum models per job for FQN selectors (selectors with fewer models will be combined)
    # Only works with method='fqn'. When set > 1, selectors with fewer models are combined
    # into a single job that runs multiple selectors (e.g., dbt build --selector A --selector B)
    min_models_per_job: int = 1


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
            exclusion_mode=selector_data.get("exclusion_mode", "union"),
            include_freshness_selectors=selector_data.get("include_freshness_selectors", False),
            freshness_selector_names=selector_data.get("freshness_selector_names", []),
            exclude_freshness_selector_names=selector_data.get(
                "exclude_freshness_selector_names", []
            ),
            include_parent_sources=selector_data.get("include_parent_sources", True),
            prefix_order=selector_data.get("prefix_order", []),
            path_grouping_level=selector_data.get("path_grouping_level", 1),
            selector_prefix=selector_data.get("selector_prefix", "maestro"),
            warn_on_manual_overlaps=selector_data.get("warn_on_manual_overlaps", True),
            include_seeds_selectors=selector_data.get("include_seeds_selectors", False),
            seeds_selector_method=selector_data.get("seeds_selector_method", "path"),
            seeds_path=selector_data.get("seeds_path", ""),
            include_snapshots_selectors=selector_data.get("include_snapshots_selectors", False),
            snapshots_selector_method=selector_data.get("snapshots_selector_method", "path"),
            snapshots_path=selector_data.get("snapshots_path", ""),
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
            min_models_per_job=job_data.get("min_models_per_job", 1),
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
        Save configuration to YAML file with comments

        Args:
            yaml_path: Path to save configuration
        """
        os.makedirs(os.path.dirname(yaml_path) or ".", exist_ok=True)

        config_template = f"""# =============================================================================
# dbt-job-maestro Configuration
# =============================================================================
# Generate selectors and jobs: maestro generate --config {os.path.basename(yaml_path)}
# Generate jobs only: maestro generate-jobs --config {os.path.basename(yaml_path)}
# Check deployment: maestro check --config {os.path.basename(yaml_path)}
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
  # Generation method: 'fqn', 'path', or 'tag'
  # - fqn: Groups models by dependencies (recommended for most projects)
  # - path: One selector per directory
  # - tag: One selector per dbt tag
  method: {self.selector.method}

  # Group models by shared dependencies (only for method=fqn)
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
  # ADVANCED OPTIONS
  # ---------------------------------------------------------------------------
  # Include parent sources when a model depends on them
  include_parent_sources: {str(self.selector.include_parent_sources).lower()}

  # Custom prefix order for sorting selectors (empty = alphabetical)
  # Example: ['stg', 'int', 'fct', 'dim'] or ['bronze', 'silver', 'gold']
  prefix_order: {self.selector.prefix_order}

  # Directory level for path grouping (0=root, 1=first subdirectory, etc.)
  path_grouping_level: {self.selector.path_grouping_level}

  # Prefix for auto-generated selectors (selectors without this prefix are manual)
  selector_prefix: {self.selector.selector_prefix}

  # Warn when multiple manual selectors cover the same model
  warn_on_manual_overlaps: {str(self.selector.warn_on_manual_overlaps).lower()}

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
  # Job orchestration: 'simple', 'cron_incremental', or 'cascade'
  # - simple: All jobs use the same cron_schedule
  # - cron_incremental: Jobs staggered by cron_increment_minutes
  # - cascade: Jobs trigger after previous job completes
  orchestration_mode: {self.job.orchestration_mode}

  # Cron schedule for simple mode (e.g., "0 */6 * * *" = every 6 hours)
  cron_schedule: {self.job.cron_schedule}

  # Starting hour for first job (0-23) - for cron_incremental/cascade modes
  start_hour: {self.job.start_hour}

  # Starting minute for first job (0-59)
  start_minute: {self.job.start_minute}

  # Minutes between jobs (for cron_incremental mode)
  cron_increment_minutes: {self.job.cron_increment_minutes}

  # Days of week for cron (empty = every day)
  # Example: ['MON', 'TUE', 'WED', 'THU', 'FRI']
  cron_days_of_week: {self.job.cron_days_of_week}

  # ---------------------------------------------------------------------------
  # ADVANCED JOB OPTIONS
  # ---------------------------------------------------------------------------
  # Minimum models per job (smaller selectors are combined into one job)
  # Only works with method=fqn. Set to 1 to disable combining.
  min_models_per_job: {self.job.min_models_per_job}

  # Cascade mode: initial deployment (true = use cron, false = use triggers)
  # Phase 1: Set true, deploy, get job IDs from dbt Cloud
  # Phase 2: Set false, populate job_id_mapping, redeploy
  cascade_initial_deployment: {str(self.job.cascade_initial_deployment).lower()}

  # Job ID mapping for cascade mode phase 2
  # Example: {{"dbt_maestro_staging": 12345, "dbt_maestro_marts": 12346}}
  job_id_mapping: {self.job.job_id_mapping or '{}'}

# -----------------------------------------------------------------------------
# DEPLOYMENT
# -----------------------------------------------------------------------------
deployment:
  # Git branch that triggers deployment (e.g., 'main', 'production')
  deploy_branch: {self.deployment.deploy_branch}

  # Validate dbt-jobs-as-code is installed before deployment
  require_dbt_jobs_as_code: {str(self.deployment.require_dbt_jobs_as_code).lower()}

  # Path to dbt project (for checking packages.yml)
  dbt_project_path: {self.deployment.dbt_project_path}
"""

        with open(yaml_path, "w") as f:
            f.write(config_template)
