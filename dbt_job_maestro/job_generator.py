"""
Generate dbt Cloud job definitions from selectors

This module generates jobs.yml file that can be deployed using dbt-jobs-as-code package.
See: https://github.com/dbt-labs/dbt-jobs-as-code
"""

import os
import yaml
from typing import Any, Dict, List

from dbt_job_maestro.config import JobConfig


class JobGenerator:
    """Generate dbt Cloud job definitions from selectors"""

    def __init__(self, config: JobConfig):
        """
        Initialize job generator

        Args:
            config: JobConfig instance
        """
        self.config = config

    def generate_jobs(
        self, selectors: List[Dict[str, Any]], existing_jobs: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """
        Generate jobs from selectors

        Args:
            selectors: List of selector definitions
            existing_jobs: Existing jobs to preserve (optional)

        Returns:
            Dictionary of job definitions compatible with dbt-jobs-as-code
        """
        if existing_jobs is None:
            existing_jobs = {}

        jobs = existing_jobs.get("jobs", {}).copy()

        # Filter out freshness selectors
        non_freshness_selectors = [
            s
            for s in selectors
            if not s["name"].startswith("freshness_")
            and not s["name"].startswith("automatically_generated_freshness_")
        ]

        # Filter based on maestro/manual selector inclusion settings
        filtered_selectors = []
        auto_prefix = f"{self.config.selector_prefix}_"
        for selector in non_freshness_selectors:
            selector_name = selector["name"]
            is_auto_generated = selector_name.startswith(auto_prefix)

            # Skip auto-generated selectors if they're excluded
            if is_auto_generated and not self.config.include_maestro_selectors_in_jobs:
                continue

            # Skip manual selectors if they're excluded
            if not is_auto_generated and not self.config.include_manual_selectors_in_jobs:
                continue

            filtered_selectors.append(selector)

        # Separate selectors into large and small based on min_models_per_job
        # Only applies to FQN selectors (those with _model_count metadata)
        large_selectors = []
        small_selectors = []

        if self.config.min_models_per_job > 1:
            for selector in filtered_selectors:
                model_count = selector.get("_model_count")
                if model_count is not None and model_count < self.config.min_models_per_job:
                    small_selectors.append(selector)
                else:
                    large_selectors.append(selector)
        else:
            large_selectors = filtered_selectors

        # Create individual jobs for large selectors
        for idx, selector in enumerate(large_selectors):
            selector_name = selector["name"]

            # Generate job name
            job_name = self._generate_job_name(selector_name)

            # Check if job is manually created (should not be overwritten)
            if job_name in jobs and self._is_manually_created(jobs[job_name]):
                continue

            # Create job definition with orchestration
            job = self._create_job_definition(
                selector_name,
                job_index=idx,
                total_jobs=len(large_selectors) + (1 if small_selectors else 0),
                previous_job_name=(
                    self._generate_job_name(large_selectors[idx - 1]["name"]) if idx > 0 else None
                ),
            )
            jobs[job_name] = job

        # Create combined job for small selectors
        if small_selectors:
            combined_job_name = f"{self.config.job_name_prefix}_combined_small_selectors"

            # Check if job is manually created (should not be overwritten)
            if combined_job_name not in jobs or not self._is_manually_created(
                jobs[combined_job_name]
            ):
                selector_names = [s["name"] for s in small_selectors]
                job = self._create_combined_job_definition(
                    selector_names,
                    job_index=len(large_selectors),
                    total_jobs=len(large_selectors) + 1,
                    previous_job_name=(
                        self._generate_job_name(large_selectors[-1]["name"])
                        if large_selectors
                        else None
                    ),
                )
                jobs[combined_job_name] = job

        return {"jobs": jobs}

    def _generate_job_name(self, selector_name: str) -> str:
        """
        Generate job name from selector name

        Args:
            selector_name: Name of the selector

        Returns:
            Job name
        """
        # Remove prefixes used in selector names
        name = selector_name
        for prefix in [
            "automatically_generated_selector_",
            "tag_",
            "path_",
            "model_",
            "selector_",
        ]:
            if name.startswith(prefix):
                name = name[len(prefix) :]
                break

        return f"{self.config.job_name_prefix}_{name}"

    def _create_job_definition(
        self,
        selector_name: str,
        job_index: int = 0,
        total_jobs: int = 1,
        previous_job_name: str = None,
    ) -> Dict[str, Any]:
        """
        Create job definition for a selector

        Args:
            selector_name: Name of the selector
            job_index: Index of this job in the list (for cron_incremental)
            total_jobs: Total number of jobs being created
            previous_job_name: Name of the previous job (for cascade mode)

        Returns:
            Job definition dictionary compatible with dbt-jobs-as-code
        """
        job_dbt_name = f"{self.config.job_name_prefix}-{selector_name}"

        # Determine orchestration based on mode
        if self.config.orchestration_mode == "cron_incremental":
            cron_schedule = self._generate_incremental_cron(job_index)
            triggers = {
                "git_provider_webhook": False,
                "github_webhook": False,
                "schedule": True,
            }
            schedule = {"cron": cron_schedule}
        elif self.config.orchestration_mode == "cascade":
            if job_index == 0:
                # First job is always scheduled
                cron_schedule = self._generate_start_time_cron()
                triggers = {
                    "git_provider_webhook": False,
                    "github_webhook": False,
                    "schedule": True,
                }
                schedule = {"cron": cron_schedule}
            else:
                # Subsequent jobs trigger on completion of previous
                if self.config.cascade_initial_deployment:
                    # PHASE 1: Initial deployment - all jobs are scheduled
                    # (Need job IDs first, so temporarily use cron schedule)
                    cron_schedule = self._generate_start_time_cron()
                    triggers = {
                        "git_provider_webhook": False,
                        "github_webhook": False,
                        "schedule": True,
                    }
                    schedule = {"cron": cron_schedule}
                else:
                    # PHASE 2: Update with cascade triggers using job IDs
                    previous_job_id = self.config.job_id_mapping.get(previous_job_name)
                    if previous_job_id is None:
                        raise ValueError(
                            f"Job ID not found for '{previous_job_name}' in job_id_mapping. "
                            f"Make sure to populate job_id_mapping after initial deployment."
                        )
                    triggers = {
                        "git_provider_webhook": False,
                        "github_webhook": False,
                        "schedule": False,
                        "on_job_completion": {
                            "job_id": previous_job_id,
                            "statuses": ["success", "error", "cancelled"],
                        },
                    }
                    schedule = None
        else:  # simple mode (default)
            triggers = {
                "git_provider_webhook": False,
                "github_webhook": False,
                "schedule": True,
            }
            schedule = {"cron": self.config.cron_schedule}

        # Build job definition matching dbt-jobs-as-code schema
        job = {
            "account_id": self.config.account_id,
            "dbt_version": self.config.dbt_version if self.config.dbt_version else None,
            "deferring_job_definition_id": None,
            "environment_id": self.config.environment_id,
            "execute_steps": [f"dbt build --selector {selector_name}"],
            "execution": {
                "timeout_seconds": self.config.timeout_seconds,
            },
            "generate_docs": self.config.generate_docs,
            "name": job_dbt_name,
            "project_id": self.config.project_id,
            "run_generate_sources": self.config.run_generate_sources,
            "settings": {
                "target_name": self.config.target_name,
                "threads": self.config.threads,
            },
            "state": 1,
            "triggers": triggers,
        }

        # Add schedule if applicable
        if schedule:
            job["schedule"] = schedule

        return job

    def _create_combined_job_definition(
        self,
        selector_names: List[str],
        job_index: int = 0,
        total_jobs: int = 1,
        previous_job_name: str = None,
    ) -> Dict[str, Any]:
        """
        Create job definition for multiple selectors combined into one job.

        This is used when min_models_per_job is set and selectors have fewer
        models than the threshold.

        Args:
            selector_names: List of selector names to include in the job
            job_index: Index of this job in the list (for cron_incremental)
            total_jobs: Total number of jobs being created
            previous_job_name: Name of the previous job (for cascade mode)

        Returns:
            Job definition dictionary compatible with dbt-jobs-as-code
        """
        job_dbt_name = f"{self.config.job_name_prefix}-combined_small_selectors"

        # Determine orchestration based on mode
        if self.config.orchestration_mode == "cron_incremental":
            cron_schedule = self._generate_incremental_cron(job_index)
            triggers = {
                "git_provider_webhook": False,
                "github_webhook": False,
                "schedule": True,
            }
            schedule = {"cron": cron_schedule}
        elif self.config.orchestration_mode == "cascade":
            if job_index == 0:
                # First job is always scheduled
                cron_schedule = self._generate_start_time_cron()
                triggers = {
                    "git_provider_webhook": False,
                    "github_webhook": False,
                    "schedule": True,
                }
                schedule = {"cron": cron_schedule}
            else:
                # Subsequent jobs trigger on completion of previous
                if self.config.cascade_initial_deployment:
                    # PHASE 1: Initial deployment - all jobs are scheduled
                    cron_schedule = self._generate_start_time_cron()
                    triggers = {
                        "git_provider_webhook": False,
                        "github_webhook": False,
                        "schedule": True,
                    }
                    schedule = {"cron": cron_schedule}
                else:
                    # PHASE 2: Update with cascade triggers using job IDs
                    previous_job_id = self.config.job_id_mapping.get(previous_job_name)
                    if previous_job_id is None:
                        raise ValueError(
                            f"Job ID not found for '{previous_job_name}' in job_id_mapping. "
                            f"Make sure to populate job_id_mapping after initial deployment."
                        )
                    triggers = {
                        "git_provider_webhook": False,
                        "github_webhook": False,
                        "schedule": False,
                        "on_job_completion": {
                            "job_id": previous_job_id,
                            "statuses": ["success", "error", "cancelled"],
                        },
                    }
                    schedule = None
        else:  # simple mode (default)
            triggers = {
                "git_provider_webhook": False,
                "github_webhook": False,
                "schedule": True,
            }
            schedule = {"cron": self.config.cron_schedule}

        # Build execute_steps with multiple --selector flags
        selector_args = " ".join([f"--selector {name}" for name in selector_names])
        execute_steps = [f"dbt build {selector_args}"]

        # Build job definition matching dbt-jobs-as-code schema
        job = {
            "account_id": self.config.account_id,
            "dbt_version": self.config.dbt_version if self.config.dbt_version else None,
            "deferring_job_definition_id": None,
            "environment_id": self.config.environment_id,
            "execute_steps": execute_steps,
            "execution": {
                "timeout_seconds": self.config.timeout_seconds,
            },
            "generate_docs": self.config.generate_docs,
            "name": job_dbt_name,
            "project_id": self.config.project_id,
            "run_generate_sources": self.config.run_generate_sources,
            "settings": {
                "target_name": self.config.target_name,
                "threads": self.config.threads,
            },
            "state": 1,
            "triggers": triggers,
        }

        # Add schedule if applicable
        if schedule:
            job["schedule"] = schedule

        return job

    def _is_manually_created(self, job: Dict[str, Any]) -> bool:
        """
        Check if a job was manually created

        Args:
            job: Job definition

        Returns:
            True if manually created, False otherwise
        """
        description = job.get("description", "")
        return description.startswith("manually_created")

    def _generate_incremental_cron(self, job_index: int) -> str:
        """
        Generate incremental cron schedule for a job.

        Args:
            job_index: Index of the job (0-based)

        Returns:
            Cron schedule string (e.g., "5 6 * * *" for 6:05 AM every day)
        """
        # Calculate total minutes from start time
        total_minutes = (
            self.config.start_hour * 60
            + self.config.start_minute
            + (job_index * self.config.cron_increment_minutes)
        )

        # Handle wrap-around for 24-hour clock
        hour = (total_minutes // 60) % 24
        minute = total_minutes % 60

        # Build day of week part
        if self.config.cron_days_of_week:
            # Map day names to cron values (0=Sunday, 1=Monday, etc.)
            day_mapping = {
                "SUN": "0",
                "MON": "1",
                "TUE": "2",
                "WED": "3",
                "THU": "4",
                "FRI": "5",
                "SAT": "6",
            }
            days = ",".join(
                [day_mapping.get(day.upper(), "*") for day in self.config.cron_days_of_week]
            )
        else:
            days = "*"

        # Cron format: minute hour day_of_month month day_of_week
        return f"{minute} {hour} * * {days}"

    def _generate_start_time_cron(self) -> str:
        """
        Generate cron schedule for the start time (first job in cascade).

        Returns:
            Cron schedule string
        """
        # Build day of week part
        if self.config.cron_days_of_week:
            day_mapping = {
                "SUN": "0",
                "MON": "1",
                "TUE": "2",
                "WED": "3",
                "THU": "4",
                "FRI": "5",
                "SAT": "6",
            }
            days = ",".join(
                [day_mapping.get(day.upper(), "*") for day in self.config.cron_days_of_week]
            )
        else:
            days = "*"

        return f"{self.config.start_minute} {self.config.start_hour} * * {days}"

    def write_jobs(self, jobs: Dict[str, Any], output_path: str) -> None:
        """
        Write jobs to YAML file

        Args:
            jobs: Dictionary of job definitions
            output_path: Path to output file
        """
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

        with open(output_path, "w") as outfile:
            yaml.dump(jobs, outfile, default_flow_style=False, sort_keys=False, indent=2)

    def read_existing_jobs(self, file_path: str) -> Dict[str, Any]:
        """
        Read existing jobs from file

        Args:
            file_path: Path to jobs file

        Returns:
            Dictionary of existing jobs
        """
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            with open(file_path, "r") as infile:
                content = yaml.safe_load(infile)
                if content is None:
                    return {}
                return content
        return {}
