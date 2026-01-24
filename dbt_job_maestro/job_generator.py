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

        for idx, selector in enumerate(filtered_selectors):
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
                total_jobs=len(filtered_selectors),
                previous_job_name=(
                    self._generate_job_name(filtered_selectors[idx - 1]["name"])
                    if idx > 0
                    else None
                ),
            )
            jobs[job_name] = job

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
                "github_webhook": False,
                "git_provider_webhook": False,
                "custom_branch_only": True,
                "schedule": True,
            }
            schedule = {"cron": cron_schedule}
        elif self.config.orchestration_mode == "cascade":
            if job_index == 0:
                # First job is always scheduled
                cron_schedule = self._generate_start_time_cron()
                triggers = {
                    "github_webhook": False,
                    "git_provider_webhook": False,
                    "custom_branch_only": True,
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
                        "github_webhook": False,
                        "git_provider_webhook": False,
                        "custom_branch_only": True,
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
                        "github_webhook": False,
                        "git_provider_webhook": False,
                        "custom_branch_only": True,
                        "schedule": False,
                        "on_job_completion": {
                            "job_id": previous_job_id,
                            "statuses": [
                                "success",
                                "error",
                                "cancelled",
                            ],  # Any status triggers next
                        },
                    }
                    schedule = None
        else:  # simple mode (default)
            triggers = {
                "github_webhook": False,
                "git_provider_webhook": False,
                "custom_branch_only": True,
                "schedule": True,
            }
            schedule = {"cron": self.config.cron_schedule}

        job = {
            "identifier": self._generate_job_name(selector_name),
            "name": job_dbt_name,
            "dbt_version": self.config.dbt_version or None,
            "triggers": triggers,
            "execution": {
                "timeout_seconds": self.config.timeout_seconds,
            },
            "settings": {
                "threads": self.config.threads,
                "target_name": self.config.target_name,
            },
            "generate_docs": self.config.generate_docs,
            "run_generate_sources": self.config.run_generate_sources,
            "execute_steps": [f"dbt build --selector {selector_name}"],
        }

        # Add schedule if applicable
        if schedule:
            job["schedule"] = schedule

        # Add dbt Cloud IDs if provided
        if self.config.account_id:
            job["account_id"] = self.config.account_id
        if self.config.project_id:
            job["project_id"] = self.config.project_id
        if self.config.environment_id:
            job["environment_id"] = self.config.environment_id

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
