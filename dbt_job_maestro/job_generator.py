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

        for selector in selectors:
            selector_name = selector["name"]

            # Skip freshness selectors (they're referenced by main jobs)
            if selector_name.startswith("freshness_") or selector_name.startswith(
                "automatically_generated_freshness_"
            ):
                continue

            # Generate job name
            job_name = self._generate_job_name(selector_name)

            # Check if job is manually created (should not be overwritten)
            if job_name in jobs and self._is_manually_created(jobs[job_name]):
                continue

            # Create job definition
            job = self._create_job_definition(selector_name)
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

    def _create_job_definition(self, selector_name: str) -> Dict[str, Any]:
        """
        Create job definition for a selector

        Args:
            selector_name: Name of the selector

        Returns:
            Job definition dictionary compatible with dbt-jobs-as-code
        """
        job_dbt_name = f"{self.config.job_name_prefix}-{selector_name}"

        job = {
            "identifier": self._generate_job_name(selector_name),
            "name": job_dbt_name,
            "dbt_version": self.config.dbt_version or None,
            "triggers": {
                "github_webhook": False,
                "git_provider_webhook": False,
                "custom_branch_only": True,
                "schedule": True,
            },
            "schedule": {"cron": self.config.cron_schedule},
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
