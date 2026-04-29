"""
dbt-job-maestro: Generate dbt selectors and jobs from manifest.json

This package analyzes your dbt project's manifest.json and generates selectors
by grouping models based on their dependency graph (FQN method). Optionally
generates dbt Cloud job definitions.
"""

__version__ = "0.3.0"

from dbt_job_maestro.manifest_parser import ManifestParser
from dbt_job_maestro.graph_builder import GraphBuilder
from dbt_job_maestro.job_generator import JobGenerator
from dbt_job_maestro.config import Config, SelectorConfig, JobConfig

__all__ = [
    "ManifestParser",
    "GraphBuilder",
    "JobGenerator",
    "Config",
    "SelectorConfig",
    "JobConfig",
]
