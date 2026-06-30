"""
dbt-job-maestro: Generate dbt selectors, jobs, and Airflow DAGs from manifest.json

This package analyzes your dbt project's manifest.json and generates selectors
by grouping models based on their dependency graph (FQN method). From those
selectors it can optionally generate dbt Cloud job definitions and/or Apache
Airflow DAGs.
"""

__version__ = "0.3.0"

from dbt_job_maestro.manifest_parser import ManifestParser
from dbt_job_maestro.graph_builder import GraphBuilder
from dbt_job_maestro.job_generator import JobGenerator
from dbt_job_maestro.airflow_dag_generator import AirflowDAGGenerator
from dbt_job_maestro.config import Config, SelectorConfig, JobConfig, AirflowConfig

__all__ = [
    "ManifestParser",
    "GraphBuilder",
    "JobGenerator",
    "AirflowDAGGenerator",
    "Config",
    "SelectorConfig",
    "JobConfig",
    "AirflowConfig",
]
