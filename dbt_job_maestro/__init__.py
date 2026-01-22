"""
dbt-job-maestro: Generate dbt selectors from manifest.json

This package helps you automatically generate dbt selectors by analyzing your dbt project's
manifest.json file. It groups models by:
- Shared dependencies (FQN method)
- Folder structure (Path method)
- dbt tags (Tag method)
- Mixed strategies

For materializing selectors as dbt Cloud jobs, use the dbt-jobs-as-code package.
"""

__version__ = "0.1.0"

from dbt_job_maestro.manifest_parser import ManifestParser
from dbt_job_maestro.graph_builder import GraphBuilder
from dbt_job_maestro.selector_generator import SelectorGenerator
from dbt_job_maestro.job_generator import JobGenerator
from dbt_job_maestro.config import Config, SelectorConfig, JobConfig, DeploymentConfig

__all__ = [
    "ManifestParser",
    "GraphBuilder",
    "SelectorGenerator",
    "JobGenerator",
    "Config",
    "SelectorConfig",
    "JobConfig",
    "DeploymentConfig",
]
