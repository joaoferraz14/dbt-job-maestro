#!/usr/bin/env python3
"""
Fetch dbt Cloud job IDs for cascade mode configuration.

This script retrieves job IDs from dbt Cloud API and formats them
for use in the job_id_mapping configuration field.

Usage:
    export DBT_API_TOKEN="your_token_here"
    python scripts/fetch_job_ids.py --account-id 12345 --project-id 67890

Requirements:
    pip install requests
"""

import argparse
import os
import sys
import requests


def fetch_job_ids(account_id: int, project_id: int = None, api_token: str = None):
    """
    Fetch job IDs from dbt Cloud API.

    Args:
        account_id: dbt Cloud account ID
        project_id: Optional project ID to filter jobs
        api_token: dbt Cloud API token (defaults to DBT_API_TOKEN env var)

    Returns:
        Dictionary mapping job names to job IDs
    """
    if api_token is None:
        api_token = os.getenv("DBT_API_TOKEN")
        if not api_token:
            raise ValueError(
                "DBT_API_TOKEN environment variable not set. "
                "Set it with: export DBT_API_TOKEN='your_token'"
            )

    headers = {"Authorization": f"Token {api_token}", "Content-Type": "application/json"}

    # Fetch jobs from dbt Cloud API
    url = f"https://cloud.getdbt.com/api/v2/accounts/{account_id}/jobs/"

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching jobs from dbt Cloud API: {e}", file=sys.stderr)
        sys.exit(1)

    jobs = data.get("data", [])

    # Filter by project if specified
    if project_id:
        jobs = [job for job in jobs if job.get("project_id") == project_id]

    # Build mapping
    job_mapping = {}
    for job in jobs:
        job_name = job.get("name", "").replace("-", "_")
        job_id = job.get("id")
        if job_name and job_id:
            job_mapping[job_name] = job_id

    return job_mapping


def main():
    parser = argparse.ArgumentParser(
        description="Fetch dbt Cloud job IDs for cascade mode configuration"
    )
    parser.add_argument("--account-id", type=int, required=True, help="dbt Cloud account ID")
    parser.add_argument(
        "--project-id",
        type=int,
        help="dbt Cloud project ID (optional, filters jobs to this project)",
    )
    parser.add_argument(
        "--api-token", help="dbt Cloud API token (defaults to DBT_API_TOKEN env var)"
    )
    parser.add_argument(
        "--format",
        choices=["yaml", "json", "table"],
        default="yaml",
        help="Output format (default: yaml)",
    )

    args = parser.parse_args()

    print(f"Fetching jobs from dbt Cloud account {args.account_id}...", file=sys.stderr)
    if args.project_id:
        print(f"Filtering to project {args.project_id}...", file=sys.stderr)

    job_mapping = fetch_job_ids(
        account_id=args.account_id, project_id=args.project_id, api_token=args.api_token
    )

    if not job_mapping:
        print("No jobs found!", file=sys.stderr)
        sys.exit(1)

    print(f"\nFound {len(job_mapping)} jobs:", file=sys.stderr)
    print("", file=sys.stderr)

    # Output in requested format
    if args.format == "yaml":
        print("# Add this to your maestro-config.yml under job:")
        print("job_id_mapping:")
        for job_name, job_id in sorted(job_mapping.items()):
            print(f"  {job_name}: {job_id}")
    elif args.format == "json":
        import json

        print(json.dumps(job_mapping, indent=2))
    else:  # table
        print(f"{'Job Name':<50} {'Job ID':<10}")
        print("-" * 60)
        for job_name, job_id in sorted(job_mapping.items()):
            print(f"{job_name:<50} {job_id:<10}")


if __name__ == "__main__":
    main()
