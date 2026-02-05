#!/usr/bin/env python3
"""
Update maestro-config.yml with job IDs for cascade mode.

This script takes job IDs (from fetch_job_ids.py or manual input)
and updates the maestro-config.yml file with the job_id_mapping.

Usage:
    python scripts/update_config_with_job_ids.py \
        --config maestro-config.yml \
        --job-ids job_ids.yml \
        --disable-initial-deployment
"""

import argparse
import sys
import yaml


def update_config_with_job_ids(config_path: str, job_ids_path: str, disable_initial: bool = True):
    """
    Update maestro-config.yml with job IDs.

    Args:
        config_path: Path to maestro-config.yml
        job_ids_path: Path to YAML file with job_id_mapping
        disable_initial: Set cascade_initial_deployment to False
    """
    # Read config
    with open(config_path, "r") as f:
        config = yaml.safe_load(f) or {}

    # Read job IDs
    with open(job_ids_path, "r") as f:
        job_ids_data = yaml.safe_load(f) or {}

    # Extract job_id_mapping
    if "job_id_mapping" in job_ids_data:
        job_id_mapping = job_ids_data["job_id_mapping"]
    else:
        # Assume the entire file is the mapping
        job_id_mapping = job_ids_data

    # Update config
    if "job" not in config:
        config["job"] = {}

    config["job"]["job_id_mapping"] = job_id_mapping

    if disable_initial:
        config["job"]["cascade_initial_deployment"] = False

    # Write updated config
    with open(config_path, "w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False, indent=2)

    print(f"✅ Updated {config_path} with {len(job_id_mapping)} job IDs")
    if disable_initial:
        print("✅ Set cascade_initial_deployment: false")


def main():
    parser = argparse.ArgumentParser(
        description="Update maestro-config.yml with job IDs for cascade mode"
    )
    parser.add_argument("--config", required=True, help="Path to maestro-config.yml file")
    parser.add_argument("--job-ids", required=True, help="Path to YAML file with job_id_mapping")
    parser.add_argument(
        "--disable-initial-deployment",
        action="store_true",
        help="Set cascade_initial_deployment to false",
    )

    args = parser.parse_args()

    try:
        update_config_with_job_ids(
            config_path=args.config,
            job_ids_path=args.job_ids,
            disable_initial=args.disable_initial_deployment,
        )
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
