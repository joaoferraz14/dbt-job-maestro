"""Deployment helpers for dbt-jobs-as-code integration"""

import os
import subprocess
import yaml
from pathlib import Path
from typing import Optional, Tuple


def check_dbt_jobs_as_code_installed() -> bool:
    """
    Check if dbt-jobs-as-code package is installed

    Returns:
        True if installed, False otherwise
    """
    try:
        import importlib.util
        spec = importlib.util.find_spec("dbt_jobs_as_code")
        return spec is not None
    except (ImportError, AttributeError):
        return False


def check_packages_yml(dbt_project_path: str = ".") -> Tuple[bool, Optional[str]]:
    """
    Check if dbt-jobs-as-code is in packages.yml

    Args:
        dbt_project_path: Path to dbt project directory

    Returns:
        Tuple of (is_present, packages_yml_path)
    """
    packages_paths = [
        Path(dbt_project_path) / "packages.yml",
        Path(dbt_project_path) / "dependencies.yml",  # dbt 1.6+
    ]

    for packages_path in packages_paths:
        if packages_path.exists():
            try:
                with open(packages_path, "r") as f:
                    packages = yaml.safe_load(f) or {}

                packages_list = packages.get("packages", [])
                for package in packages_list:
                    # Check for dbt-jobs-as-code in various formats
                    if isinstance(package, dict):
                        git_url = package.get("git", "")
                        if "dbt-jobs-as-code" in git_url:
                            return True, str(packages_path)

                return False, str(packages_path)
            except Exception:
                pass

    return False, None


def add_to_packages_yml(dbt_project_path: str = ".") -> bool:
    """
    Add dbt-jobs-as-code to packages.yml

    Args:
        dbt_project_path: Path to dbt project directory

    Returns:
        True if successful, False otherwise
    """
    packages_path = Path(dbt_project_path) / "packages.yml"

    try:
        # Read existing packages or create new structure
        if packages_path.exists():
            with open(packages_path, "r") as f:
                packages = yaml.safe_load(f) or {}
        else:
            packages = {}

        # Ensure packages list exists
        if "packages" not in packages:
            packages["packages"] = []

        # Check if already present
        for package in packages["packages"]:
            if isinstance(package, dict):
                git_url = package.get("git", "")
                if "dbt-jobs-as-code" in git_url:
                    return True  # Already present

        # Add dbt-jobs-as-code
        packages["packages"].append({
            "git": "https://github.com/dbt-labs/dbt-jobs-as-code.git",
            "revision": "main",  # Use latest or specify version
        })

        # Write back
        with open(packages_path, "w") as f:
            yaml.dump(packages, f, default_flow_style=False, sort_keys=False, indent=2)

        return True

    except Exception as e:
        print(f"Error adding to packages.yml: {e}")
        return False


def get_current_branch() -> Optional[str]:
    """
    Get the current git branch

    Returns:
        Branch name or None if not in a git repository
    """
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None


def should_deploy(deploy_branch: str) -> bool:
    """
    Check if current branch matches deployment branch

    Args:
        deploy_branch: Branch that triggers deployment

    Returns:
        True if should deploy, False otherwise
    """
    current_branch = get_current_branch()
    if current_branch is None:
        return False

    return current_branch == deploy_branch


def validate_deployment_requirements(
    dbt_project_path: str = ".", deploy_branch: str = "main"
) -> Tuple[bool, list]:
    """
    Validate all requirements for deployment

    Args:
        dbt_project_path: Path to dbt project
        deploy_branch: Branch that triggers deployment

    Returns:
        Tuple of (is_valid, list_of_issues)
    """
    issues = []

    # Check current branch
    current_branch = get_current_branch()
    if current_branch != deploy_branch:
        issues.append(
            f"Not on deployment branch (current: {current_branch}, expected: {deploy_branch})"
        )

    # Check if dbt-jobs-as-code is installed
    if not check_dbt_jobs_as_code_installed():
        issues.append("dbt-jobs-as-code package is not installed")

    # Check if it's in packages.yml
    in_packages, packages_path = check_packages_yml(dbt_project_path)
    if not in_packages:
        if packages_path:
            issues.append(f"dbt-jobs-as-code not found in {packages_path}")
        else:
            issues.append("packages.yml not found")

    return len(issues) == 0, issues
