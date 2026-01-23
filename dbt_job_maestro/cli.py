"""Command-line interface for dbt-job-maestro"""

import click
import sys
from pathlib import Path

from dbt_job_maestro.config import Config, SelectorConfig
from dbt_job_maestro.manifest_parser import ManifestParser
from dbt_job_maestro.graph_builder import GraphBuilder
from dbt_job_maestro.selector_generator import SelectorGenerator
from dbt_job_maestro.selector_orchestrator import SelectorOrchestrator
from dbt_job_maestro.job_generator import JobGenerator


@click.group()
@click.version_option(version="0.1.0")
def main():
    """
    dbt-job-maestro: Generate dbt selectors from manifest.json

    Automatically generate dbt selector definitions by analyzing your dbt project's
    dependency graph, folder structure, and tags.

    For creating dbt Cloud jobs from selectors, use the dbt-jobs-as-code package.
    """
    pass


@main.command()
@click.option(
    "--config",
    "-c",
    help="Path to configuration YAML file",
    type=click.Path(exists=True),
)
@click.option(
    "--manifest",
    "-m",
    help="Path to dbt manifest.json file (overrides config)",
    type=click.Path(exists=True),
)
@click.option(
    "--output",
    "-o",
    help="Output file for selectors (overrides config)",
    type=click.Path(),
)
@click.option(
    "--method",
    "-t",
    type=click.Choice(["fqn", "path", "tag", "mixed"], case_sensitive=False),
    help="Selector generation method (overrides config)",
)
@click.option(
    "--group-by-dependencies/--no-group-by-dependencies",
    default=None,
    help="Group models by shared dependencies (overrides config)",
)
@click.option(
    "--exclude-tag",
    multiple=True,
    help="Tags to exclude from selectors (can be used multiple times, adds to config)",
)
@click.option(
    "--path-level",
    type=int,
    help="Directory level for path grouping (overrides config)",
)
@click.option(
    "--min-models",
    type=int,
    help="Minimum models per selector (overrides config)",
)
@click.option(
    "--no-freshness",
    is_flag=True,
    default=False,
    help="Disable generation of freshness selectors",
)
def generate(
    config,
    manifest,
    output,
    method,
    group_by_dependencies,
    exclude_tag,
    path_level,
    min_models,
    no_freshness,
):
    """
    Generate dbt selectors from manifest.json

    You can use a config file for settings or pass options via command line.
    Command line options override config file settings.

    Examples:

      # Use config file
      dbt-job-maestro generate --config config.yml

      # Use command line options
      dbt-job-maestro generate --manifest target/manifest.json --method fqn

      # Mix config file with overrides
      dbt-job-maestro generate --config config.yml --exclude-tag deprecated
    """
    try:
        # Load config from file or use defaults
        if config:
            click.echo(f"Loading configuration from {config}...")
            cfg = Config.from_yaml(config)
        else:
            cfg = Config()

        # Override config with command line options
        if manifest:
            cfg.manifest_path = manifest
        if output:
            cfg.selectors_output_file = output
        if method:
            cfg.selector.method = method
        if group_by_dependencies is not None:
            cfg.selector.group_by_dependencies = group_by_dependencies
        if exclude_tag:
            # Add to existing exclude tags
            cfg.selector.exclude_tags = list(set(cfg.selector.exclude_tags + list(exclude_tag)))
        if path_level is not None:
            cfg.selector.path_grouping_level = path_level
        if min_models is not None:
            cfg.selector.min_models_per_selector = min_models
        if no_freshness:
            cfg.selector.include_freshness_selectors = False

        click.echo(f"Reading manifest from {cfg.manifest_path}...")
        parser = ManifestParser(cfg.manifest_path)

        click.echo("Building dependency graph...")
        models = parser.get_models()
        click.echo(f"Found {len(models)} models")

        graph = GraphBuilder(models)

        click.echo(f"Generating selectors using method: {cfg.selector.method}...")
        if cfg.selector.exclude_tags:
            click.echo(f"Excluding tags: {', '.join(cfg.selector.exclude_tags)}")

        # Use new SelectorOrchestrator for supported methods
        if cfg.selector.method in ["fqn", "mixed"]:
            generator = SelectorOrchestrator(parser, graph, cfg.selector)
        else:
            # Fall back to SelectorGenerator for backward compatibility
            generator = SelectorGenerator(parser, graph, cfg.selector)

        selectors = generator.generate_selectors()

        output_path = Path(cfg.output_dir) / cfg.selectors_output_file
        click.echo(f"Writing {len(selectors)} selectors to {output_path}...")
        generator.write_selectors(selectors, str(output_path))

        click.echo(click.style("\n✓ Selectors generated successfully!", fg="green", bold=True))
        click.echo(f"\nOutput: {output_path}")

        if cfg.selector.method == "fqn":
            components = graph.find_connected_components()
            click.echo(f"Generated {len([s for s in selectors if not s['name'].startswith('freshness_')])} selector groups")

        click.echo("\n" + "=" * 60)
        click.echo("Next Steps:")
        click.echo("=" * 60)
        click.echo("1. Review the generated selectors in your selectors.yml file")
        click.echo("2. Test selectors: dbt list --selector <selector_name>")
        click.echo("3. To create dbt Cloud jobs from these selectors,")
        click.echo("   use the dbt-jobs-as-code package:")
        click.echo("   https://pypi.org/project/dbt-jobs-as-code/")

    except FileNotFoundError as e:
        click.echo(click.style(f"\n✗ Error: {e}", fg="red", bold=True), err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(click.style(f"\n✗ Error: {e}", fg="red", bold=True), err=True)
        import traceback
        traceback.print_exc()
        sys.exit(1)


@main.command()
@click.option(
    "--config",
    "-c",
    help="Path to configuration YAML file",
    type=click.Path(exists=True),
)
@click.option(
    "--selectors",
    "-s",
    help="Path to selectors YAML file (overrides config)",
    type=click.Path(exists=True),
)
@click.option(
    "--output",
    "-o",
    help="Output file for jobs (overrides config)",
    type=click.Path(),
)
@click.option(
    "--account-id",
    type=int,
    help="DBT Cloud account ID (overrides config)",
)
@click.option(
    "--project-id",
    type=int,
    help="DBT Cloud project ID (overrides config)",
)
@click.option(
    "--environment-id",
    type=int,
    help="DBT Cloud environment ID (overrides config)",
)
def generate_jobs(config, selectors, output, account_id, project_id, environment_id):
    """
    Generate jobs.yml from selectors for dbt-jobs-as-code

    Creates a jobs.yml file that can be deployed to dbt Cloud using:
    dbt-jobs-as-code sync-jobs jobs.yml

    Examples:

      # Using config file
      dbt-job-maestro generate-jobs --config config.yml

      # Using command line options
      dbt-job-maestro generate-jobs --selectors selectors.yml --output jobs.yml

      # Override config settings
      dbt-job-maestro generate-jobs --config config.yml --account-id 12345
    """
    try:
        # Load config from file or use defaults
        if config:
            click.echo(f"Loading configuration from {config}...")
            cfg = Config.from_yaml(config)
        else:
            cfg = Config()

        # Override config with command line options
        if selectors:
            cfg.selectors_output_file = selectors
        if output:
            cfg.jobs_output_file = output
        if account_id is not None:
            cfg.job.account_id = account_id
        if project_id is not None:
            cfg.job.project_id = project_id
        if environment_id is not None:
            cfg.job.environment_id = environment_id

        # Read selectors
        click.echo(f"Reading selectors from {cfg.selectors_output_file}...")
        import yaml

        with open(cfg.selectors_output_file, "r") as f:
            selector_data = yaml.safe_load(f)
            selector_list = selector_data.get("selectors", [])

        if not selector_list:
            click.echo(
                click.style("✗ No selectors found in file", fg="red", bold=True), err=True
            )
            sys.exit(1)

        click.echo(f"Generating jobs for {len(selector_list)} selectors...")

        # Generate jobs
        job_generator = JobGenerator(cfg.job)
        existing_jobs = job_generator.read_existing_jobs(cfg.jobs_output_file)
        jobs = job_generator.generate_jobs(selector_list, existing_jobs)

        click.echo(f"Writing {len(jobs.get('jobs', {}))} jobs to {cfg.jobs_output_file}...")
        job_generator.write_jobs(jobs, cfg.jobs_output_file)

        click.echo(click.style("\n✓ Jobs generated successfully!", fg="green", bold=True))
        click.echo(f"\nOutput: {cfg.jobs_output_file}")

        if not cfg.job.account_id or not cfg.job.project_id or not cfg.job.environment_id:
            click.echo(
                click.style(
                    "\n⚠ Warning: DBT Cloud IDs not fully configured.",
                    fg="yellow",
                )
            )
            click.echo("Update config.yml with your dbt Cloud account/project/environment IDs.")

        click.echo("\n" + "=" * 60)
        click.echo("Next Steps:")
        click.echo("=" * 60)
        click.echo("1. Review the generated jobs.yml file")
        click.echo("2. Commit selectors.yml and jobs.yml to version control")
        click.echo("3. Deploy to dbt Cloud using dbt-jobs-as-code:")
        click.echo(f"   dbt-jobs-as-code sync-jobs {cfg.jobs_output_file}")
        click.echo("\n4. Or set up CI/CD to auto-deploy on merge to main branch")

    except FileNotFoundError as e:
        click.echo(click.style(f"\n✗ Error: {e}", fg="red", bold=True), err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(click.style(f"\n✗ Error: {e}", fg="red", bold=True), err=True)
        import traceback
        traceback.print_exc()
        sys.exit(1)


@main.command()
@click.option(
    "--manifest",
    "-m",
    default="target/manifest.json",
    help="Path to dbt manifest.json file",
    type=click.Path(exists=True),
)
def info(manifest):
    """
    Display information about the dbt project

    Analyzes the manifest to show:
    - Total number of models
    - Available tags and model counts
    - Folder structure and model counts
    - Dependency analysis (connected components, independent models)
    """
    try:
        parser = ManifestParser(manifest)
        models = parser.get_models()
        graph = GraphBuilder(models)

        click.echo("\n" + "=" * 60)
        click.echo("DBT Project Information")
        click.echo("=" * 60)

        click.echo(f"\n📊 Total Models: {len(models)}")

        # Show tags
        all_tags = parser.get_all_tags()
        if all_tags:
            click.echo(f"\n🏷️  Tags ({len(all_tags)}):")
            for tag in sorted(all_tags):
                models_with_tag = len(parser.get_models_by_tag(tag))
                click.echo(f"   - {tag}: {models_with_tag} models")
        else:
            click.echo("\n🏷️  No tags found")

        # Show paths
        path_prefixes = parser.get_path_prefixes(level=1)
        if path_prefixes:
            click.echo(f"\n📁 Path Prefixes ({len(path_prefixes)}):")
            for path in sorted(path_prefixes):
                models_in_path = len(parser.get_models_by_path(path))
                click.echo(f"   - {path}: {models_in_path} models")

        # Show dependencies
        components = graph.find_connected_components()
        independent = graph.find_independent_models()

        click.echo(f"\n🔗 Dependency Analysis:")
        click.echo(f"   - Connected components: {len(components)}")
        click.echo(f"   - Independent models: {len(independent)}")
        if components:
            largest = max(len(c) for c in components)
            click.echo(f"   - Largest component: {largest} models")

        click.echo("\n" + "=" * 60)
        click.echo("💡 Suggested Selector Methods:")
        click.echo("=" * 60)

        if all_tags and len(all_tags) >= 3:
            click.echo("✓ Tag-based: You have multiple tags - consider --method tag")

        if path_prefixes and len(path_prefixes) >= 3:
            click.echo("✓ Path-based: You have multiple directories - consider --method path")

        if components and len(components) >= 2:
            click.echo("✓ FQN-based: You have connected components - consider --method fqn")

        click.echo("\n")

    except FileNotFoundError as e:
        click.echo(click.style(f"\n✗ Error: {e}", fg="red", bold=True), err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(click.style(f"\n✗ Error: {e}", fg="red", bold=True), err=True)
        sys.exit(1)


@main.command()
@click.option(
    "--output",
    "-o",
    default="config.yml",
    help="Output file for configuration template",
    type=click.Path(),
)
def init(output):
    """
    Create a configuration file template

    Generates a config.yml file with all available options and documentation.
    You can then customize this file for your project.
    """
    try:
        if Path(output).exists():
            if not click.confirm(f"{output} already exists. Overwrite?"):
                click.echo("Cancelled.")
                return

        # Create a default config and save it
        config = Config()
        config.to_yaml(output)

        click.echo(click.style(f"\n✓ Configuration template created: {output}", fg="green", bold=True))
        click.echo("\nEdit this file to customize selector generation for your project.")
        click.echo(f"\nThen run: dbt-job-maestro generate --config {output}")

    except Exception as e:
        click.echo(click.style(f"\n✗ Error: {e}", fg="red", bold=True), err=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
