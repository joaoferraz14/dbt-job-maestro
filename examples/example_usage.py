"""
Example usage of dbt-job-maestro as a Python library
"""

from dbt_job_maestro import ManifestParser, GraphBuilder, SelectorGenerator
from dbt_job_maestro.config import Config, SelectorConfig


def example_with_config_file():
    """Example: Load configuration from YAML file"""
    print("=" * 60)
    print("Example 1: Using configuration file")
    print("=" * 60)

    # Load configuration from file
    config = Config.from_yaml("config.yml")

    # Parse manifest
    parser = ManifestParser(config.manifest_path)
    models = parser.get_models()
    print(f"Found {len(models)} models in manifest")

    # Build dependency graph
    graph = GraphBuilder(models)

    # Generate selectors
    generator = SelectorGenerator(parser, graph, config.selector)
    selectors = generator.generate_selectors()
    print(f"Generated {len(selectors)} selectors")

    # Write to file
    generator.write_selectors(selectors, config.selectors_output_file)
    print(f"✓ Selectors written to {config.selectors_output_file}\n")


def example_generate_fqn_selectors():
    """Example: Generate selectors using FQN method"""
    print("=" * 60)
    print("Example 2: Generate FQN-based selectors")
    print("=" * 60)

    # Parse manifest
    parser = ManifestParser("target/manifest.json")
    models = parser.get_models()
    print(f"Found {len(models)} models in manifest")

    # Build dependency graph
    graph = GraphBuilder(models)

    # Configure selector generation
    selector_config = SelectorConfig(
        method="fqn",
        group_by_dependencies=True,
        exclude_tags=["deprecated", "archived"],
        include_freshness_selectors=True,
    )

    # Generate selectors
    selector_gen = SelectorGenerator(parser, graph, selector_config)
    selectors = selector_gen.generate_selectors()
    print(f"Generated {len(selectors)} selectors")

    # Write to file
    selector_gen.write_selectors(selectors, "selectors_fqn.yml")
    print("✓ Selectors written to selectors_fqn.yml\n")


def example_generate_path_selectors():
    """Example: Generate selectors using path method"""
    print("=" * 60)
    print("Example 3: Generate path-based selectors")
    print("=" * 60)

    parser = ManifestParser("target/manifest.json")
    models = parser.get_models()
    graph = GraphBuilder(models)

    selector_config = SelectorConfig(
        method="path",
        path_grouping_level=1,  # Group by first subdirectory
        min_models_per_selector=3,  # Only create selectors with 3+ models
    )

    selector_gen = SelectorGenerator(parser, graph, selector_config)
    selectors = selector_gen.generate_selectors()
    print(f"Generated {len(selectors)} path-based selectors")

    selector_gen.write_selectors(selectors, "selectors_path.yml")
    print("✓ Selectors written to selectors_path.yml\n")


def example_generate_tag_selectors():
    """Example: Generate selectors using tag method"""
    print("=" * 60)
    print("Example 4: Generate tag-based selectors")
    print("=" * 60)

    parser = ManifestParser("target/manifest.json")
    models = parser.get_models()
    graph = GraphBuilder(models)

    # Show available tags
    all_tags = parser.get_all_tags()
    print(f"Available tags: {', '.join(sorted(all_tags))}")

    selector_config = SelectorConfig(
        method="tag",
        exclude_tags=["deprecated"],
    )

    selector_gen = SelectorGenerator(parser, graph, selector_config)
    selectors = selector_gen.generate_selectors()
    print(f"Generated {len(selectors)} tag-based selectors")

    selector_gen.write_selectors(selectors, "selectors_tag.yml")
    print("✓ Selectors written to selectors_tag.yml\n")


def example_custom_exclusions():
    """Example: Advanced exclusions"""
    print("=" * 60)
    print("Example 5: Custom exclusions")
    print("=" * 60)

    parser = ManifestParser("target/manifest.json")
    models = parser.get_models()
    graph = GraphBuilder(models)

    selector_config = SelectorConfig(
        method="fqn",
        exclude_tags=["deprecated", "experimental", "temp"],
        exclude_models=["test_model", "temp_stg_users"],
        exclude_paths=["models/staging/legacy"],
    )

    selector_gen = SelectorGenerator(parser, graph, selector_config)
    selectors = selector_gen.generate_selectors()
    print(f"Generated {len(selectors)} selectors with exclusions applied")

    selector_gen.write_selectors(selectors, "selectors_filtered.yml")
    print("✓ Selectors written to selectors_filtered.yml\n")


def example_project_info():
    """Example: Get project information"""
    print("=" * 60)
    print("Example 6: Analyze project structure")
    print("=" * 60)

    parser = ManifestParser("target/manifest.json")
    models = parser.get_models()
    graph = GraphBuilder(models)

    print(f"Total models: {len(models)}")

    # Show tags
    all_tags = parser.get_all_tags()
    print(f"\nTags ({len(all_tags)}):")
    for tag in sorted(all_tags):
        count = len(parser.get_models_by_tag(tag))
        print(f"  - {tag}: {count} models")

    # Show paths
    path_prefixes = parser.get_path_prefixes(level=1)
    print(f"\nPath prefixes ({len(path_prefixes)}):")
    for path in sorted(path_prefixes):
        count = len(parser.get_models_by_path(path))
        print(f"  - {path}: {count} models")

    # Show dependency analysis
    components = graph.find_connected_components()
    independent = graph.find_independent_models()

    print(f"\nDependency Analysis:")
    print(f"  - Connected components: {len(components)}")
    print(f"  - Independent models: {len(independent)}")
    if components:
        largest = max(len(c) for c in components)
        print(f"  - Largest component: {largest} models")
    print()


def example_save_config():
    """Example: Create and save configuration"""
    print("=" * 60)
    print("Example 7: Create and save configuration")
    print("=" * 60)

    # Create custom configuration
    config = Config()
    config.manifest_path = "target/manifest.json"
    config.selectors_output_file = "my_selectors.yml"

    config.selector.method = "mixed"
    config.selector.exclude_tags = ["deprecated", "test"]
    config.selector.include_freshness_selectors = True
    config.selector.min_models_per_selector = 2

    # Save to file
    config.to_yaml("my_config.yml")
    print("✓ Configuration saved to my_config.yml")

    # Load it back
    loaded_config = Config.from_yaml("my_config.yml")
    print(f"✓ Configuration loaded: method={loaded_config.selector.method}\n")


if __name__ == "__main__":
    # Run examples
    # Note: Make sure you have a manifest.json in target/ directory

    try:
        print("\n")
        print("=" * 60)
        print("dbt-job-maestro Examples")
        print("=" * 60)
        print("\n")

        example_project_info()
        example_generate_fqn_selectors()
        example_generate_path_selectors()
        example_generate_tag_selectors()
        example_custom_exclusions()
        example_save_config()

        # Only run config file example if config exists
        import os
        if os.path.exists("config.yml"):
            example_with_config_file()

        print("=" * 60)
        print("✓ All examples completed successfully!")
        print("=" * 60)
        print("\nNext steps:")
        print("1. Review the generated selector files")
        print("2. Test selectors: dbt list --selector <selector_name>")
        print("3. For creating dbt Cloud jobs, use dbt-jobs-as-code:")
        print("   https://pypi.org/project/dbt-jobs-as-code/")
        print()

    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("\nMake sure you have a manifest.json file in target/ directory")
        print("Run 'dbt compile' in your dbt project first")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
