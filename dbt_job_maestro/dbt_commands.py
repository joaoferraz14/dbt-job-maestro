"""Per-selector dbt command resolution and step building.

Single source of truth shared by the dbt Cloud job generator and the Airflow DAG
generator so both translate a selector's configured command the same way.

A selector may run one of four commands:

* ``build``     - ``dbt build --selector X``            (default)
* ``run``       - ``dbt run --selector X``
* ``test``      - ``dbt test --selector X``
* ``run_test``  - ``dbt run --selector X`` **then** ``dbt test --selector X``

``run_test`` expands to TWO ordered steps. In dbt Cloud these are two
``execute_steps``; in Airflow they become two tasks wired ``run >> test`` (so the
test only runs if the run succeeds).
"""

from typing import Dict, List, Optional

# Commands a selector may be configured to run.
VALID_COMMANDS = ("build", "run", "run_test", "test")


def validate_commands(
    default_command: str,
    selector_commands: Optional[Dict[str, str]] = None,
) -> None:
    """Validate a default command and per-selector command overrides.

    Args:
        default_command: Command applied to selectors without an override.
        selector_commands: Mapping of selector name -> command override.

    Raises:
        ValueError: If any command is not one of ``VALID_COMMANDS``.
    """
    if default_command not in VALID_COMMANDS:
        raise ValueError(
            f"Invalid default_command '{default_command}'. "
            f"Must be one of: {', '.join(VALID_COMMANDS)}"
        )
    for name, command in (selector_commands or {}).items():
        if command not in VALID_COMMANDS:
            raise ValueError(
                f"Invalid command '{command}' for selector '{name}'. "
                f"Must be one of: {', '.join(VALID_COMMANDS)}"
            )


def resolve_command(
    selector_name: str,
    default_command: str = "build",
    selector_commands: Optional[Dict[str, str]] = None,
    selector_prefix: str = "",
) -> str:
    """Resolve the command for a selector.

    Per-selector overrides apply to **manual** selectors only. Auto-generated
    selectors (name starting with ``{selector_prefix}_``) always use
    ``default_command`` so the whole automated set runs the same command; any
    override keyed to an auto-generated selector is ignored.

    Args:
        selector_name: Name of the selector.
        default_command: Command used for auto-generated selectors and for
            manual selectors without an override.
        selector_commands: Mapping of manual selector name -> command override.
        selector_prefix: Prefix identifying auto-generated selectors (e.g.
            "maestro"). Empty disables the auto/manual distinction (overrides
            then apply to any selector).

    Returns:
        One of ``VALID_COMMANDS``.
    """
    if selector_prefix and selector_name.startswith(f"{selector_prefix}_"):
        return default_command
    return (selector_commands or {}).get(selector_name, default_command)


def build_steps(
    command: str,
    selector_name: str,
    extra_flags: str = "",
    full_refresh: bool = False,
) -> List[str]:
    """Build the dbt CLI step(s) for a selector's command.

    Args:
        command: One of ``VALID_COMMANDS``.
        selector_name: Selector to target via ``--selector``.
        extra_flags: Runtime flags appended to each step (already
            space-joined, e.g. "--project-dir /x --threads 8"). Empty for
            dbt Cloud, where the environment supplies these.
        full_refresh: When True, add ``--full-refresh`` to ``build``/``run``
            steps (``test`` steps never take it).

    Returns:
        Ordered list of dbt command strings (two for ``run_test``, else one).
    """
    suffix = f" {extra_flags}".rstrip() if extra_flags else ""

    def _step(verb: str, allow_full_refresh: bool = True) -> str:
        fr = " --full-refresh" if full_refresh and allow_full_refresh else ""
        return f"dbt {verb}{fr} --selector {selector_name}{suffix}"

    if command == "run":
        return [_step("run")]
    if command == "test":
        return [_step("test", allow_full_refresh=False)]
    if command == "run_test":
        return [_step("run"), _step("test", allow_full_refresh=False)]
    # default / "build"
    return [_step("build")]
