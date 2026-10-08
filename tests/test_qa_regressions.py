"""Regression tests from the QA campaign.

Covers three bugs found while validating every CLI arg / config option against a
real dbt manifest:

* F1 - the suite was not isolated from the current working directory: any folder
  containing a ``selectors.yml`` (e.g. a dbt project root) poisoned tests via
  ManualSelector's cwd lookup. Fixed by the autouse fixture in conftest.py.
* F2 - duplicate selector names when a model is literally named like a reserved
  selector (the dbt_artifacts package ships models named ``seeds``/``snapshots``),
  which collided with the dedicated seeds/snapshots selectors.
* F3 - isolated models were double-covered: each got its own ``maestro_<name>``
  selector AND was also bundled into ``selector_independent`` (isolated nodes are
  reported both as size-1 connected components and as independent models).
"""

import dataclasses
import json
import os
import tempfile

import pytest

from dbt_job_maestro.manifest_parser import ManifestParser
from dbt_job_maestro.graph_builder import GraphBuilder
from dbt_job_maestro.selectors.fqn_selector import FQNSelector
from dbt_job_maestro.selector_orchestrator import SelectorOrchestrator
from dbt_job_maestro.config import Config, SelectorConfig
from dbt_job_maestro.model_resolver import ModelResolver


def _write_manifest(nodes):
    f = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
    json.dump({"nodes": nodes}, f)
    f.close()
    return f.name


def _model(name, fqn_folder, depends=None, resource_type="model", tags=None):
    return {
        "name": name,
        "fqn": ["project", fqn_folder, name],
        "path": f"{fqn_folder}/{name}.sql",
        "original_file_path": f"models/{fqn_folder}/{name}.sql",
        "tags": tags or [],
        "resource_type": resource_type,
        "depends_on": {"nodes": depends or []},
    }


# --------------------------------------------------------------------------- F3
class TestIndependentModelsNotDoubleCovered:
    """F3: an isolated model must appear in exactly one auto-generated selector."""

    @pytest.fixture
    def graph_with_isolated(self):
        nodes = {
            # multi-model component: stg_a -> fct_b
            "model.project.stg_a": _model("stg_a", "staging"),
            "model.project.fct_b": _model("fct_b", "marts", depends=["model.project.stg_a"]),
            # two fully isolated models
            "model.project.iso_one": _model("iso_one", "misc"),
            "model.project.iso_two": _model("iso_two", "misc"),
        }
        path = _write_manifest(nodes)
        parser = ManifestParser(path)
        return parser, GraphBuilder(parser.get_models())

    def test_isolated_model_has_single_selector(self, graph_with_isolated):
        parser, graph = graph_with_isolated
        config = SelectorConfig(group_by_dependencies=True, combine_single_model_selectors=False)
        selectors = FQNSelector(parser, graph, config).generate(excluded_models=set())

        names = [s["name"] for s in selectors]
        # Each isolated model gets its own selector ...
        assert "maestro_iso_one" in names
        assert "maestro_iso_two" in names
        # ... and the redundant catch-all is NOT also created.
        assert "selector_independent" not in names

        # No model is covered by more than one selector.
        blob = json.dumps(selectors)
        assert blob.count('"value": "iso_one"') == 1
        assert blob.count('"value": "iso_two"') == 1

    def test_genuinely_independent_only_model_still_covered(self):
        """A model that is independent but not a single-model component is kept.

        Defensive: if the two graph notions ever diverge, independent models that
        did not receive their own selector must still land in selector_independent.
        """
        nodes = {"model.project.solo": _model("solo", "misc")}
        path = _write_manifest(nodes)
        parser = ManifestParser(path)
        graph = GraphBuilder(parser.get_models())
        config = SelectorConfig(group_by_dependencies=True, combine_single_model_selectors=False)
        selectors = FQNSelector(parser, graph, config).generate(excluded_models=set())
        blob = json.dumps(selectors)
        assert blob.count('"value": "solo"') == 1  # covered exactly once


# ------------------------------------------------ existing_selectors_path plumbing
class TestExistingSelectorsPathPlumbing:
    """The orchestrator must forward existing_selectors_path to ManualSelector.

    Regression: the param existed on SelectorOrchestrator/ManualSelector but was
    never forwarded (orchestrator built ManualSelector without it, CLI never
    passed it), so manual-selector preservation silently fell back to a cwd
    lookup for 'selectors.yml'. This proves preservation works for a custom
    filename in a directory that is NOT the cwd.
    """

    def test_manual_selector_preserved_from_custom_path(self, tmp_path):
        # conftest._isolated_cwd has chdir'd us to an empty tmp dir, so there is
        # no 'selectors.yml' in cwd. Put the existing selectors somewhere else
        # entirely, under a non-default filename.
        nodes = {"model.project.x": _model("x", "marts")}
        parser = ManifestParser(_write_manifest(nodes))
        graph = GraphBuilder(parser.get_models())

        custom = tmp_path / "nested" / "my_selectors.yml"
        custom.parent.mkdir(parents=True)
        custom.write_text(
            "selectors:\n"
            "  - name: my_manual_pipeline\n"
            "    description: hand written\n"
            "    definition:\n"
            "      method: fqn\n"
            "      value: x\n"
        )

        orch = SelectorOrchestrator(
            parser, graph, SelectorConfig(), existing_selectors_path=str(custom)
        )
        selectors = orch.generate_selectors()
        names = [s["name"] for s in selectors]
        assert "my_manual_pipeline" in names, (
            "manual selector at a custom path was not preserved; "
            "existing_selectors_path is not being forwarded to ManualSelector"
        )

    def test_no_existing_path_and_no_cwd_file_yields_no_manual(self, tmp_path):
        """Control: with no path and no cwd selectors.yml, nothing is preserved."""
        nodes = {"model.project.x": _model("x", "marts")}
        parser = ManifestParser(_write_manifest(nodes))
        graph = GraphBuilder(parser.get_models())
        orch = SelectorOrchestrator(parser, graph, SelectorConfig())
        names = [s["name"] for s in orch.generate_selectors()]
        assert "my_manual_pipeline" not in names


@pytest.mark.parametrize(
    "config",
    [
        SelectorConfig(exclude_models=["middle"]),
        SelectorConfig(exclude_tags=["skip"]),
        SelectorConfig(exclude_paths=["manual"]),
    ],
    ids=["model", "tag", "path"],
)
def test_config_exclusions_remove_models_after_grouping(tmp_path, config):
    nodes = {
        "model.project.upstream": _model("upstream", "staging"),
        "model.project.middle": _model(
            "middle", "manual", depends=["model.project.upstream"], tags=["skip"]
        ),
        "model.project.downstream": _model("downstream", "marts", depends=["model.project.middle"]),
    }
    parser = ManifestParser(_write_manifest(nodes))
    orchestrator = SelectorOrchestrator(
        parser,
        GraphBuilder(parser.get_models()),
        config,
        existing_selectors_path=str(tmp_path / "selectors.yml"),
    )
    selectors = orchestrator.generate_selectors()
    assert len(selectors) == 1
    assert selectors[0]["definition"] == {
        "union": [
            {"method": "fqn", "value": "upstream"},
            {"method": "fqn", "value": "downstream"},
            {
                "exclude": (
                    [{"method": "fqn", "value": "middle"}]
                    if config.exclude_models
                    else (
                        [{"method": "tag", "value": "skip"}]
                        if config.exclude_tags
                        else [{"method": "path", "value": "manual"}]
                    )
                )
            },
        ]
    }
    assert orchestrator.model_coverage.fully_excluded == {"middle"}
    output = tmp_path / "generated.yml"
    orchestrator.write_selectors(selectors, str(output))
    assert "exclude:" in output.read_text()


def test_unmatched_config_model_exclusion_warns(tmp_path, caplog):
    parser = ManifestParser(_write_manifest({"model.project.x": _model("x", "marts")}))
    orchestrator = SelectorOrchestrator(
        parser,
        GraphBuilder(parser.get_models()),
        SelectorConfig(exclude_models=["model.project.x"]),
        existing_selectors_path=str(tmp_path / "selectors.yml"),
    )
    orchestrator.generate_selectors()
    assert "exclude_models entries did not match manifest model names" in caplog.text
    assert orchestrator.model_coverage.selected == {"x"}


def test_config_exclusions_never_modify_manual_selector(tmp_path):
    parser = ManifestParser(
        _write_manifest(
            {
                "model.project.manual": _model("manual", "marts", tags=["skip"]),
                "model.project.auto": _model("auto", "marts"),
            }
        )
    )
    manual = {
        "name": "hand_written",
        "definition": {"union": [{"method": "fqn", "value": "manual"}]},
    }
    output = tmp_path / "selectors.yml"
    output.write_text(json.dumps({"selectors": [manual]}))
    orchestrator = SelectorOrchestrator(
        parser,
        GraphBuilder(parser.get_models()),
        SelectorConfig(exclude_tags=["skip"], exclude_models=["manual"]),
        existing_selectors_path=str(output),
    )
    selectors = orchestrator.generate_selectors()
    assert next(s for s in selectors if s["name"] == "hand_written") == manual
    automatic = next(s for s in selectors if s["name"].startswith("maestro_"))
    assert automatic["definition"]["union"] == [
        {"method": "fqn", "value": "auto"},
        {
            "exclude": [
                {"method": "tag", "value": "skip"},
                {"method": "fqn", "value": "manual"},
            ]
        },
    ]
    assert orchestrator.model_coverage.fully_excluded == set()


def test_advanced_runtime_exclusion_intersection_is_emitted(tmp_path):
    rules = [
        {
            "intersection": [
                {"method": "config.materialized", "value": "view"},
                {"method": "state", "value": "unmodified"},
                {"method": "state", "value": "old"},
            ]
        }
    ]
    config = SelectorConfig(exclude_rules=rules)
    config.validate()
    parser = ManifestParser(_write_manifest({"model.project.x": _model("x", "marts")}))
    orchestrator = SelectorOrchestrator(
        parser,
        GraphBuilder(parser.get_models()),
        config,
        existing_selectors_path=str(tmp_path / "selectors.yml"),
    )
    selectors = orchestrator.generate_selectors()
    assert selectors[0]["definition"]["union"] == [
        {"method": "fqn", "value": "x"},
        {"exclude": rules},
    ]
    assert orchestrator.model_coverage.unsupported_methods == {"state", "config.materialized"}
    assert config.exclude_rules == rules
    output = tmp_path / "selectors.yml"
    orchestrator.write_selectors(selectors, str(output))
    assert "value: unmodified" in output.read_text()
    assert "value: view" in output.read_text()


@pytest.mark.parametrize(
    "rules",
    [
        "state:old",
        [{"intersection": []}],
        [{"method": "state", "value": "invalid"}],
        [{"method": "config.materialized", "value": None}],
        [{"method": "invalid", "value": "view"}],
    ],
)
def test_invalid_advanced_exclusions_are_rejected(rules):
    with pytest.raises(ValueError):
        SelectorConfig(exclude_rules=rules).validate()


def test_runtime_intersection_is_accepted_by_dbt_parser(monkeypatch):
    dbt_cli = pytest.importorskip("dbt.graph.cli")
    from dbt.flags import get_flags

    monkeypatch.setattr(get_flags(), "INDIRECT_SELECTION", "eager", raising=False)
    rules = [
        {
            "intersection": [
                {"method": "config.materialized", "value": "view"},
                {"method": "state", "value": "unmodified"},
                {"method": "state", "value": "old"},
            ]
        }
    ]
    config = SelectorConfig(exclude_rules=rules)
    parser = ManifestParser(_write_manifest({"model.project.x": _model("x", "marts")}))
    selector = FQNSelector(parser, GraphBuilder(parser.get_models()), config).generate(set())[0]
    spec = dbt_cli.parse_from_definition(selector["definition"], {})
    assert type(spec).__name__ == "SelectionDifference"


def test_unchanged_selector_write_preserves_file_mtime(tmp_path):
    parser = ManifestParser(_write_manifest({"model.project.x": _model("x", "marts")}))
    orchestrator = SelectorOrchestrator(
        parser,
        GraphBuilder(parser.get_models()),
        SelectorConfig(),
        existing_selectors_path=str(tmp_path / "selectors.yml"),
    )
    selectors = orchestrator.generate_selectors()
    output = tmp_path / "selectors.yml"
    orchestrator.write_selectors(selectors, str(output))
    os.utime(output, ns=(1_000_000_000, 1_000_000_000))
    original = output.read_bytes()
    orchestrator.write_selectors(selectors, str(output))
    assert output.stat().st_mtime_ns == 1_000_000_000
    assert output.read_bytes() == original

    selectors[0]["description"] = "Updated selector description"
    orchestrator.write_selectors(selectors, str(output))
    assert "Updated selector description" in output.read_text()
    assert output.stat().st_mtime_ns != 1_000_000_000


@pytest.mark.parametrize(
    "manual_selection",
    [
        {"method": "fqn", "value": "middle"},
        {"method": "tag", "value": "manual"},
        {"method": "path", "value": "manual"},
    ],
    ids=["fqn", "tag", "path"],
)
def test_manual_model_exclusion_preserves_complete_automated_grouping(tmp_path, manual_selection):
    nodes = {
        "model.project.upstream": _model("upstream", "staging"),
        "model.project.middle": _model(
            "middle", "manual", depends=["model.project.upstream"], tags=["manual"]
        ),
        "model.project.downstream": _model("downstream", "marts", depends=["model.project.middle"]),
    }
    parser = ManifestParser(_write_manifest(nodes))
    graph = GraphBuilder(parser.get_models())
    existing = tmp_path / "selectors.yml"
    existing.write_text(
        json.dumps(
            {
                "selectors": [
                    {
                        "name": "manual_middle",
                        "definition": {"union": [manual_selection]},
                    }
                ]
            }
        )
    )
    orchestrator = SelectorOrchestrator(
        parser,
        graph,
        SelectorConfig(),
        existing_selectors_path=str(existing),
    )

    selectors = orchestrator.generate_selectors()

    automatic = [selector for selector in selectors if selector["name"].startswith("maestro_")]
    assert len(automatic) == 1
    automatic_selector = automatic[0]
    assert automatic_selector["definition"] == {
        "union": [
            {"method": "fqn", "value": "upstream"},
            {"method": "fqn", "value": "downstream"},
        ]
    }
    positive_fqns = {
        item["value"]
        for item in automatic_selector["definition"]["union"]
        if item.get("method") == "fqn"
    }
    assert positive_fqns == {"upstream", "downstream"}
    assert ModelResolver(parser, graph).resolve_selector(automatic_selector).models == {
        "upstream",
        "downstream",
    }
    assert ModelResolver(parser, graph).resolve_selector(
        next(selector for selector in selectors if selector["name"] == "manual_middle")
    ).models == {"middle"}


# --------------------------------------------------------------------------- F2
@pytest.mark.parametrize(
    "include_other_selector, expected_selected, expected_excluded, expected_gaps",
    [
        (False, {"upstream"}, {"middle"}, {"downstream"}),
        (True, {"upstream", "middle"}, set(), {"downstream"}),
    ],
)
def test_model_coverage_distinguishes_exclusions_from_gaps(
    tmp_path, include_other_selector, expected_selected, expected_excluded, expected_gaps
):
    nodes = {
        "model.project.upstream": _model("upstream", "staging"),
        "model.project.middle": _model("middle", "staging", tags=["skip"]),
        "model.project.downstream": _model("downstream", "marts"),
    }
    parser = ManifestParser(_write_manifest(nodes))
    orchestrator = SelectorOrchestrator(parser, GraphBuilder(parser.get_models()), SelectorConfig())
    selectors = [
        {
            "name": "manual_staging",
            "definition": {
                "union": [
                    {"method": "path", "value": "staging"},
                    {"exclude": [{"method": "tag", "value": "skip"}]},
                ]
            },
        }
    ]
    if include_other_selector:
        selectors.append(
            {
                "name": "maestro_middle",
                "definition": {"union": [{"method": "fqn", "value": "middle"}]},
            }
        )
    coverage = orchestrator.get_model_coverage(selectors)
    assert coverage.total == 3
    assert coverage.selected == expected_selected
    assert coverage.fully_excluded == expected_excluded
    assert coverage.unexplained == expected_gaps
    assert len(coverage.selected | coverage.fully_excluded | coverage.unexplained) == 3


def test_model_coverage_does_not_claim_verification_for_unsupported_methods():
    parser = ManifestParser(_write_manifest({"model.project.x": _model("x", "marts")}))
    orchestrator = SelectorOrchestrator(parser, GraphBuilder(parser.get_models()), SelectorConfig())
    coverage = orchestrator.get_model_coverage(
        [
            {
                "name": "manual_reference",
                "definition": {"union": [{"method": "selector", "value": "other"}]},
            }
        ]
    )
    assert coverage.unsupported_methods == {"selector"}
    assert coverage.unexplained == {"x"}


class TestUniqueSelectorNames:
    """F2: generated selectors must never share a name."""

    def test_ensure_unique_names_renames_collisions(self):
        nodes = {"model.project.x": _model("x", "misc")}
        parser = ManifestParser(_write_manifest(nodes))
        orch = SelectorOrchestrator(parser, GraphBuilder(parser.get_models()), SelectorConfig())
        selectors = [
            {"name": "maestro_seeds", "definition": {}},
            {"name": "maestro_seeds", "definition": {}},
            {"name": "maestro_seeds", "definition": {}},
            {"name": "maestro_other", "definition": {}},
        ]
        orch._ensure_unique_names(selectors)
        names = [s["name"] for s in selectors]
        assert names == ["maestro_seeds", "maestro_seeds_2", "maestro_seeds_3", "maestro_other"]
        assert len(names) == len(set(names))

    def test_model_named_like_reserved_seed_selector(self):
        """A model literally named 'seeds' + include_seeds must not duplicate names."""
        nodes = {
            "model.project.seeds": _model("seeds", "sources"),  # collides with reserved name
            "seed.project.real_seed": _model("real_seed", "seeds", resource_type="seed"),
            "model.project.normal": _model("normal", "marts"),
        }
        parser = ManifestParser(_write_manifest(nodes))
        graph = GraphBuilder(parser.get_models())
        config = SelectorConfig(include_seeds_selectors=True, seeds_selector_method="path")
        selectors = SelectorOrchestrator(parser, graph, config).generate_selectors()
        names = [s["name"] for s in selectors]
        assert len(names) == len(set(names)), f"duplicate names: {names}"
        # both the model-derived and the dedicated seeds selector survive (disambiguated)
        assert sum(n.startswith("maestro_seeds") for n in names) >= 2


# --------------------------------------------------------------------------- F1
class TestCwdIsolation:
    """F1: tests must not pick up a selectors.yml from the working directory."""

    def test_autouse_fixture_changes_cwd(self, tmp_path):
        # conftest._isolated_cwd has chdir'd us into a pytest tmp dir, so no
        # stray selectors.yml from a project root can leak in.
        assert "selectors.yml" not in os.listdir(os.getcwd())


# ---------------------------------------------------------------- config round-trip
class TestConfigRoundTrip:
    """to_yaml -> from_yaml must preserve every option (hand-rolled template)."""

    def _diff(self, a, b, path=""):
        out = []
        if dataclasses.is_dataclass(a) and dataclasses.is_dataclass(b):
            for f in dataclasses.fields(a):
                out += self._diff(getattr(a, f.name), getattr(b, f.name), f"{path}.{f.name}")
        elif a != b:
            out.append(f"{path}: {a!r} != {b!r}")
        return out

    def test_defaults_round_trip(self, tmp_path):
        p = str(tmp_path / "c.yml")
        c1 = Config()
        c1.to_yaml(p)
        assert self._diff(c1, Config.from_yaml(p)) == []

    def test_customized_round_trip(self, tmp_path):
        p = str(tmp_path / "c.yml")
        c = Config()
        c.selector.exclude_tags = ["a", "b"]
        c.selector.exclusion_mode = "intersection"
        c.selector.combine_single_model_selectors = True
        c.job.account_id = 111
        c.job.orchestration_mode = "staggered"
        c.job.cron_days_of_week = ["MON", "FRI"]
        c.job.min_models_per_job = 5
        c.airflow.dags_dir = "dags/"
        c.airflow.min_models_per_dag = 85
        c.airflow.tags = ["x", "y"]
        c.airflow.full_refresh.enabled = True
        c.to_yaml(p)
        assert self._diff(c, Config.from_yaml(p)) == []
