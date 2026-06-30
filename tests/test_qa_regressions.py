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


# --------------------------------------------------------------------------- F2
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
