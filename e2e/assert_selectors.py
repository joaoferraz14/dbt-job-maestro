"""Assert the selector-generation matrix produced what each config promised.

Portable across dbt projects: every expectation is derived from the artifacts and
the baseline selectors file rather than hardcoded counts, so this works on any
project rather than the one it was first written against.

Reads the files written by run_selector_matrix.sh into $MAESTRO_E2E_ARTIFACTS.

Env:
  MAESTRO_E2E_ARTIFACTS   dir holding selectors_<label>.yml   [e2e/artifacts]
  BASELINE_SELECTORS      the project's original selectors.yml (optional; when
                          set, manual-selector preservation is checked against it)
  MAESTRO_E2E_MANIFEST    manifest.json (optional; enables model-count checks)
"""

import json
import os
import sys

import yaml

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ART = os.environ.get("MAESTRO_E2E_ARTIFACTS", os.path.join(REPO, "e2e", "artifacts"))
BASELINE = os.environ.get("BASELINE_SELECTORS", "")
MANIFEST = os.environ.get("MAESTRO_E2E_MANIFEST", "")

results = []


def check(label, condition, detail=""):
    results.append((bool(condition), label, detail))
    print(f"  [{'PASS' if condition else 'FAIL'}] {label}" + (f"  -- {detail}" if detail else ""))


def load(label):
    path = os.path.join(ART, f"selectors_{label}.yml")
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return (yaml.safe_load(f) or {}).get("selectors", []) or []


def names(sels):
    return [s["name"] for s in sels]


def by_name(sels, name):
    return next((s for s in sels if s["name"] == name), None)


def methods_of(sel):
    """Method dicts inside a selector's positive (union/intersection) clauses."""
    found = []

    def walk(node):
        if isinstance(node, dict):
            if "method" in node:
                found.append(node)
            for key in ("union", "intersection"):
                for item in node.get(key, []) or []:
                    walk(item)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(sel.get("definition", {}))
    return found


def pairs_of(sel):
    return [(m.get("method"), m.get("value")) for m in methods_of(sel)]


def exclude_values(sel):
    found = []

    def walk(node):
        if isinstance(node, dict):
            if "exclude" in node:
                found.append(node["exclude"])
            for key in ("union", "intersection"):
                for item in node.get(key, []) or []:
                    walk(item)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(sel.get("definition", {}))
    return found


def exclude_shapes(sels):
    """dbt requires a list under `exclude`. Classify what was emitted."""
    shapes = set()
    for sel in sels:
        for value in exclude_values(sel):
            if isinstance(value, dict):
                shapes.add("dict")
            elif isinstance(value, list):
                if any(isinstance(v, dict) and "intersection" in v for v in value):
                    shapes.add("nested-int")
                else:
                    shapes.add("flat-list")
    return shapes


def auto(sels, prefix="maestro_"):
    return [s for s in sels if s["name"].startswith(prefix)]


def manual(sels, prefix="maestro_"):
    return [
        s
        for s in sels
        if not s["name"].startswith(prefix) and not s["name"].startswith("freshness_")
    ]


BASELINE_NAMES = set()
MANUAL_NAMES = set()
if BASELINE and os.path.exists(BASELINE):
    BASELINE_NAMES = set(names((yaml.safe_load(open(BASELINE)) or {}).get("selectors", []) or []))
    # A baseline captured from a project that already ran maestro contains
    # auto-generated names too. Those are legitimately dropped when a config does
    # not generate them, so only genuinely hand-written selectors are expected to
    # survive every run.
    MANUAL_NAMES = {
        n for n in BASELINE_NAMES if not n.startswith("maestro_") and not n.startswith("freshness_")
    }

MODEL_NAMES = set()
if MANIFEST and os.path.exists(MANIFEST):
    _m = json.load(open(MANIFEST))
    MODEL_NAMES = {
        v["name"] for v in _m.get("nodes", {}).values() if v.get("resource_type") == "model"
    }

BASE = load("01-baseline")
if BASE is None:
    print("no artifacts found - run e2e/run_selector_matrix.sh first")
    sys.exit(2)

print("\n=== 01-baseline: defaults, manual preservation ===")
check("produced selectors", len(BASE) > 0, f"{len(BASE)} selectors")
check("auto-generated selectors exist", len(auto(BASE)) > 0, f"{len(auto(BASE))} auto")
check("no duplicate selector names", len(names(BASE)) == len(set(names(BASE))))
check("every auto selector has a definition", all(s.get("definition") for s in auto(BASE)))
check(
    "every auto selector selects something",
    all(methods_of(s) for s in auto(BASE)),
    f"{sum(1 for s in auto(BASE) if not methods_of(s))} empty",
)
if MANUAL_NAMES:
    check(
        "all baseline manual selectors preserved",
        MANUAL_NAMES <= set(names(BASE)),
        f"missing={sorted(MANUAL_NAMES - set(names(BASE)))[:3]}",
    )
if MODEL_NAMES:
    referenced = {v for s in auto(BASE) for m, v in pairs_of(s) if m == "fqn"}
    check(
        "auto selectors reference only real models",
        referenced <= MODEL_NAMES | {"*"},
        f"unknown={sorted(referenced - MODEL_NAMES - {'*'})[:3]}",
    )

SPECIAL = load("02-special")
if SPECIAL:
    print("\n=== 02-special: seeds / snapshots / full-refresh / orphans ===")
    seeds = by_name(SPECIAL, "maestro_seeds")
    snaps = by_name(SPECIAL, "maestro_snapshots")
    fr = by_name(SPECIAL, "maestro_full_refresh_incremental")
    orph = by_name(SPECIAL, "maestro_orphan_models")
    check("maestro_seeds exists", seeds is not None)
    check("maestro_seeds selects by path", seeds and any(m == "path" for m, _ in pairs_of(seeds)))
    check("maestro_snapshots exists", snaps is not None)
    check(
        "maestro_snapshots selects by path", snaps and any(m == "path" for m, _ in pairs_of(snaps))
    )
    check("maestro_full_refresh_incremental exists", fr is not None)
    check(
        "full-refresh intersects fqn:* with config.materialized:incremental",
        fr
        and ("fqn", "*") in pairs_of(fr)
        and ("config.materialized", "incremental") in pairs_of(fr),
    )
    check("maestro_orphan_models exists (combine_single_model_selectors)", orph is not None)
    if orph:
        n = len([1 for m, _ in pairs_of(orph) if m == "fqn"])
        check("orphan selector bundles more than one model", n > 1, f"{n} fqn entries")
    check(
        "combining orphans yields fewer selectors than the baseline",
        len(SPECIAL) < len(BASE),
        f"{len(SPECIAL)} vs baseline {len(BASE)}",
    )

FQN = load("03-special-fqn")
if FQN:
    print("\n=== 03-special-fqn: seeds/snapshots via fqn method ===")
    s3 = by_name(FQN, "maestro_seeds")
    n3 = by_name(FQN, "maestro_snapshots")
    check(
        "seeds selector uses fqn entries, not path",
        s3 and pairs_of(s3) and all(m == "fqn" for m, _ in pairs_of(s3)),
    )
    check(
        "snapshots selector uses fqn entries, not path",
        n3 and pairs_of(n3) and all(m == "fqn" for m, _ in pairs_of(n3)),
    )
    check(
        "exactly one combined seeds selector",
        len([x for x in names(FQN) if x == "maestro_seeds"]) == 1,
    )

EX_U, EX_I = load("04-exclusions"), load("05-exclusions-intersection")
if EX_U and EX_I:
    print("\n=== 04 vs 05: exclusion_mode union vs intersection ===")
    su, si = exclude_shapes(auto(EX_U)), exclude_shapes(auto(EX_I))
    check("exclusions were emitted at all", su or si, f"union={su} intersection={si}")
    check("union mode emits a flat exclude list", su == {"flat-list"}, f"shapes={su}")
    check(
        "intersection mode nests criteria under one intersection entry",
        si == {"nested-int"},
        f"shapes={si}",
    )
    check("no exclude block is a dict (dbt requires a list)", "dict" not in su | si)

PFX = load("06-prefix-format")
if PFX:
    print("\n=== 06-prefix-format: custom prefix, raw manual, indirect_selection ===")
    orch = [n for n in names(PFX) if n.startswith("orch_")]
    check("auto selectors use the custom prefix orch_", len(orch) > 0, f"{len(orch)} orch_")
    # Under a custom prefix, any pre-existing maestro_* selector is a MANUAL
    # selector and must be preserved - so the only maestro_* names allowed are
    # ones that were already in the baseline.
    leaked = [n for n in names(PFX) if n.startswith("maestro_") and n not in BASELINE_NAMES]
    check(
        "no NEW default-prefix selectors were generated under a custom prefix",
        not leaked,
        f"leaked={leaked[:3]}",
    )
    auto_pfx = auto(PFX, "orch_")
    with_is = [
        s
        for s in auto_pfx
        if methods_of(s) and all(m.get("indirect_selection") == "cautious" for m in methods_of(s))
    ]
    check(
        "indirect_selection set on every method of every auto selector",
        auto_pfx and len(with_is) == len(auto_pfx),
        f"{len(with_is)}/{len(auto_pfx)}",
    )
    check(
        "indirect_selection NOT injected into manual selectors",
        not [
            s for s in manual(PFX, "orch_") if any("indirect_selection" in m for m in methods_of(s))
        ],
    )
    check(
        "indirect_selection never emitted as a root-level 'default' dict "
        "(dbt rejects the whole file)",
        not [s["name"] for s in PFX if isinstance(s.get("default"), dict)],
    )
    if MANUAL_NAMES:
        raw = open(os.path.join(ART, "selectors_06-prefix-format.yml")).read()
        sample = sorted(MANUAL_NAMES)[:5]
        check(
            "reformat_manual_selectors=false keeps manual text verbatim",
            all(f"- name: {n}\n" in raw for n in sample),
            f"checked {sample}",
        )

NOFRESH, ALLFRESH, WLFRESH = (
    load("07-freshness-removal"),
    load("08-freshness-all"),
    load("09-freshness-whitelist"),
)
if NOFRESH:
    print("\n=== 07 / 08 / 09: freshness lifecycle ===")
    check(
        "07 removes all auto freshness selectors",
        not [n for n in names(NOFRESH) if n.startswith("freshness_")],
    )
if ALLFRESH:
    f8 = [n for n in names(ALLFRESH) if n.startswith("freshness_")]
    a8 = auto(ALLFRESH)
    check(
        "08 empty whitelist gives every auto selector a freshness variant",
        len(f8) == len(a8),
        f"freshness={len(f8)} auto={len(a8)}",
    )
    check(
        "08 freshness names mirror their base selector",
        all(n[len("freshness_") :] in set(names(ALLFRESH)) for n in f8[:20]),
    )
if WLFRESH:
    f9 = sorted(n for n in names(WLFRESH) if n.startswith("freshness_"))
    check(
        "09 whitelist minus blacklist yields fewer than 'all'",
        not ALLFRESH or len(f9) < len([n for n in names(ALLFRESH) if n.startswith("freshness_")]),
        f"{len(f9)} freshness selectors: {f9[:3]}",
    )
    if f9:
        fsel = by_name(WLFRESH, f9[0])
        base_name = f9[0][len("freshness_") :]
        check(
            "freshness definition intersects its selector with source_status:fresher",
            fsel
            and ("source_status", "fresher") in pairs_of(fsel)
            and ("selector", base_name) in pairs_of(fsel),
        )

print("\n=== cross-config invariants ===")
for label in [
    "01-baseline",
    "02-special",
    "03-special-fqn",
    "04-exclusions",
    "05-exclusions-intersection",
    "06-prefix-format",
    "07-freshness-removal",
    "08-freshness-all",
    "09-freshness-whitelist",
]:
    sels = load(label)
    if not sels:
        continue
    dupes = [n for n in set(names(sels)) if names(sels).count(n) > 1]
    check(f"{label}: unique selector names", not dupes, f"dupes={dupes[:3]}")
    check(f"{label}: no exclude block is a dict", "dict" not in exclude_shapes(sels))
    if MANUAL_NAMES and label != "06-prefix-format":
        check(
            f"{label}: baseline manual selectors preserved",
            MANUAL_NAMES <= set(names(sels)),
            f"missing={sorted(MANUAL_NAMES - set(names(sels)))[:3]}",
        )

failed = [r for r in results if not r[0]]
print(f"\n{'='*70}\nRESULT: {len(results)-len(failed)}/{len(results)} assertions passed")
if failed:
    print("FAILED:")
    for _, label, detail in failed:
        print(f"  - {label}  {detail}")
sys.exit(1 if failed else 0)
