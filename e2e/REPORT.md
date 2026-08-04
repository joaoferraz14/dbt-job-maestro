# End-to-end validation report — selector generation + Airflow

**Scope:** selector generation, Airflow DAG generation, live Airflow import + execution,
bounded warehouse materialization · **Out of scope:** dbt Cloud `generate-jobs` (generated,
never synced or run)

The validation ran against a large real-world dbt project (**2740 models, 143 seeds,
13 snapshots, 727 sources, 74 hand-written selectors**) on Snowflake. That project is not part
of this repo and is not named here; the harness in `e2e/` is parameterised so the same matrix
runs against any dbt project. Where a number below is project-specific it is labelled as
"the test project".

---

## 1. Executive summary

Selector generation and the Airflow path **now work end to end** — but only after fixing
**four defects that each made a documented config option produce unusable output**. Three of
the four were total failures, not degradations: they broke every selector or every DAG in the
file, not just the affected one.

| | Verdict |
|---|---|
| **Selector generation** | **PASS — after 3 fixes.** Was FAIL: `exclude_tags`/`exclude_paths`/`exclude_models`, `full_refresh_exclude_*`, and `indirect_selection` each produced a `selectors.yml` that **dbt refuses to parse**. |
| **Airflow orchestration** | **PASS — after 2 fixes.** Was FAIL on Airflow 3 (100% of DAGs failed import) and FAIL whenever `output_dir` was set (`generate-dags` could not find its input). |

Evidence: **306 unit tests** pass (up from 275 collected / 3 failing before), selector-matrix
and DAG-matrix assertions all green, every generated selector file accepted by real dbt,
**9/9 DAG variants loaded into real Airflow with zero import errors** (on both 2.10.5 and
3.2.2), and **8/8 bounded DAG runs** materializing **7/7 expected relations** in the warehouse.

The headline risk: before these fixes, a team enabling exclusions or non-eager indirect
selection got a `selectors.yml` that bricked **every** dbt invocation in the project, and a
team on Airflow 3 got **zero** working DAGs. Both fail loudly and immediately, so neither is a
silent-corruption risk.

---

## 2. Findings

Severity: **P0** = produces unusable output; **P1** = wrong results or broken command;
**P2** = maintainability / ergonomics.

### P0-1 · Airflow 3: every generated DAG fails to import · **FIXED**

`airflow_dag_generator.py` emitted `DAG(schedule_interval=...)`, removed in Airflow 3.0.

```
TypeError: DAG.__init__() got an unexpected keyword argument 'schedule_interval'
```

Measured on Airflow 3.2.2 against a 155-DAG variant: **0 DAGs loaded, 155 import errors.** Two
further Airflow-3 breakages in the same template: `default_args["sla"]` (task SLAs removed in
3.0) and the deprecated `airflow.operators.bash` import.

This was **already failing this repo's own test suite** — 3 tests in `TestAirflowDAGRendering`
were red whenever Airflow 3 was the installed version.

**Fix:** added `airflow.airflow_version: 2|3` (default `2`). For `3` the generator emits
`schedule=`, imports `airflow.providers.standard.operators.bash`, and omits `sla`. Verified
**byte-identical v2 output across 10 scenarios** so existing users see zero churn. The 3 red
tests now target whichever Airflow is installed and pass on both 2.10.5 and 3.2.2.

### P0-2 · Any exclusion config makes dbt reject the whole selectors.yml · **FIXED**

Maestro wrote `exclude:` as a **dict**; dbt requires a **list**.

```
Runtime Error
  Could not read selector file data: Invalid value for key "exclude". Expected a list.
```

Triggered by `exclude_tags`, `exclude_paths`, `exclude_models`, and all three
`full_refresh_exclude_*`. The failure is file-wide: **every** selector becomes unusable,
including untouched hand-written ones.

Empirically determined correct forms (node counts from the test project):

| Form | dbt |
|---|---|
| `exclude: {union: [...]}` — what maestro emitted | **REJECTED** |
| `exclude: [{method…}, …]` | accepted — 4657 nodes (union semantics) |
| `exclude: [{intersection: [...]}]` | accepted — 4972 nodes (intersection semantics) |

The differing counts confirm `exclusion_mode` is meaningful: a dbt `exclude` list is implicitly
a union, so intersection semantics require the nested wrapper.

**Fix:** `fqn_selector._create_exclusion` emits a flat list for `union`, and a single nested
`{"intersection": [...]}` entry for `intersection` (skipping the wrapper for a single
criterion, where AND-of-one is itself). `_generate_full_refresh_selector` likewise emits a list.

### P0-3 · `indirect_selection` makes dbt reject the whole selectors.yml · **FIXED**

Written as a root-level `default: {indirect_selection: cautious}`:

```
Could not parse selector file data:
  indirect_selection: cautious
Valid root-level selector definitions: union, intersection, string, dictionary.
```

`default` is dbt's **boolean** "use this selector when none is specified" flag, and real
projects use it (the test project sets it to a Jinja expression on one selector). Maestro
overloaded the same key with a dict on every auto selector, so any `indirect_selection` other
than `eager` bricked the file. **`indirect_selection` had zero test coverage**, which is why
this shipped.

**Fix:** new `SelectorOrchestrator._apply_indirect_selection` sets `indirect_selection` on each
selector **method inside `definition`** (dbt's documented location), leaving `exclude` blocks
alone. Verified with `dbt parse`. 7 new tests added.

### P1-4 · `output_dir` silently breaks `generate-dags` and `generate-jobs` · **FIXED**

`generate` writes to `output_dir/selectors_output_file`, but both downstream commands opened
the bare filename relative to **cwd**:

```
✗ Error: [Errno 2] No such file or directory: 'selectors.yml'
```

Any project setting `output_dir` gets 0 DAGs unless run from that directory. **Fix:** shared
`cli._resolve_output_path` resolves against `output_dir`, honours absolute paths, and treats an
explicit `--selectors`/`--output` flag as verbatim.

### P1-5 · Resolver ignored `exclude` entries listed beside their siblings · **FIXED**

A bare `- exclude:` inside a `union` list applies to the whole union, but `resolve_selector`
handed it to `_resolve_item`, which subtracted it from an **empty local set** — a silent no-op.
Since this is how dbt, maestro, and hand-written selectors all express exclusions,
manual-selector coverage was **over-reported**, wrongly excluding models from auto-generation.

Impact on the test project: baseline generation went **387 → 388 selectors**; computed coverage
1193 → 1192 models. `_resolve_exclusions` now also accepts both list and legacy dict shapes, so
a stale file keeps resolving identically.

### P1-6 · `ModelResolver` only understands `fqn` and `path` · **NOT FIXED (reported)**

**28 of the test project's 74** hand-written selectors resolve to **0 models** because they use
`tag:`, `config.materialized:`, `source_status:`, `state:` etc. Consequences:

- their models are **not** excluded from auto-generation → double coverage;
- `selector_types.count_fqn_models` under-counts them → `min_models_per_dag` mis-partitions.

Cross-check: `dbt ls --selector <name>` resolved 2 models for a selector where maestro's
resolver reported 0 and warned about 2 "nonexistent" models — resolver and dbt disagree on the
same file.

**Recommended:** resolve `tag` and `config.*` from the manifest (both already available in
`ManifestParser`), and until then emit a **warning** when a manual selector resolves to 0
models — currently silent, so the miscount is invisible.

### P2-7 · Three dead duplicate methods · **REMOVED**

`_create_exclusion`, `_create_freshness_selector`, `_should_create_freshness` existed in
**both** `selector_orchestrator.py` and `selectors/fqn_selector.py`. Only the `fqn_selector`
copies are reachable, and the two had already diverged (`_create_freshness_selector` takes
`(base_name)` in one and `(base_name, models)` in the other). The orchestrator's
`_create_exclusion` still emitted the P0-2 broken shape — a live trap for anyone fixing the
wrong copy. Removed the 3 unreachable methods.

### P2-8 · No way to configure the dbt executable · **NOT FIXED (reported)**

Generated `bash_command`s hardcode a bare `dbt`, so an Airflow worker without dbt on `PATH`
fails, and a host where a different dbt distribution comes first on `PATH` silently runs the
wrong engine (hit during this validation: a preview build of dbt shadowed dbt-core).
**Recommended:** add `airflow.dbt_executable_path` (default `dbt`). `scripts/maestro_airflow_up.sh`
works around it by putting the chosen dbt's directory first on `PATH`.

### P2-9 · Freshness whitelist silently matches nothing · **NOT FIXED (reported)**

`freshness_selector_names` is matched against **auto-generated** selector names. Naming
*manual* selectors produces **zero** freshness selectors with no warning — it cost a full
config cycle to notice. **Recommended:** warn when a whitelist entry matches no generated
selector.

### P2-10 · Manual DAGs consume staggered slots · behavioral, reported

`dag_index` advances for every DAG, but manual DAGs take `manual_schedule_interval` instead of
their slot, so the staggered sequence has gaps (`06:00, 06:07, 09:23, …`). Not a correctness
bug — every auto cron sits on the configured grid with **no collisions** — but the cadence is
not the contiguous ramp the config implies.

### P2-11 · Pre-existing lint failure · fixed incidentally

`tests/test_selector_commands.py` failed `black --check` before any of this work, so the lint
workflow was already red. Reformatted — the only change in the patch not tied to a finding.

### Operational note · `dbt_target` must match the project's schema-naming contract

Many dbt projects switch schema naming on `target.name` (a `generate_schema_name` macro keyed
to `dev`/`ci` is a common pattern, with a fall-through `else` that returns the bare production
schema name). During this validation a custom target name hit that `else` branch and wrote 7
relations into shared, production-named schemas of the **dev** database instead of the
developer's personal schemas. Nothing pre-existing was overwritten and all 7 were dropped, but
the placement was invisible to a schema-prefix check.

Because maestro is what injects `--target` into generated commands, this is worth documenting:
**use a target name the project actually expects.** `scripts/maestro_airflow_up.sh` calls this
out in its `--target` help text.

---

## 3. What was tested

### 3a. Selector generation — 9 configs

Templates in `e2e/configs/`, driven by `e2e/run_selector_matrix.sh`, asserted by
`e2e/assert_selectors.py`.

| Config | Exercises | Result |
|---|---|---|
| `01-baseline` | defaults, manual preservation, overlap detection | PASS · 74 manual + 314 auto = 388 selectors; 496 overlap warnings surfaced |
| `02-special` | seeds + snapshots by `path`, full-refresh selector with all 3 exclusion kinds, orphan combining | PASS · **exposed P0-2** |
| `03-special-fqn` | seeds/snapshots via `fqn` method | PASS · one combined selector each |
| `04-exclusions` | `exclude_tags`/`paths`/`models`, `exclusion_mode: union` | PASS after P0-2 fix · flat exclude list |
| `05-exclusions-intersection` | `exclusion_mode: intersection` | PASS after P0-2 fix · nested `intersection` entry |
| `06-prefix-format` | custom `selector_prefix`, `reformat_manual_selectors: false`, `indirect_selection: cautious`, `prefix_order` | PASS after P0-3 fix · manual blocks verbatim |
| `07-freshness-removal` | `include_freshness_selectors: false` after `02` | PASS · all auto freshness removed |
| `08-freshness-all` | empty whitelist ⇒ all | PASS · one freshness selector per auto selector (79/79) |
| `09-freshness-whitelist` | whitelist + blacklist | PASS · blacklist takes priority |

Also verified: `maestro init` output round-trips through `Config.from_yaml`; `maestro info`
reports the right model count; selector names unique in every config; all hand-written
selectors preserved in every config.

**dbt acceptance** (`dbt ls --selector`, read-only): every generated file accepted post-fix.

### 3b. Airflow DAG generation — 9 configs

Driven by `e2e/run_dag_matrix.sh`, asserted by `e2e/assert_dags.py`.

| Config | Exercises | Result |
|---|---|---|
| `10-af-simple` | `simple` mode, one DAG per selector, runtime flags | PASS · every command carries `--project-dir/--profiles-dir/--target/--threads` |
| `11-af-staggered` | `staggered`, increments, `cron_days_of_week`, SLAs, `execution_order` | PASS · crons on the grid, no collisions; seeds/snapshots before models |
| `12-af-none` | `orchestration_mode: none` | PASS · all `schedule_interval=None`, no SLA |
| `13-af-combined-failfast` | `min_models_per_dag: 5`, `continue_on_failure: false` | PASS · combined DAG = 68 tasks / **67 chain links** |
| `14-af-combined-continue` | `continue_on_failure: true` | PASS · same 68 tasks / **0 chain links** |
| `15-af-commands` | `default_command: run_test` + manual overrides | PASS · auto gets `run >> test` and **ignores** its override; manual overrides honoured; seeds/snapshots keep their verbs |
| `16-af-fullrefresh` | auto full-refresh + all 4 `custom_schedules` shapes + seeds full-refresh | PASS · one DAG per schedule, each with its own cron and correct `--full-refresh` command |
| `17-af-legacy` | legacy `orchestration_mode: dependency` | PASS · normalized to `simple` |
| `18-af-v3` | `airflow_version: 3` | PASS · `schedule=`, provider import, no `sla` |

Selector names that are not valid Python identifiers (hyphens, dots) render valid variable
names via `_safe_var`. Every generated file byte-compiles.

**Idempotency** (`e2e/assert_idempotency.py`) — over a dirty dags dir: stale auto DAG deleted,
legacy single-DAG file deleted, hand-written DAG **byte-identical and retained**. Two further
regenerations byte-stable with nothing removed. Regenerating from 155 selectors down to 2
removed exactly 153 and still preserved the hand-written file.

### 3c. Live Airflow import validation — 9/9 variants, 0 errors

`e2e/validate_airflow_imports.sh` loads each variant into a real `DagBag`.

| Variant | Airflow | Files | DAGs | Errors |
|---|---|---|---|---|
| 10, 11, 12, 15, 17 | 2.10.5 | 155 | 155 | 0 |
| 13, 14 (combined) | 2.10.5 | 88 | 88 | 0 |
| 16 (full-refresh) | 2.10.5 | 161 | 161 | 0 |
| 18 (`airflow_version: 3`) | 3.2.2 | 155 | 155 | 0 |
| **10 on Airflow 3 — the defect** | 3.2.2 | 155 | **0** | **155** |

`continue_on_failure` confirmed through Airflow's own parsed graph, not just generated text:

| Variant | Tasks | Edges | Roots | Leaves |
|---|---|---|---|---|
| fail-fast | 68 | 67 | 1 | 1 |
| continue | 68 | **0** | 68 | 68 |

So fail-fast marks all downstream `upstream_failed`; with `continue_on_failure` no sibling can
be skipped. (The skip behaviour is Airflow's; what maestro controls is whether the edge exists.)

### 3d. Bounded execution — 8/8 runs, 7/7 relations

Run via `airflow dags test` on Airflow 2.10.5, invoking real dbt against a dev warehouse.
Every command verb got a success case:

| Command | Tasks | dbt result |
|---|---|---|
| `dbt seed --selector …` | 1 OK | PASS=4 ERROR=0 · 100 rows inserted |
| `dbt snapshot --selector …` | 1 OK | PASS=4 ERROR=0 |
| `dbt run` **then** `dbt test` (run_test) | **2 OK** | PASS=5 ERROR=0 |
| `dbt build --selector …` | 1 OK | PASS=11 ERROR=0 |
| `dbt run --selector …` | 1 OK | PASS=5 ERROR=0 |
| `dbt test --selector …` | 1 OK | — |
| `dbt build --full-refresh --selector …` | 1 OK | PASS=5 ERROR=0 |
| `dbt seed --full-refresh --selector …` | 1 OK | PASS=4 ERROR=0 |

7 relations created and verified in `INFORMATION_SCHEMA` (1 seed, 1 snapshot, 2 views,
3 tables; row counts 17 – 10 489). Selectors resolving to 1000+ nodes were validated by
`dbt ls` only and never built.

Four earlier runs failed for **environmental** reasons, not maestro: two selectors referenced
upstream schemas that do not exist in that dev account (`Schema '…' does not exist or not
authorized`), a `test`-only selector inherently presumes its models already exist, and a
full-refresh of one of those same models. In each case maestro produced the correct selector
and command and Airflow executed the task — the models themselves failed. This is the
"configured correctly" vs "actually executed successfully" distinction.

---

## 4. Recommended fixes, ranked

**Shipped**

1. `airflow.airflow_version: 2|3` — Airflow 3 support (P0-1).
2. `exclude` emitted as a list, with nested `intersection` for that mode (P0-2).
3. `indirect_selection` on definition methods, not root-level `default` (P0-3).
4. `cli._resolve_output_path` so `generate-jobs`/`generate-dags` honour `output_dir` (P1-4).
5. Resolver handles union-sibling `exclude` and both exclude shapes (P1-5).
6. Removed 3 dead duplicate methods (P2-7).
7. +31 tests, including the first coverage of `indirect_selection` and exclude shape.
8. `scripts/maestro_airflow_up.sh` — one-command local Airflow for any dbt project.

**Recommended next**

9. **Extend `ModelResolver` to `tag` and `config.*`** (P1-6) — highest remaining value.
   Interim: warn when a manual selector resolves to 0 models.
10. **`airflow.dbt_executable_path`** (P2-8) — removes a silent dependency on worker `PATH`.
11. **Warn on no-op freshness whitelist entries** (P2-9).
12. **Normalize `\`→`/` in path handling; escalate unmatched paths from info to warning** —
    the test project had a Windows-separator path that silently matched nothing.
13. **Document the `dbt_target` / `generate_schema_name` contract** (§2 operational note).
14. Consider defaulting `airflow_version` to `3` in a future major — `schedule=` alone works on
    Airflow 2.4+ through 3.x.

---

## 5. Verdict

| Criterion | Result |
|---|---|
| Unit tests | **306 pass** (was 275 collected / **3 failing**) |
| Selector configs generated & asserted | **9/9** |
| Generated selectors accepted by real dbt | **all** |
| Airflow configs generated & asserted | **9/9** |
| Idempotency | all assertions pass |
| DAG import validation | **9/9 variants, 0 errors** (Airflow 2.10.5 and 3.2.2) |
| Command verbs executed successfully | **8/8** |
| Relations materialized | **7/7** |
| Test project's working tree | unchanged (`selectors.yml` restored from baseline) |
| Lint | black clean, flake8 clean |

### **Selector generation: PASS** · **Airflow: PASS**

Both only after the fixes above. Before them, both paths **FAIL** for any project that enables
exclusions, sets `indirect_selection`, sets `output_dir`, or runs Airflow 3.

**Residual risk:** P1-6 (resolver blind to `tag`/`config.*`) is unfixed and silently skews
coverage analysis on real projects.

---

## 6. Running this yourself

The fastest way to see maestro working is the launcher, which needs only a dbt project:

```bash
./scripts/maestro_airflow_up.sh up --project-dir /path/to/your/dbt/project
# -> generates selectors + DAGs, installs Airflow, serves http://localhost:8080
./scripts/maestro_airflow_up.sh down
```

To run the full validation matrix:

```bash
export DBT_PROJECT_DIR=/path/to/your/dbt/project
export DBT_PROFILES_DIR=~/.dbt
export DBT_TARGET=dev                       # a target your project expects
export MAESTRO_E2E_WORKDIR=/tmp/maestro-e2e

# 0. manifest (no warehouse writes)
dbt parse --project-dir "$DBT_PROJECT_DIR" --profiles-dir "$DBT_PROFILES_DIR" \
          --target "$DBT_TARGET" --target-path "$MAESTRO_E2E_WORKDIR/target"

# 1. render the config templates for your paths
./e2e/render_configs.sh

# 2. selector generation
./e2e/run_selector_matrix.sh
python  e2e/assert_selectors.py
./e2e/validate_with_dbt.sh

# 3. DAG generation
./e2e/run_dag_matrix.sh
python  e2e/assert_dags.py
python  e2e/assert_idempotency.py

# 4. load them into a real Airflow
./e2e/validate_airflow_imports.sh
```

Notes:

- The matrix writes `selectors.yml` into your project (dbt only reads it from the project root).
  The first run captures your original file as a baseline and restores it at the end.
- Several templates reference deliberately generic tags/paths/models/selectors
  (`example_tag_a`, `your_manual_selector_a`, …). Edit the rendered configs to use real names
  from your project if you want those paths to select anything.
- Bounded warehouse execution was project-specific and is not scripted here; use the launcher
  and trigger a small selector from the UI instead.
