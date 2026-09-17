# Music Data Reliability

[English](README.md) · [Español](README.es.md)

A **Data Reliability** and **Data Observability** prototype built on a music-preference analytics system.

Four independent layers of control — an ingestion contract, transformation, SLOs and observability — protect a feature set destined to feed a recommendation engine. The project does not assert that all four are necessary: it measures it.

## The thesis

A music-preference system infers taste from behaviour, with no explicit ratings to check against. A corrupted event is not a lost record; it is a falsified opinion attributed to a real person.

Failures fall into two families with opposite natures:

- An `ms_played` greater than the track's duration is **impossible**, and can be written down as a rule.
- An `ms_played` inflated by 25% is **plausible on every single row**, and only shows up when the distribution is compared against its own history.

Both break the product equally, and neither kind of control catches both. The demonstration is the detection matrix: 14 anomalies against 4 controls, measured rather than declared.

## Separation of responsibilities

| Control | Question it answers | Layer | On failure |
| --- | --- | --- | --- |
| Great Expectations | Does the incoming data satisfy the contract? | Pre-`RAW` | Blocks or quarantines |
| dbt tests | Is the model structurally coherent? | `STAGING`, `CORE` | Fails the build |
| Soda | Does the dataset meet its SLO? | `CORE`, `MARTS` | Alerts, withdraws certification |
| Monte Carlo | Is the data behaving as it always has? | All | Alerts with lineage |

Non-duplication rules, enforced in the code:

1. A check is declared once, in the layer where it first becomes enforceable.
2. Great Expectations never validates transformed tables; its domain ends at `RAW`.
3. Soda does not reimplement dbt's structural tests.
4. **Monte Carlo is given no thresholds.** The moment one is written, the control stops being observability and becomes a Soda check under a different name.

## Two design decisions that hold everything up

**Nothing is discarded silently.** `stg_listening_events` deduplicates, but flags every row with `was_deduplicated`. `fct_listening_events` uses a `LEFT JOIN` against the catalogue and flags `is_orphan_song` instead of filtering. `stg_rejected_events` gathers everything dropped, with its reason. A test that passes because the offending rows were removed upstream is worse than no test at all: it reports health manufactured by the very pipeline it was meant to audit.

**Certification is decided by Soda, not dbt.** `feature_eligible` is a property of the user (enough signal to profile them). `MARTS.DATA_QUALITY_STATUS.CERTIFIED` is a property of the dataset, written by `certify.py` from the scan result. A dataset cannot certify itself while failing its own checks.

## Requirements

| Requirement | Version | Notes |
| --- | --- | --- |
| Python | 3.11 | The `numpy<2` pin comes from Great Expectations 0.18 |
| Docker + Compose | 24.0 / v2 | Kafka, Redis, LocalStack |
| Snowflake account | A trial is enough | The only mandatory remote service |
| Monte Carlo account | — | Optional: without it the pipeline runs on three layers |

## Getting started

```bash
cp .env.example .env && $EDITOR .env
python -m venv .venv && source .venv/bin/activate
make setup
make snowflake-init
make pipeline
```

`make help` lists every target.

## The experiment

```bash
make baseline                 # 14 days of clean history
make experiment ANOMALY=A9    # inject, run the pipeline, measure
make experiment-all           # all 14
make matrix                   # build the observed matrix
```

`run_injection.py` injects one anomaly, runs the full pipeline and writes a row per (anomaly × control) pair with what was observed and the latency since injection. `build_matrix.py` reads only those runs. If there are none, it fails:

```
No runs found in experiment/results/.
The matrix is measured, not declared: run `make experiment` first.
```

The hypothesis lives separately, in `experiment/expected_matrix.yml`, and the report diffs the observed matrix against it. An expectation that turns out to be wrong is a finding to write up, not a number to quietly correct.

Three states are recorded, not two: **detected**, **not detected**, and **control unavailable**. A tool that is absent and a tool that failed to notice are different findings, and collapsing them invalidates the matrix. When the contract rejects a batch, dbt and Soda are marked unavailable — they never got to see the anomaly.

### Anomaly catalogue

| # | Anomaly | Family |
| --- | --- | --- |
| A1 | `user_id` null in 3% of rows | Deterministic |
| A2 | `ms_played` greater than `duration_ms` | Deterministic |
| A3 | Unknown `event_type` (`share`) | Deterministic |
| A4 | `event_id` duplicated in 1% of rows | Deterministic |
| A5 | `song_id` outside the catalogue | Structural |
| A6 | `country` column dropped | Structural |
| A7 | 10-hour load delay | Temporal |
| A8 | 40% drop in `BR` events | Behavioural |
| A9 | Skip rate falls from 32% to 19% | Behavioural |
| A10 | Volume doubles | Behavioural |
| A11 | `mobile` relabelled as other platforms | Behavioural |
| A12 | `ms_played` shifted +25% | Behavioural |
| A13 | 15% of users reduced to a single event | Coverage |
| A14 | `duration_ms` changes from INTEGER to STRING | Structural |

A11 **relabels** rather than deletes. Deleting those rows would take 57% of the volume with them and any volume monitor would see it; the point of A11 is an anomaly visible only in the distribution of a field.

A7 delays `event_ts` **and** `_ingested_at`. Moving only the first would blind `dbt source freshness`, which is precisely the control meant to catch it.

## Data model

The contract lives in [`ingestion/great_expectations/contract.yml`](ingestion/great_expectations/contract.yml). That file **is** the contract: `validate_batch.py` holds no rules of its own, it only executes what is declared there. Changing it is a reviewable pull request.

Domain invariants:

- `ms_played <= duration_ms`
- A `skip` implies `ms_played < duration_ms * 0.8`
- A `like` may have `ms_played` of 0
- A `song_id` always maps to the same `artist_id` and `genre`
- Events sharing a `session_id` belong to a single `user_id`

Failure policy: row-level violations go to quarantine with their reason; past 5% of the batch, the whole batch is rejected. A schema failure rejects the batch regardless of percentage, because a schema is not a per-row property.

### Layers

| Schema | Contents | Guarantee |
| --- | --- | --- |
| `RAW` | Events that passed the contract, with `_batch_id` | Immutable |
| `RAW.LISTENING_EVENTS_QUARANTINE` | Rejected rows, with the reason | Auditable |
| `STAGING` | Typed, deduplicated and flagged | One record per event |
| `CORE` | `dim_*` and `fct_listening_events` | Referential integrity, orphans flagged |
| `MARTS` | Features and affinities, 30-day window | Certifiable |

Primary output: `MARTS.USER_LISTENING_FEATURES`, one row per user with `top_genre`, `genre_diversity` (normalised Shannon entropy), `skip_rate`, `replay_rate`, `like_rate`, `sessions_30d`, `avg_session_minutes`, `activity_tier` and `feature_eligible`.

## Generator

Vectorised: 100,000 events in under a second.

- **Hourly and weekly seasonality.** Monte Carlo needs a shape to learn from; uniform timestamps teach it nothing.
- **Real sessions**: contiguous events from the same user less than 30 minutes apart. Without this, any session feature is noise.
- **Reproducible**: with `--seed` and `--as-of` pinned, two runs produce identical frames. Without `--as-of` the output depends on the wall clock and the experiment is not reproducible.

```bash
make generate DAYS=7 AS_OF=2026-09-16T00:00:00
```

## Tests

```bash
make verify       # everything checkable without a warehouse: lint + 58 tests
make test         # pytest suite
make test-models  # the real dbt SQL, on DuckDB
make lint         # dbt parse + Snowflake grammar via sqlfluff
```

Snowflake is the only remote service, and almost nothing needs it to be validated. Three verification layers cover what can be covered without it:

| Layer | What it proves | What it does not |
| --- | --- | --- |
| `dbt parse` | Refs, Jinja, YAML, dependency graph | Runs no query at all |
| `sqlfluff` | That the SQL parses against the Snowflake grammar | No semantics, no types |
| DuckDB | The real model SQL against generated data | DuckDB is not Snowflake |

`sqlfluff` is configured as a syntax gate, not a style gate: the active rule list is narrowed to a single trivial rule, because parsing and templating errors are reported regardless. A linter arguing about whitespace trains everyone to ignore its output, and with it the one signal that matters.

The suite covers four things: that the generator is reproducible and respects the invariants, that **every anomaly does what its name claims**, that the contract accepts what is clean and rejects exactly what is forbidden, and that the models produce the right arithmetic.

The third group includes the test that gives the project its point:

```python
@pytest.mark.parametrize("anomaly", ["A5","A7","A8","A9","A10","A11","A12","A13"])
def test_behavioural_anomalies_pass_the_contract(...):
    assert res["verdict"] == "accepted"
```

Eight anomalies **must** sail through the deterministic control without a single failure. If any were caught there, the case for a behavioural layer would be weaker.

Without these tests, a false negative in the matrix could be a bug in the injector rather than a blind spot in a control, and there would be no way to tell the two apart.

`test_model_semantics.py` runs the **unmodified** SQL of all eleven models against generated data, with three functions shimmed (`iff`, `dateadd`, `current_timestamp()`) because DuckDB spells them differently. It verifies what none of the other layers can: that entropy stays within [0,1], that genre affinities sum to 1 per user, that min-max normalisation really is per user, and that an orphan `song_id` arrives at the fact table flagged instead of disappearing.

That last test fails if anyone reverts the `LEFT JOIN` to an inner join. It is the most dangerous regression in the project, and it now has a test that catches it.

## Layout

```
generator/          catalogue, events and the 14 anomalies
ingestion/          YAML contract, gate, idempotent load, certification
dbt_music/          staging → core → marts, tests and contracts
soda/               checks for raw, core and marts
montecarlo/         monitors as code, with no thresholds
experiment/         measurement harness and hypothesis
infra/snowflake/    idempotent bootstrap
tests/              pytest
docs/               contract and observed matrix
```

## Status

Implemented and verified locally: generator, anomalies, ingestion contract with quarantine, measurement harness, test suite, model semantics.

Snowflake is needed only for real execution: `dbt build` against the warehouse, Soda scans and certification. The models' logic and syntax are already verified without it. A Monte Carlo account is needed for the fourth column of the matrix.

Out of scope in this phase: model training, a serving API, production streaming and multi-region operation.
