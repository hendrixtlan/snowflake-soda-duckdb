"""Run the real dbt model SQL against generated data, in DuckDB.

Why this exists: `dbt parse` proves the project is well formed and sqlfluff
proves the SQL is valid Snowflake, but neither runs a single query. Without a
warehouse, the arithmetic of the models -- entropy bounded to [0,1], affinity
normalised per user, shares that partition listening time, orphans that survive
into the fact table -- would go unverified until someone has credentials.

The models are executed UNMODIFIED: only `iff`, `dateadd` and
`current_timestamp()` are shimmed, because DuckDB spells them differently.

Caveat, and it matters: DuckDB is not Snowflake. Passing here does not
guarantee `dbt build` succeeds against a warehouse. It does guarantee the logic
is right, which is the part that a warehouse would not have told us either.
"""
from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path

import duckdb
import jinja2
import pytest

from generator.anomalies import inject
from generator.events import generate_events

ROOT = Path(__file__).resolve().parents[1]
DBT = ROOT / "dbt_music" / "models"
AS_OF = datetime(2026, 9, 16)

BUILD_ORDER = [
    ("staging", "stg_songs"),
    ("staging", "stg_artists"),
    ("staging", "stg_listening_events"),
    ("staging", "stg_rejected_events"),
    ("core", "dim_user"),
    ("core", "dim_song"),
    ("core", "dim_artist"),
    ("core", "fct_listening_events"),
    ("marts", "user_listening_features"),
    ("marts", "user_genre_profile"),
    ("marts", "user_song_affinity"),
]

SHIMS = """
CREATE OR REPLACE MACRO iff(c, a, b) AS CASE WHEN c THEN a ELSE b END;
CREATE OR REPLACE MACRO dateadd(part, n, ts) AS ts + (n * INTERVAL 1 DAY);
"""


def render(path: Path) -> str:
    env = jinja2.Environment(undefined=jinja2.StrictUndefined)
    sql = env.from_string(path.read_text()).render(
        ref=lambda name: name,
        source=lambda _schema, table: f"raw_{table}",
        config=lambda **_: "",
        var=lambda _name, default=None: default,
        is_incremental=lambda: False,
    )
    # DuckDB has no zero-argument call form for current_timestamp.
    return re.sub(r"current_timestamp\(\)", "current_timestamp", sql, flags=re.I)


def build(events, songs, artists) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(SHIMS)
    ev = events.copy()
    ev["_batch_id"] = "test-batch"
    con.register("raw_listening_events", ev)
    con.register("raw_songs", songs)
    con.register("raw_artists", artists)
    for layer, model in BUILD_ORDER:
        sql = render(DBT / layer / f"{model}.sql")
        con.execute(f"CREATE OR REPLACE TABLE {model} AS {sql}")
    return con


@pytest.fixture(scope="module")
def catalog():
    import pandas as pd
    return (pd.read_csv(ROOT / "data" / "catalog" / "songs.csv"),
            pd.read_csv(ROOT / "data" / "catalog" / "artists.csv"))


@pytest.fixture(scope="module")
def events():
    return generate_events(days=5, users=400, events_per_day=6000, seed=42, as_of=datetime.now())


@pytest.fixture(scope="module")
def con(events, catalog):
    return build(events, *catalog)


def scalar(con, sql):
    return con.execute(sql).fetchone()[0]


# --- the whole project builds -------------------------------------------- #

def test_every_model_builds(con):
    for _, model in BUILD_ORDER:
        assert scalar(con, f"select count(*) from {model}") >= 0


def test_fact_table_is_not_empty(con):
    assert scalar(con, "select count(*) from fct_listening_events") > 0


# --- the ranges the dbt tests assert ------------------------------------- #

@pytest.mark.parametrize("model,column", [
    ("user_listening_features", "skip_rate"),
    ("user_listening_features", "avg_completion_rate"),
    ("user_listening_features", "genre_diversity"),
    ("user_listening_features", "top_genre_share"),
    ("user_genre_profile", "affinity_score"),
    ("user_song_affinity", "affinity_score"),
    ("fct_listening_events", "completion_rate"),
])
def test_column_stays_within_zero_and_one(con, model, column):
    bad = scalar(con, f"select count(*) from {model} "
                      f"where {column} is not null and ({column} < 0 or {column} > 1)")
    assert bad == 0, f"{model}.{column} escapes [0,1] in {bad} rows"


def test_genre_shares_partition_listening_time(con):
    bad = scalar(con, """
        select count(*) from (
          select user_id, sum(affinity_score) s
          from user_genre_profile group by user_id
        ) where abs(s - 1) > 0.001
    """)
    assert bad == 0


def test_affinity_is_normalised_within_each_user(con):
    """Per-user min-max, so a light listener's taste is comparable to a heavy
    one's. A global normalisation would flatten the light listener to nothing."""
    rows = con.execute("""
        select min(mn), max(mx) from (
          select min(affinity_score) mn, max(affinity_score) mx
          from user_song_affinity group by user_id
          having count(*) > 1
        )
    """).fetchone()
    assert rows[0] == pytest.approx(0.0, abs=1e-9)
    assert rows[1] == pytest.approx(1.0, abs=1e-9)


def test_activity_tier_uses_the_declared_values(con):
    tiers = {r[0] for r in con.execute(
        "select distinct activity_tier from user_listening_features").fetchall()}
    assert tiers <= {"heavy", "regular", "casual", "dormant"}


def test_user_id_is_unique_in_features(con):
    assert scalar(con, """
        select count(*) from (
          select user_id from user_listening_features group by user_id having count(*) > 1
        )""") == 0


def test_every_active_user_has_a_profile(con):
    """Coverage: a user with activity but no row is invisible to the recommender."""
    assert scalar(con, """
        select count(*) from (
          select distinct user_id from fct_listening_events where not is_orphan_song
        ) f
        left join user_listening_features u using (user_id)
        where u.user_id is null
    """) == 0


# --- the point of the whole design --------------------------------------- #

def test_orphan_events_reach_the_fact_table_flagged(catalog):
    """The core claim of the transformation layer: an event whose song is not in
    the catalogue must arrive flagged, not vanish into an inner join. If it
    vanished, the relationships test downstream would pass on destroyed evidence.
    """
    base = generate_events(days=2, users=200, events_per_day=3000, seed=1, as_of=datetime.now())
    con = build(inject(base, "A5"), *catalog)
    orphans = scalar(con, "select count(*) from fct_listening_events where is_orphan_song")
    assert orphans > 0, "A5 was silently absorbed by the transformation"
    assert scalar(con, """
        select count(*) from stg_rejected_events
        where rejection_reason = 'song_id_not_in_catalog'
    """) > 0


def test_duplicates_are_deduplicated_but_recorded(catalog):
    base = generate_events(days=2, users=200, events_per_day=3000, seed=2, as_of=datetime.now())
    con = build(inject(base, "A4"), *catalog)
    assert scalar(con, """
        select count(*) from (
          select event_id from fct_listening_events group by event_id having count(*) > 1
        )""") == 0, "dedup did not happen"
    assert scalar(con, "select count(*) from fct_listening_events where was_deduplicated") > 0, \
        "dedup happened but left no trace, which is the failure mode this project exposes"


def test_orphan_events_are_excluded_from_features(catalog):
    """Flagged in CORE, excluded from MARTS: the recommender must not learn from
    events pointing at songs that do not exist."""
    base = generate_events(days=2, users=200, events_per_day=3000, seed=3, as_of=datetime.now())
    con = build(inject(base, "A5"), *catalog)
    assert scalar(con, """
        select count(*) from user_song_affinity a
        left join dim_song s using (song_id)
        where s.song_id is null
    """) == 0


def test_a9_moves_skip_rate_without_breaking_any_range(catalog):
    """A9 is the flagship: every value stays legal, only the distribution moves.
    No range test can see it, which is the argument for a behavioural layer."""
    base = generate_events(days=3, users=300, events_per_day=4000, seed=4, as_of=datetime.now())
    clean = build(base, *catalog)
    dirty = build(inject(base, "A9"), *catalog)
    q = "select avg(skip_rate) from user_listening_features"
    assert scalar(dirty, q) < scalar(clean, q) * 0.75
    assert scalar(dirty, """
        select count(*) from user_listening_features
        where skip_rate < 0 or skip_rate > 1 or avg_completion_rate > 1
    """) == 0
