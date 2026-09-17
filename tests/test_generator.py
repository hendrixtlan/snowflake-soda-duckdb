"""The generator has to be trustworthy before anything measured with it is.

A false negative in the detection matrix could be a bug in the injector rather
than a blind spot in a control. These tests exist to rule that out.
"""
from __future__ import annotations

from datetime import datetime
import uuid

import pytest

from generator.events import generate_events

AS_OF = datetime(2026, 9, 16)


@pytest.fixture(scope="module")
def events():
    return generate_events(days=3, users=500, events_per_day=4000, seed=42, as_of=AS_OF)


def test_run_is_reproducible():
    a = generate_events(days=1, users=200, events_per_day=1000, seed=7, as_of=AS_OF)
    b = generate_events(days=1, users=200, events_per_day=1000, seed=7, as_of=AS_OF)
    assert a.equals(b), "same seed and as_of must produce an identical frame"


def test_different_seed_changes_output():
    a = generate_events(days=1, users=200, events_per_day=1000, seed=7, as_of=AS_OF)
    b = generate_events(days=1, users=200, events_per_day=1000, seed=8, as_of=AS_OF)
    assert not a.equals(b)


def test_event_ids_are_unique_v4_uuids(events):
    assert events.event_id.is_unique
    assert all(uuid.UUID(x).version == 4 for x in events.event_id.head(1000))


def test_ms_played_never_exceeds_duration(events):
    assert (events.ms_played <= events.duration_ms).all()


def test_skips_stay_under_eighty_percent(events):
    skips = events[events.event_type == "skip"]
    assert (skips.ms_played < skips.duration_ms * 0.8).all()


def test_song_maps_to_one_artist_and_genre(events):
    per_song = events.groupby("song_id")[["artist_id", "genre"]].nunique()
    assert (per_song == 1).all().all()


def test_session_belongs_to_a_single_user(events):
    assert (events.groupby("session_id").user_id.nunique() == 1).all()


def test_sessions_are_temporally_contiguous(events):
    """A session must be a run of activity, not a random bucket."""
    spans = events.groupby("session_id").event_ts.agg(["min", "max"])
    gaps = (spans["max"] - spans["min"]).dt.total_seconds() / 60
    assert gaps.max() < 24 * 60


def test_timestamps_have_daily_seasonality(events):
    """Monte Carlo needs a shape to learn. Uniform timestamps teach it nothing."""
    by_hour = events.event_ts.dt.hour.value_counts()
    assert by_hour.max() / by_hour.min() > 3


def test_required_columns_present(events):
    expected = {
        "event_id", "user_id", "session_id", "song_id", "artist_id", "genre",
        "event_type", "event_ts", "duration_ms", "ms_played", "device_type",
        "country", "_ingested_at", "_source_file",
    }
    assert expected <= set(events.columns)
