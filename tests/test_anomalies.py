"""Every anomaly must actually do what its name claims.

Each test asserts the observable signature of the anomaly, so that a control
failing to detect one is evidence about the control and not about the injector.
"""
from __future__ import annotations

from datetime import datetime

import pandas as pd
import pytest

from generator.anomalies import inject
from generator.events import generate_events

AS_OF = datetime(2026, 9, 16)


@pytest.fixture(scope="module")
def base():
    return generate_events(days=2, users=400, events_per_day=5000, seed=42, as_of=AS_OF)


def skip_rate(df: pd.DataFrame) -> float:
    listening = df.event_type.isin(["play", "skip", "pause", "replay"]).sum()
    return (df.event_type == "skip").sum() / listening if listening else 0.0


def test_a1_nulls_user_id(base):
    out = inject(base, "A1")
    assert 0.02 < out.user_id.isna().mean() < 0.04


def test_a2_breaks_the_duration_invariant(base):
    out = inject(base, "A2")
    assert (out.ms_played > out.duration_ms).sum() > 0


def test_a3_introduces_unknown_event_type(base):
    assert "share" in set(inject(base, "A3").event_type)


def test_a4_duplicates_event_ids(base):
    out = inject(base, "A4")
    assert out.event_id.duplicated().sum() > 0


def test_a5_creates_orphan_song_ids(base):
    out = inject(base, "A5")
    assert "S99999" in set(out.song_id)
    assert "S99999" not in set(base.song_id)


def test_a6_drops_a_declared_column(base):
    assert "country" not in inject(base, "A6").columns


def test_a7_delays_both_event_and_ingest_time(base):
    """A load delay that leaves _ingested_at untouched cannot be seen by dbt
    source freshness, which is precisely the control meant to catch it."""
    out = inject(base, "A7")
    ev = (base.event_ts.max() - out.event_ts.max()).total_seconds() / 3600
    ing = (pd.to_datetime(base._ingested_at.max())
           - pd.to_datetime(out._ingested_at.max())).total_seconds() / 3600
    assert ev == pytest.approx(10, abs=0.1)
    assert ing == pytest.approx(10, abs=0.1)


def test_a8_shrinks_one_country_only(base):
    out = inject(base, "A8")
    before = (base.country == "BR").sum()
    after = (out.country == "BR").sum()
    assert after == pytest.approx(before * 0.6, rel=0.05)
    assert (out.country == "MX").sum() == (base.country == "MX").sum()


def test_a9_lowers_skip_rate_without_breaking_any_rule(base):
    out = inject(base, "A9")
    assert skip_rate(out) < skip_rate(base) * 0.7
    assert (out.ms_played <= out.duration_ms).all()
    assert set(out.event_type) <= {"play", "skip", "pause", "replay", "like"}


def test_a10_doubles_volume_without_duplicate_ids(base):
    out = inject(base, "A10")
    assert len(out) == 2 * len(base)
    assert out.event_id.duplicated().sum() == 0


def test_a11_removes_a_platform_without_touching_volume(base):
    """A11 must be a pure distribution shift. If it also drops rows, a volume
    monitor catches it and it stops being a Monte-Carlo-only anomaly."""
    out = inject(base, "A11")
    assert len(out) == len(base)
    assert (out.device_type == "mobile").sum() == 0


def test_a12_shifts_playtime_while_staying_valid(base):
    out = inject(base, "A12")
    assert out.ms_played.mean() > base.ms_played.mean() * 1.05
    assert (out.ms_played <= out.duration_ms).all()
    skips = out[out.event_type == "skip"]
    assert (skips.ms_played < skips.duration_ms * 0.8).all()


def test_a13_starves_a_slice_of_users(base):
    out = inject(base, "A13")
    thin = (out.user_id.value_counts() == 1).sum()
    assert thin > (base.user_id.value_counts() == 1).sum()


def test_a14_changes_a_column_type(base):
    out = inject(base, "A14")
    # pandas reports this as `object` or `str` depending on the version; what
    # matters to the contract is that it is no longer numeric.
    assert out.duration_ms.dtype.kind not in "iuf"
    assert isinstance(out.duration_ms.iloc[0], str)


def test_unknown_anomaly_is_rejected(base):
    with pytest.raises(ValueError):
        inject(base, "A99")
