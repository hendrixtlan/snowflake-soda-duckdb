"""The ingestion gate must accept clean data and quarantine exactly what the
contract forbids -- no more, no less."""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "ingestion" / "great_expectations"))

from generator.anomalies import inject          # noqa: E402
from generator.events import generate_events    # noqa: E402
from validate_batch import validate             # noqa: E402

AS_OF = datetime(2026, 9, 16)


@pytest.fixture(scope="module")
def base():
    return generate_events(days=1, users=300, events_per_day=3000, seed=42, as_of=AS_OF)


def run(df, tmp_path, name):
    p = tmp_path / f"{name}.parquet"
    df.to_parquet(p, index=False)
    return validate(p)


def test_clean_batch_is_accepted(base, tmp_path):
    res = run(base, tmp_path, "clean")
    assert res["verdict"] == "accepted"
    assert res["rows_quarantined"] == 0
    assert res["failed_expectations"] == []


@pytest.mark.parametrize("anomaly,expectation", [
    ("A1", "user_id_not_null"),
    ("A2", "ms_played_le_duration"),
    ("A3", "event_type_allowed"),
    ("A4", "event_id_unique"),
])
def test_row_level_violations_are_quarantined(base, tmp_path, anomaly, expectation):
    res = run(inject(base, anomaly), tmp_path, anomaly)
    assert expectation in res["failed_expectations"]
    assert res["rows_quarantined"] > 0


@pytest.mark.parametrize("anomaly,expectation", [
    ("A6", "schema_columns_present"),
    ("A14", "schema_types_match"),
])
def test_schema_violations_reject_the_batch(base, tmp_path, anomaly, expectation):
    res = run(inject(base, anomaly), tmp_path, anomaly)
    assert res["verdict"] == "rejected_schema"
    assert expectation in res["failed_expectations"]


@pytest.mark.parametrize("anomaly", ["A5", "A7", "A8", "A9", "A10", "A11", "A12", "A13"])
def test_behavioural_anomalies_pass_the_contract(base, tmp_path, anomaly):
    """These are the ones that matter. Each produces perfectly valid records and
    must sail through the deterministic gate -- that is the whole argument for
    needing a behavioural layer at all."""
    res = run(inject(base, anomaly), tmp_path, anomaly)
    assert res["verdict"] == "accepted", f"{anomaly} should not be caught by the contract"
    assert res["rows_quarantined"] == 0
