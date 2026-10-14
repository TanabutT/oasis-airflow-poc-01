"""Tests for the bronze -> silver cleaning SQL, run on the sample JSON (no RustFS or Postgres needed).

Run from the repo root:  PYTHONPATH=. pytest tests/
"""
from pathlib import Path

import pytest

from pipeline.indicators import transform_local

SAMPLE = Path(__file__).parent.parent / "data" / "source" / "indicators" / "indicators_sample.json"


@pytest.fixture(scope="module")
def silver():
    return transform_local([SAMPLE], batch_id="test_batch")


def test_one_row_per_indicator(silver):
    ids = silver["indicators"].column("indicator_id").to_pylist()
    assert ids == ["3.2.TheilInd1", "3.2_PRI.STUDENTS", "5.1.13_AFG.TOTA.AID.WB"]


def test_blank_unit_becomes_null(silver):
    units = silver["indicators"].column("unit").to_pylist()
    assert units == [None, None, None]


def test_source_id_is_typed(silver):
    rows = silver["indicators"].to_pylist()
    assert [r["source_id"] for r in rows] == [37, 34, 34]
    assert rows[0]["source_name"] == "LAC Equity Lab"


def test_text_is_trimmed_and_slashes_unescaped(silver):
    rows = {r["indicator_id"]: r for r in silver["indicators"].to_pylist()}
    assert not rows["3.2.TheilInd1"]["source_note"].startswith(" ")
    assert not rows["3.2.TheilInd1"]["source_note"].endswith(" ")
    org = rows["5.1.13_AFG.TOTA.AID.WB"]["source_organization"]
    assert "\\/" not in org
    assert "http://www.globalpartnership.org/our-work/" in org


def test_topics_exploded_and_placeholders_dropped(silver):
    topics = silver["indicator_topics"].to_pylist()
    assert topics == [
        {"indicator_id": "3.2.TheilInd1", "topic_id": 11, "topic_name": "Poverty", "batch_id": "test_batch"}
    ]


def test_batch_id_is_stamped_on_every_row(silver):
    for table in silver.values():
        assert set(table.column("batch_id").to_pylist()) == {"test_batch"}
