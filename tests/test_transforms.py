"""Unit tests of the cleaning rules on tiny hand-written inputs (local Spark)."""
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src" / "pipeline"))
import transforms as T  # noqa: E402
from schemas import AIRLINES, AIRPORTS, ROUTES  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.master("local[2]").appName("tests")
         .config("spark.sql.shuffle.partitions", 2)
         .config("spark.driver.memory", "512m").config("spark.ui.enabled", "false").getOrCreate())
    yield s
    s.stop()


def _df(spark, schema, rows):
    return spark.createDataFrame([r + (None,) for r in rows], schema)


AIRPORT_ROWS = [
    ("1", "Alpha", "A", "France", "AAA", "LFAA", "48.0", "2.0", "100", "1", "E", "Europe/Paris", "airport", "OurAirports"),
    ("2", "Beta", "B", "France", "BBB", "LFBB", "45.0", "5.0", "100", "1", "E", "Europe/Paris", "airport", "OurAirports"),
    ("3", "Gamma", "C", "Spain", "CCC", "LECC", "40.0", "-3.0", "100", "1", "E", "Europe/Madrid", "airport", "OurAirports"),
    ("4", "Bad lat", "D", "Spain", "\\N", "LEDD", "123.0", "0.0", "0", "1", "E", "\\N", "airport", "OurAirports"),
]


def test_airports_null_marker_and_invalid_coordinates(spark):
    out = T.clean_airports(_df(spark, AIRPORTS, AIRPORT_ROWS))
    assert {r.airport_id for r in out.collect()} == {1, 2, 3}  # latitude 123 rejected


def test_airlines_active_flag_case_insensitive(spark):
    raw = _df(spark, AIRLINES, [("10", "X", "\\N", "XX", "XXX", "", "France", "n"),
                                ("11", "Y", "\\N", "YY", "YYY", "", "France", "Y"),
                                ("-1", "Unknown", "\\N", "-", "N/A", "\\N", "\\N", "Y")])
    out = {r.airline_id: r.is_active for r in T.clean_airlines(raw).collect()}
    assert out == {10: False, 11: True}  # placeholder id -1 dropped


def test_routes_repair_and_quarantine(spark):
    airports = T.clean_airports(_df(spark, AIRPORTS, AIRPORT_ROWS))
    raw = _df(spark, ROUTES, [
        ("AF", "1", "AAA", "1", "BBB", "2", "", "0", "320"),      # valid
        ("AF", "1", "AAA", "\\N", "CCC", "3", "Y", "0", "320"),   # src id missing -> repaired by IATA
        ("AF", "1", "AAA", "999", "CCC", "3", "", "0", ""),       # orphan id -> repaired (duplicate key)
        ("AF", "1", "ZZZ", "\\N", "CCC", "3", "", "0", "320"),    # unknown airport -> quarantine
        ("AF", "1", "AAA", "1", "AAA", "1", "", "0", "320"),      # self loop -> quarantine
    ])
    valid, quarantine = T.clean_routes(raw, airports)
    assert valid.count() == 2  # duplicate (AF, 1, 3) removed after repair
    assert valid.where("src_repaired").count() == 1
    reasons = sorted(r.reject_reason for r in quarantine.collect())
    assert reasons == ["self_loop", "unknown_source_airport"]


def test_hub_ranking_counts_distinct_destinations(spark):
    airports = T.clean_airports(_df(spark, AIRPORTS, AIRPORT_ROWS))
    raw = _df(spark, ROUTES, [
        ("AF", "1", "AAA", "1", "BBB", "2", "", "0", "320"),
        ("IB", "2", "AAA", "1", "BBB", "2", "Y", "0", "320"),  # same destination, other airline
        ("AF", "1", "AAA", "1", "CCC", "3", "", "0", "320"),
    ])
    valid, _ = T.clean_routes(raw, airports)
    top = T.hub_ranking(valid, airports).where("rank = 1").first()
    assert (top.iata, top.n_destinations, top.n_intl_destinations, top.n_routes) == ("AAA", 2, 1, 3)
    assert top.pct_intl_destinations == 50.0
    assert top.pct_codeshare == 33.3  # '' must count as "not codeshare", not NULL
