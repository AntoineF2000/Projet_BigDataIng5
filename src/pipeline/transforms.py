"""Pure DataFrame transformations (bronze -> silver -> gold).

Functions take and return DataFrames and never touch storage, so they are unit
tested locally (tests/test_transforms.py) and reused unchanged on the cluster.
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from schemas import CORRUPT_COL


def _clean_str(col: str) -> Column:
    """Trim; map '' and the OpenFlights null marker '\\N' to NULL."""
    c = F.trim(F.col(col))
    return F.when((c == "") | (c == "\\N"), None).otherwise(c)


_INT_RE = r"^-?[0-9]{1,9}$"
_DOUBLE_RE = r"^-?[0-9]+(\.[0-9]+)?$"


def _safe_int(col: str) -> Column:
    """Validate before casting: invalid values become NULL (and are counted)
    instead of failing the job when ANSI mode is on (Spark 4 default)."""
    c = _clean_str(col)
    return F.when(c.rlike(_INT_RE), c.cast("int"))


def _safe_double(col: str) -> Column:
    c = _clean_str(col)
    return F.when(c.rlike(_DOUBLE_RE), c.cast("double"))


# --------------------------------------------------------------------------- silver
def clean_airports(raw: DataFrame) -> DataFrame:
    return (
        raw.where(F.col(CORRUPT_COL).isNull())
        .select(
            _safe_int("airport_id").alias("airport_id"),
            _clean_str("name").alias("name"),
            _clean_str("city").alias("city"),
            _clean_str("country").alias("country"),
            _clean_str("iata").alias("iata"),
            _clean_str("icao").alias("icao"),
            _safe_double("latitude").alias("latitude"),
            _safe_double("longitude").alias("longitude"),
            _safe_int("altitude_ft").alias("altitude_ft"),
            _safe_double("utc_offset").alias("utc_offset"),
            _clean_str("dst").alias("dst"),
            _clean_str("tz_database").alias("tz_database"),
        )
        .where(F.col("airport_id").isNotNull())
        .where(F.col("latitude").between(-90, 90) & F.col("longitude").between(-180, 180))
        .dropDuplicates(["airport_id"])
    )


def clean_airlines(raw: DataFrame) -> DataFrame:
    return (
        raw.where(F.col(CORRUPT_COL).isNull())
        .select(
            _safe_int("airline_id").alias("airline_id"),
            _clean_str("name").alias("name"),
            _clean_str("iata").alias("iata"),
            _clean_str("icao").alias("icao"),
            _clean_str("country").alias("country"),
            # 'n' occurs once in the raw file: normalise case before casting to bool
            F.coalesce(F.upper(_clean_str("active")) == "Y", F.lit(False)).alias("is_active"),
        )
        .where(F.col("airline_id").isNotNull() & (F.col("airline_id") > 0))
        .dropDuplicates(["airline_id"])
    )


def _resolve_airport(routes: DataFrame, airports: DataFrame, side: str) -> DataFrame:
    """Resolve ``{side}_airport_id``.

    1. keep the id if it exists in airports;
    2. otherwise (``\\N`` or orphan id) repair it from the IATA/ICAO code.
    Adds ``{side}_repaired`` so the number of repaired rows is reported.
    """
    ids = airports.select(F.col("airport_id").alias(f"_{side}_known_id"))
    by_iata = airports.where(F.col("iata").isNotNull()).select(
        F.col("iata").alias(f"_{side}_iata"), F.col("airport_id").alias(f"_{side}_id_iata"))
    by_icao = airports.where(F.col("icao").isNotNull()).select(
        F.col("icao").alias(f"_{side}_icao"), F.col("airport_id").alias(f"_{side}_id_icao"))
    raw_id, code = f"{side}_airport_id_raw", f"{side}_code"
    out = (
        routes.join(F.broadcast(ids), F.col(raw_id) == F.col(f"_{side}_known_id"), "left")
        .join(F.broadcast(by_iata), F.col(code) == F.col(f"_{side}_iata"), "left")
        .join(F.broadcast(by_icao), F.col(code) == F.col(f"_{side}_icao"), "left")
    )
    resolved = F.coalesce(F.col(f"_{side}_known_id"), F.col(f"_{side}_id_iata"), F.col(f"_{side}_id_icao"))
    return (
        out.withColumn(f"{side}_airport_id", resolved)
        .withColumn(f"{side}_repaired", F.col(f"_{side}_known_id").isNull() & resolved.isNotNull())
        .drop(f"_{side}_known_id", f"_{side}_iata", f"_{side}_id_iata", f"_{side}_icao", f"_{side}_id_icao")
    )


def clean_routes(raw: DataFrame, airports: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return ``(valid_routes, quarantine)``; quarantine keeps a ``reject_reason``."""
    typed = raw.where(F.col(CORRUPT_COL).isNull()).select(
        _clean_str("airline_code").alias("airline_code"),
        _safe_int("airline_id").alias("airline_id"),
        F.upper(_clean_str("src_code")).alias("src_code"),
        _safe_int("src_airport_id").alias("src_airport_id_raw"),
        F.upper(_clean_str("dst_code")).alias("dst_code"),
        _safe_int("dst_airport_id").alias("dst_airport_id_raw"),
        F.coalesce(F.upper(_clean_str("codeshare")) == "Y", F.lit(False)).alias("is_codeshare"),
        _safe_int("stops").alias("stops"),
        F.split(_clean_str("equipment"), " ").alias("equipment"),
    )
    resolved = _resolve_airport(_resolve_airport(typed, airports, "src"), airports, "dst")

    reason = (
        F.when(F.col("src_airport_id").isNull(), "unknown_source_airport")
        .when(F.col("dst_airport_id").isNull(), "unknown_destination_airport")
        .when(F.col("src_airport_id") == F.col("dst_airport_id"), "self_loop")
        .when(F.col("stops").isNull() | (F.col("stops") < 0), "invalid_stops")
    )
    flagged = resolved.withColumn("reject_reason", reason)
    valid = (
        flagged.where(F.col("reject_reason").isNull())
        .drop("reject_reason", "src_airport_id_raw", "dst_airport_id_raw")
        .dropDuplicates(["airline_code", "src_airport_id", "dst_airport_id"])
    )
    quarantine = flagged.where(F.col("reject_reason").isNotNull())
    return valid, quarantine


# ----------------------------------------------------------------------------- gold
def _with_countries(routes: DataFrame, airports: DataFrame) -> DataFrame:
    src = airports.select(
        F.col("airport_id").alias("src_airport_id"), F.col("iata").alias("src_iata"),
        F.col("name").alias("src_name"), F.col("city").alias("src_city"),
        F.col("country").alias("src_country"))
    dst = airports.select(F.col("airport_id").alias("dst_airport_id"), F.col("country").alias("dst_country"))
    return (
        routes.join(F.broadcast(src), "src_airport_id").join(F.broadcast(dst), "dst_airport_id")
        .withColumn("is_international", F.col("src_country") != F.col("dst_country"))
    )


def hub_ranking(routes: DataFrame, airports: DataFrame) -> DataFrame:
    """One row per departure airport, ranked by distinct direct destinations."""
    r = _with_countries(routes, airports)
    agg = r.groupBy("src_airport_id", "src_iata", "src_name", "src_city", "src_country").agg(
        F.countDistinct("dst_airport_id").alias("n_destinations"),
        F.countDistinct(F.when(F.col("is_international"), F.col("dst_airport_id"))).alias("n_intl_destinations"),
        F.countDistinct("dst_country").alias("n_destination_countries"),
        F.countDistinct("airline_code").alias("n_airlines"),
        F.count(F.lit(1)).alias("n_routes"),
        F.round(F.avg(F.col("is_codeshare").cast("int")) * 100, 1).alias("pct_codeshare"),
    )
    w = Window.orderBy(F.desc("n_destinations"), F.desc("n_routes"), F.asc("src_iata"))
    return (
        agg.withColumn("pct_intl_destinations",
                       F.round(F.col("n_intl_destinations") / F.col("n_destinations") * 100, 1))
        .withColumn("rank", F.row_number().over(w))
        .withColumnRenamed("src_airport_id", "airport_id").withColumnRenamed("src_iata", "iata")
        .withColumnRenamed("src_name", "airport").withColumnRenamed("src_city", "city")
        .withColumnRenamed("src_country", "country")
        .select("rank", "airport_id", "iata", "airport", "city", "country", "n_destinations",
                "n_intl_destinations", "pct_intl_destinations", "n_destination_countries",
                "n_airlines", "n_routes", "pct_codeshare")
    )


def country_connectivity(routes: DataFrame, airports: DataFrame) -> DataFrame:
    """One row per country: size of its network and dependence on international links."""
    r = _with_countries(routes, airports)
    return (
        r.groupBy(F.col("src_country").alias("country")).agg(
            F.countDistinct("src_airport_id").alias("n_airports_served"),
            F.count(F.lit(1)).alias("n_routes"),
            F.sum(F.col("is_international").cast("int")).alias("n_intl_routes"),
            F.countDistinct(F.when(F.col("is_international"), F.col("dst_country"))).alias("n_destination_countries"),
            F.countDistinct("airline_code").alias("n_airlines"),
        )
        .withColumn("pct_intl_routes", F.round(F.col("n_intl_routes") / F.col("n_routes") * 100, 1))
        .orderBy(F.desc("n_routes"))
    )
