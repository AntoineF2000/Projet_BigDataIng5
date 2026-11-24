"""Explicit schemas of the raw OpenFlights files.

Raw files have no header: we never rely on schema inference (non-deterministic,
slower, and it silently turns ``\\N`` into strings). Every column is read as a
string in bronze; typing happens in the silver layer where failures are counted.
Reference: https://openflights.org/data.php
"""
from pyspark.sql.types import StringType, StructField, StructType

CORRUPT_COL = "_corrupt_record"


def _strings(*names: str) -> StructType:
    fields = [StructField(n, StringType(), True) for n in names]
    fields.append(StructField(CORRUPT_COL, StringType(), True))
    return StructType(fields)


AIRPORTS = _strings(
    "airport_id", "name", "city", "country", "iata", "icao", "latitude",
    "longitude", "altitude_ft", "utc_offset", "dst", "tz_database", "type", "source",
)

AIRLINES = _strings(
    "airline_id", "name", "alias", "iata", "icao", "callsign", "country", "active",
)

ROUTES = _strings(
    "airline_code", "airline_id", "src_code", "src_airport_id", "dst_code",
    "dst_airport_id", "codeshare", "stops", "equipment",
)

COUNTRIES = _strings("name", "iso_code", "dafif_code")

RAW_FILES = {
    "airports": ("airports.dat", AIRPORTS),
    "airlines": ("airlines.dat", AIRLINES),
    "routes": ("routes.dat", ROUTES),
    "countries": ("countries.dat", COUNTRIES),
}
