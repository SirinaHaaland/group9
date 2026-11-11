
import csv
import hashlib
import json
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import ingest


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("DAT535-Bronze-Tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")

    yield session
    session.stop()


def create_flight_csv(path, extra=None, drop=()):
    extra = extra or {}
    columns = [
        c for c in ingest.REQUIRED_COLUMNS if c not in drop
    ] + list(extra)

    rows = [
        {"FlightDate": "2025-01-01", "Reporting_Airline": "AA",
         "Origin": "JFK", "Dest": "LAX", "DepDelay": "-3", "Cancelled": "0"},
        {"FlightDate": "2025-01-02", "Reporting_Airline": "UA",
         "Origin": "SFO", "Dest": "ORD", "DepDelay": "", "Cancelled": "1"},
    ]

    with path.open("w", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=columns, restval="", extrasaction="ignore"
        )
        writer.writeheader()
        for row in rows:
            writer.writerow({**row, **extra})


def create_airport_file(path):
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "1", "John F Kennedy International Airport",
            "New York", "United States", "JFK", "KJFK",
            "40.64", "-73.78", "13", "-5", "A",
            "America/New_York", "airport", "OurAirports"
        ])
        writer.writerow([
            "2", "Los Angeles International Airport",
            "Los Angeles", "United States", "LAX", "KLAX",
            "33.94", "-118.40", "125", "-8", "A",
            "America/Los_Angeles", "airport", "OurAirports"
        ])


def configure_flight_paths(monkeypatch, tmp_path):
    monkeypatch.setattr(
        ingest,
        "BRONZE_DIR",
        tmp_path / "data" / "bronze"
    )
    monkeypatch.setattr(
        ingest,
        "METADATA_DIR",
        tmp_path / "data" / "bronze" / "manifests"
    )


@pytest.mark.parametrize(
    "filename,expected",
    [
        ("flights_2025_1.csv", (2025, 1)),
        ("flights_2025_12.csv", (2025, 12)),
        ("flights_2024_6.csv", (2024, 6)),
    ],
)
def test_get_period_valid(filename, expected):
    assert ingest.get_period(filename) == expected


@pytest.mark.parametrize(
    "filename",
    [
        "flights.csv",
        "flights_2025_13.csv",
        "flights_2025_0.csv",
        "openflights_airports.csv",
    ],
)
def test_get_period_invalid(filename):
    with pytest.raises(ValueError):
        ingest.get_period(filename)


def test_file_checksum(tmp_path):
    source = tmp_path / "test.txt"
    source.write_bytes(b"DAT535 Bronze ingestion")

    expected = hashlib.sha256(
        b"DAT535 Bronze ingestion"
    ).hexdigest()

    assert ingest.file_checksum(source) == expected


def test_checksum_detects_changes(tmp_path):
    source = tmp_path / "test.txt"
    source.write_text("original data")

    first = ingest.file_checksum(source)

    source.write_text("modified data")
    second = ingest.file_checksum(source)

    assert first != second

def test_flight_ingestion(spark, tmp_path, monkeypatch):
    configure_flight_paths(monkeypatch, tmp_path)

    source = tmp_path / "flights_2025_1.csv"
    create_flight_csv(source)

    ingest.ingest_file(spark, source)

    output = (
        tmp_path / "data" / "bronze"
        / "flights" / "period=2025-01"
    )
    manifest = (
        tmp_path / "data" / "bronze"
        / "manifests" / "flights_2025_01.json"
    )

    assert output.is_dir()
    assert manifest.is_file()

    df = spark.read.parquet(str(output))

    assert df.count() == 2
    assert len(df.columns) == 18

    assert dict(df.dtypes)["DepDelay"] == "string"

    rows = df.select("DepDelay", "Cancelled").collect()
    assert ("-3", "0") in [tuple(r) for r in rows]
    assert (None, "1") in [tuple(r) for r in rows]

    for column in [
        "_source_file",
        "_ingestion_time",
        "_run_id",
    ]:
        assert column in df.columns
        assert df.filter(df[column].isNull()).count() == 0

    with manifest.open() as f:
        metadata = json.load(f)

    assert metadata["row_count"] == 2
    assert metadata["source_column_count"] == 15
    assert metadata["schema_hash"] == ingest.schema_hash(
        ingest.REQUIRED_COLUMNS
    )
    assert metadata["code_version"]
    assert metadata["status"] == "SUCCESS"
    assert metadata["checksum_sha256"] == ingest.file_checksum(source)


def test_repeat_flight_ingestion(spark, tmp_path, monkeypatch):
    configure_flight_paths(monkeypatch, tmp_path)

    source = tmp_path / "flights_2025_2.csv"
    create_flight_csv(source)

    ingest.ingest_file(spark, source)

    output = (
        tmp_path / "data" / "bronze"
        / "flights" / "period=2025-02"
    )

    first_count = spark.read.parquet(str(output)).count()

    ingest.ingest_file(spark, source)

    second_count = spark.read.parquet(str(output)).count()

    assert first_count == 2
    assert second_count == 2


def test_modified_source_rejected(spark, tmp_path, monkeypatch):
    configure_flight_paths(monkeypatch, tmp_path)

    source = tmp_path / "flights_2025_3.csv"
    create_flight_csv(source)

    ingest.ingest_file(spark, source)

    with source.open("a") as f:
        f.write("2025-03-03,AA,JFK,LAX,15,0\n")

    with pytest.raises(RuntimeError):
        ingest.ingest_file(spark, source)


def test_airport_ingestion(spark, tmp_path, monkeypatch):
    monkeypatch.setattr(ingest, "PROJECT_ROOT", tmp_path)

    source = tmp_path / "data" / "raw" / "openflights_airports.dat"
    create_airport_file(source)

    ingest.ingest_airports(spark)

    output = tmp_path / "data" / "bronze" / "airports"
    manifest = (
        tmp_path / "data" / "bronze"
        / "manifests" / "airports.json"
    )

    assert output.is_dir()
    assert manifest.is_file()

    df = spark.read.parquet(str(output))

    assert df.count() == 2
    assert len(df.columns) == 17

    codes = {
        row.IATA
        for row in df.select("IATA").collect()
    }
    assert codes == {"JFK", "LAX"}

    assert dict(df.dtypes)["Latitude"] == "string"

    with manifest.open() as f:
        metadata = json.load(f)

    assert metadata["row_count"] == 2
    assert metadata["source_column_count"] == 14
    assert metadata["status"] == "SUCCESS"

    ingest.ingest_airports(spark)

    assert spark.read.parquet(str(output)).count() == 2


def test_modified_airport_source_rejected(
    spark, tmp_path, monkeypatch
):
    monkeypatch.setattr(ingest, "PROJECT_ROOT", tmp_path)

    source = tmp_path / "data" / "raw" / "openflights_airports.dat"
    create_airport_file(source)

    ingest.ingest_airports(spark)

    with source.open("a") as f:
        f.write(
            '3,"Test Airport","City","Country",'
            '"TST","KTST",1,2,3,0,"U","UTC",'
            '"airport","OurAirports"\n'
        )

    with pytest.raises(RuntimeError):
        ingest.ingest_airports(spark)


def test_complete_monthly_delivery(tmp_path):
    files = [
        tmp_path / f"flights_2025_{month}.csv"
        for month in range(1, 13)
    ]

    receipt = ingest.verify_monthly_delivery(files, 2025)

    assert receipt["status"] == "COMPLETE"
    assert receipt["received_count"] == 12
    assert receipt["missing_months"] == []
    assert receipt["duplicate_months"] == {}


def test_missing_monthly_delivery(tmp_path):
    files = [
        tmp_path / f"flights_2025_{month}.csv"
        for month in range(1, 13)
        if month != 7
    ]

    receipt = ingest.verify_monthly_delivery(files, 2025)

    assert receipt["status"] == "INCOMPLETE"
    assert receipt["missing_months"] == [7]


def test_duplicate_monthly_delivery(tmp_path):
    files = [
        tmp_path / f"flights_2025_{month}.csv"
        for month in range(1, 13)
    ]

    files.append(tmp_path / "another_flights_2025_7.csv")

    receipt = ingest.verify_monthly_delivery(files, 2025)

    assert receipt["status"] == "INCOMPLETE"
    assert "7" in receipt["duplicate_months"]


def test_partial_year_allowed(tmp_path):
    files = [tmp_path / f"flights_2025_{m}.csv" for m in (1, 2, 3)]

    receipt = ingest.verify_monthly_delivery(files, 2025, allow_partial=True)

    assert receipt["status"] == "PARTIAL"
    assert receipt["missing_months"] == []


def test_partial_year_with_gap(tmp_path):
    files = [tmp_path / f"flights_2025_{m}.csv" for m in (1, 3)]

    receipt = ingest.verify_monthly_delivery(files, 2025, allow_partial=True)

    assert receipt["status"] == "INCOMPLETE"
    assert receipt["missing_months"] == [2]


def test_find_csv_files(tmp_path):
    for year, month in [(2024, 1), (2025, 1), (2025, 2)]:
        folder = tmp_path / str(year)
        folder.mkdir(exist_ok=True)
        (folder / f"On_Time_Reporting_Carrier_x_{year}_{month}.csv").touch()

    (tmp_path / "notes").mkdir() 

    found = ingest.find_csv_files(tmp_path)

    assert sorted(found) == [2024, 2025]
    assert len(found[2025]) == 2
    assert list(ingest.find_csv_files(tmp_path, [2025])) == [2025]


def test_find_csv_files_missing_year(tmp_path):
    (tmp_path / "2025").mkdir()
    (tmp_path / "2025" / "On_Time_Reporting_Carrier_x_2025_1.csv").touch()

    with pytest.raises(FileNotFoundError):
        ingest.find_csv_files(tmp_path, [2024])


def test_bronze_keeps_year_month_columns(
    spark, tmp_path, monkeypatch
):

    configure_flight_paths(monkeypatch, tmp_path)

    source = tmp_path / "flights_2025_7.csv"
    create_flight_csv(source, extra={"Year": "2025", "Month": "7"})

    ingest.ingest_file(spark, source)

    df = spark.read.parquet(str(tmp_path / "data" / "bronze" / "flights"))
    row = df.select("Year", "Month", "period").first()

    assert (row.Year, row.Month) == ("2025", "7")
    assert str(row.period) == "2025-07"


def test_required_columns_cover_silver():
    import clean

    assert set(clean.FLIGHT_COLUMNS) <= set(ingest.REQUIRED_COLUMNS)


def test_missing_column_fails(
    spark, tmp_path, monkeypatch
):
    configure_flight_paths(monkeypatch, tmp_path)

    source = tmp_path / "flights_2025_4.csv"
    create_flight_csv(source, drop=("ArrDelay",))

    with pytest.raises(ValueError, match="ArrDelay"):
        ingest.ingest_file(spark, source)

    bronze = tmp_path / "data" / "bronze"
    assert not (bronze / "flights" / "period=2025-04").exists()
    assert not (bronze / "manifests" / "flights_2025_04.json").exists()


def test_schema_hash():
    base = ingest.REQUIRED_COLUMNS

    assert ingest.schema_hash(base) == ingest.schema_hash(list(base))
    assert ingest.schema_hash(base) != ingest.schema_hash(
        [*base, "NewColumn"]
    )


def test_code_version_on_github(monkeypatch):
    import publish

    monkeypatch.setenv("GITHUB_SHA", "abc123")

    assert publish.code_version() == "abc123"


def test_old_year_missing_december(tmp_path):
    year_dir = tmp_path / "raw" / "2000"
    year_dir.mkdir(parents=True)
    for month in range(1, 12):
        (year_dir / f"On_Time_Reporting_Carrier_x_2000_{month}.csv").touch()

    with pytest.raises(RuntimeError, match="Incomplete delivery for 2000"):
        ingest.run(source_dir=tmp_path / "raw", data_dir=tmp_path / "out")

    receipt = json.loads(
        (tmp_path / "out" / "bronze" / "manifests" / "delivery_2000.json").read_text()
    )
    assert receipt["missing_months"] == [12]
