import json
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from aggregate import (
    build_airline_performance,
    build_daily_trends,
    check_silver_run_id,
    performance_metrics,
    validate_gold,
)
from clean import (
    add_utc_times,
    bronze_null_counts,
    column_quality,
    count_duplicate_keys,
    drop_duplicates,
    enrich_flights,
    prepare_airports,
    prepare_flights,
    quality_gate,
    save_history,
    silver_null_counts,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("DAT535-Pipeline-Tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    session.conf.set("spark.sql.session.timeZone", "UTC")

    yield session
    session.stop()


def flight_row(**overrides):
    row = {
        "FlightDate": "2025-01-01",
        "Reporting_Airline": "AA",
        "Flight_Number_Reporting_Airline": "100",
        "Origin": "JFK",
        "Dest": "LAX",
        "CRSDepTime": "0930",
        "DepDelay": "5",
        "ArrDelay": "10",
        "Cancelled": "0",
        "Diverted": "0",
        "Distance": "2475",
        "CRSArrTime": "1245",
        "CRSElapsedTime": "375",
        "DepTime": "0935",
        "ArrTime": "1255",
        "_source_file": "flights_2025_1.csv",
        "_ingestion_time": "2025-01-02T00:00:00+00:00",
        "_run_id": "test-bronze-run",
    }
    row.update(overrides)
    return row


def bronze_frame(spark, rows):
    columns = list(flight_row().keys())

    schema = StructType([
        StructField(c, StringType(), True)
        for c in columns
    ])

    data = [
        tuple(row.get(c) for c in columns)
        for row in rows
    ]

    return spark.createDataFrame(data, schema=schema)


def process_flights(spark, rows):
    return prepare_flights(bronze_frame(spark, rows))


def first_with_text_timestamps(df):
    columns = [
        F.date_format(field.name, "yyyy-MM-dd HH:mm:ss").alias(field.name)
        if isinstance(field.dataType, TimestampType)
        else F.col(field.name)
        for field in df.schema.fields
    ]
    return df.select(*columns).first()

# SILVER: FLIGHT VALIDATION

@pytest.mark.parametrize(
    "changes,status",
    [
        ({}, "completed"),
        ({"ArrDelay": "-20"}, "completed"),
        (
            {"Cancelled": "1", "DepDelay": None,
             "ArrDelay": None},
            "cancelled",
        ),
        (
            {"Diverted": "1", "ArrDelay": None},
            "diverted",
        ),
    ],
)
def test_valid_flights(spark, changes, status):
    df = process_flights(spark, [flight_row(**changes)])
    result = df.first()

    assert result.is_valid is True
    assert result.flight_status == status


@pytest.mark.parametrize(
    "changes",
    [
        {"FlightDate": None},
        {"Origin": None},
        {"Distance": "-10"},
        {"Distance": "0"},
        {"Cancelled": "2"},
        {"CRSDepTime": "2460"},
        {"CRSDepTime": None},
        {"Cancelled": "1", "Diverted": "1"},
        {"ArrDelay": None},
        {"Dest": "JFK"},                
        {"Origin": "J1"},               
        {"Distance": "99999"},
        {"ArrDelay": "99999"},
        {"FlightDate": "2025-02-01"},   
    ],
)
def test_invalid_flights(spark, changes):
    df = process_flights(spark, [flight_row(**changes)])
    result = df.first()

    assert result.is_valid is False

@pytest.mark.parametrize("elapsed", ["-60", None])
def test_invalid_elapsed_flagged(spark, elapsed):
    df = process_flights(
        spark,
        [flight_row(CRSElapsedTime=elapsed)]
    )

    df = (
        df.withColumn("origin_tz", F.lit("America/New_York"))
          .withColumn("destination_tz", F.lit("America/Los_Angeles"))
    )

    row = first_with_text_timestamps(add_utc_times(df))

    assert row.is_valid is True
    assert row.scheduled_elapsed_valid is False
    assert row.scheduled_arrival_utc is None
    assert row.actual_arrival_utc is None

def test_cancelled_flight_allows_missing_elapsed(spark):
    df = process_flights(
        spark,
        [flight_row(
            Cancelled="1",
            DepDelay=None,
            ArrDelay=None,
            CRSElapsedTime=None,
        )],
    )

    assert df.first().is_valid is True

def test_negative_delay_is_preserved(spark):
    df = process_flights(
        spark, [flight_row(DepDelay="-5", ArrDelay="-15")]
    )
    row = df.first()

    assert row.is_valid is True
    assert row.departure_delay == -5.0
    assert row.arrival_delay == -15.0
    assert row.arrival_delayed_15 == 0


def test_cancelled_flight_has_no_arrival_indicator(spark):
    df = process_flights(
        spark,
        [flight_row(Cancelled="1", ArrDelay=None)],
    )
    row = df.first()

    assert row.is_valid is True
    assert row.arrival_delayed_15 is None


def test_rejection_accounting(spark):
    df = process_flights(
        spark,
        [
            flight_row(),
            flight_row(FlightDate=None),
            flight_row(Distance="-1"),
        ],
    )

    accepted = df.filter(F.col("is_valid"))
    rejected = df.filter(~F.col("is_valid"))

    assert accepted.count() == 1
    assert rejected.count() == 2
    assert accepted.count() + rejected.count() == df.count()


# SILVER: AIRPORT ENRICHMENT

def airport_row(iata, name, latitude="40", longitude="-73", timezone="America/New_York"):
    return (
        iata, name, "United States", latitude, longitude, timezone
    )


def airport_lookup(spark, rows):
    columns = [
        "IATA", "Name", "Country", "Latitude", "Longitude", "TzDatabaseTimezone"
    ]
    df = spark.createDataFrame(rows, columns)
    return prepare_airports(df)


def test_airport_enrichment_preserves_flights(spark):
    flights = process_flights(
        spark,
        [
            flight_row(Origin="JFK", Dest="LAX"),
            flight_row(Origin="XWA", Dest="LAX"),
        ],
    )

    airports = airport_lookup(
        spark,
        [
            airport_row("JFK", "John F Kennedy Airport"),
            airport_row("LAX", "Los Angeles Airport"),
        ],
    )

    result = enrich_flights(flights, airports)

    assert result.count() == 2

    unmatched = result.filter(
        F.col("origin_airport_name").isNull()
    )
    assert unmatched.count() == 1
    assert unmatched.first().origin == "XWA"


def test_ambiguous_airport_codes_are_excluded(spark):
    airports = airport_lookup(
        spark,
        [
            airport_row("JFK", "Airport A"),
            airport_row("JFK", "Airport B"),
            airport_row("LAX", "Los Angeles Airport"),
        ],
    )

    assert airports.filter(F.col("iata") == "JFK").count() == 0
    assert airports.filter(F.col("iata") == "LAX").count() == 1


def test_invalid_airport_coordinates(spark):
    airports = airport_lookup(
        spark,
        [
            airport_row("AAA", "Valid Airport", "40", "-73"),
            airport_row("BBB", "Invalid Airport", "120", "-73"),
        ],
    )

    assert airports.count() == 1
    assert airports.first().iata == "AAA"


# GOLD: AGGREGATION CORRECTNESS

def test_gold_rates(spark):
    flights = process_flights(
        spark,
        [
            flight_row(
                Flight_Number_Reporting_Airline="100",
                ArrDelay="20",
            ),
            flight_row(
                Flight_Number_Reporting_Airline="101",
                ArrDelay="-5",
            ),
            flight_row(
                Flight_Number_Reporting_Airline="102",
                Cancelled="1",
                ArrDelay=None,
            ),
            flight_row(
                Flight_Number_Reporting_Airline="103",
                Diverted="1",
                ArrDelay=None,
            ),
        ],
    )

    gold = performance_metrics(flights, ["airline"])
    row = gold.first()

    assert row.total_flights == 4
    assert row.completed_flights == 2
    assert row.cancelled_flights == 1
    assert row.diverted_flights == 1

    assert row.cancellation_rate_pct == 25.0

    assert row.arrival_delay_rate_pct == 50.0

    assert row.avg_arrival_delay_minutes == pytest.approx(7.5)

    validate_gold(gold, 4, "test_gold_rates")


def test_gold_with_no_completed_flights(spark):
    flights = process_flights(
        spark,
        [
            flight_row(Cancelled="1", ArrDelay=None),
        ],
    )

    row = performance_metrics(flights, ["airline"]).first()

    assert row.completed_flights == 0
    assert row.cancellation_rate_pct == 100.0
    assert row.arrival_delay_rate_pct is None


def test_midnight_2400(spark):
    df = process_flights(
        spark,
        [flight_row(
            FlightDate="2025-07-24",
            _source_file="flights_2025_7.csv",
            CRSDepTime="2400"
        )]
    )

    df = (
        df.withColumn("origin_tz", F.lit("America/New_York"))
          .withColumn("destination_tz", F.lit("America/Los_Angeles"))
    )

    row = first_with_text_timestamps(add_utc_times(df))

    assert row.is_valid is True
    assert row.scheduled_departure == 2400
    assert row.departure_hour == 0

    assert str(row.scheduled_departure_local) == (
        "2025-07-25 00:00:00"
    )

    assert str(row.scheduled_departure_utc) == (
        "2025-07-25 04:00:00"
    )



def test_rejection_reason(spark):
    df = process_flights(
        spark,
        [flight_row(ArrDelay=None)]
    )

    row = df.first()

    assert row.is_valid is False
    assert "missing_arrival_delay" in row.reject_reason



def test_gold_reconciliation_detects_wrong_totals(spark):
    flights = process_flights(
        spark,
        [
            flight_row(ArrDelay="20"),
            flight_row(
                Flight_Number_Reporting_Airline="101",
                Cancelled="1",
                ArrDelay=None,
            ),
        ],
    )

    gold = performance_metrics(flights, ["airline"])

    wrong_expected = {
        "total_flights": 2,
        "cancelled_flights": 0,
    }

    with pytest.raises(RuntimeError, match="cancelled_flights"):
        validate_gold(gold, wrong_expected, "reconciliation_test")


def test_red_eye_crosses_midnight_in_utc(spark):
    df = spark.createDataFrame(
        [
            (
                "2025-01-15",
                "America/Los_Angeles",
                "America/New_York",
                2230,
                650,
                320.0,
                2240,
                715,
                10.0,
                25.0,
                "completed",
            )
        ],
        [
            "flight_date",
            "origin_tz",
            "destination_tz",
            "scheduled_departure",
            "scheduled_arrival",
            "scheduled_elapsed_min",
            "actual_departure",
            "actual_arrival",
            "departure_delay",
            "arrival_delay",
            "flight_status",
        ],
    ).withColumn(
        "flight_date",
        F.to_date("flight_date")
    )

    row = first_with_text_timestamps(add_utc_times(df))

    assert str(row.scheduled_departure_utc) == (
        "2025-01-16 06:30:00"
    )

    assert str(row.scheduled_arrival_utc) == (
        "2025-01-16 11:50:00"
    )

    assert str(row.actual_departure_utc) == (
        "2025-01-16 06:40:00"
    )

    assert str(row.actual_arrival_utc) == (
        "2025-01-16 12:15:00"
    )

    assert row.scheduled_arrival_consistent is True
    assert row.actual_departure_consistent is True
    assert row.actual_arrival_consistent is True


def test_cancelled_flight_has_no_actual_utc(spark):
    df = process_flights(
        spark,
        [
            flight_row(
                Cancelled="1",
                DepDelay=None,
                ArrDelay=None,
                DepTime=None,
                ArrTime=None,
            )
        ],
    )

    df = (
        df.withColumn("origin_tz", F.lit("America/New_York"))
          .withColumn("destination_tz", F.lit("America/Los_Angeles"))
    )

    row = first_with_text_timestamps(add_utc_times(df))

    assert row.is_valid is True
    assert row.scheduled_departure_utc is not None
    assert row.actual_departure_utc is None
    assert row.actual_arrival_utc is None

    assert row.actual_departure_consistent is None
    assert row.actual_arrival_consistent is None

def test_departure_delay_crosses_midnight(spark):
    df = process_flights(
        spark,
        [flight_row(
            FlightDate="2025-01-15",
            CRSDepTime="2350",
            DepDelay="30",
            DepTime="0020",
        )]
    )

    df = (
        df.withColumn("origin_tz", F.lit("America/New_York"))
          .withColumn("destination_tz", F.lit("America/Los_Angeles"))
    )

    row = first_with_text_timestamps(add_utc_times(df))

    assert row.actual_departure_utc == "2025-01-16 05:20:00"
    assert row.actual_departure_consistent is True


def test_silver_preserves_bronze_lineage(spark):
    df = process_flights(spark, [flight_row()])
    row = df.first()

    assert row._source_file == "flights_2025_1.csv"
    assert row._ingestion_time == "2025-01-02T00:00:00+00:00"
    assert row._run_id == "test-bronze-run"

# SILVER: DUPLICATES, QUALITY GATE, PUBLISHING

def test_drop_duplicates_ignores_lineage(spark):
    df = bronze_frame(spark, [
        flight_row(),
        flight_row(_run_id="rerun"),    
        flight_row(DepDelay="7"),       
    ])

    assert drop_duplicates(df).count() == 2


def test_identity_conflicts(spark):
    df = process_flights(spark, [flight_row(), flight_row(DepDelay="7")])

    assert count_duplicate_keys(df) == 1


GATE_LIMITS = {
    "max_rejected_pct": 1.0,
    "max_unmatched_airport_pct": 0.0,
    "max_time_mismatch_pct": 0.1,
    "max_identity_conflicts": 0,
    "max_missing_utc": 0,
    "min_time_check_coverage_pct": 99.0,
}


def gate_report(**overrides):
    report = {
        "source_records": 1000,
        "accepted_records": 990,
        "rejected_records": 10,
        "identity_conflicts": 0,
        "unmatched_origin_records": 0,
        "unmatched_destination_records": 0,
        "scheduled_arrival_mismatches": 0,
        "actual_departure_mismatches": 0,
        "actual_arrival_mismatches": 0,
        "scheduled_arrival_checked": 990,
        "actual_departure_checked": 990,
        "actual_arrival_checked": 990,
        "missing_utc": 0,
    }
    report.update(overrides)
    return report


def test_quality_gate_pass():
    assert quality_gate(gate_report(), GATE_LIMITS)["status"] == "PASS"


@pytest.mark.parametrize(
    "overrides",
    [
        {"rejected_records": 50},
        {"unmatched_origin_records": 1},
        {"actual_arrival_mismatches": 5},
        {"identity_conflicts": 1},
        {"source_records": 0},
        {"missing_utc": 1},
        {"scheduled_arrival_checked": 0, "actual_departure_checked": 0,
         "actual_arrival_checked": 0},
        {"scheduled_arrival_checked": 900},   
    ],
)
def test_quality_gate_fail(overrides):
    result = quality_gate(gate_report(**overrides), GATE_LIMITS)

    assert result["status"] == "FAIL"
    assert result["failures"]


def test_airline_punctuality_rank(spark):
    flights = process_flights(spark, [
        flight_row(Reporting_Airline="AA", ArrDelay="30"),
        flight_row(Reporting_Airline="AA", ArrDelay="45"),
        flight_row(Reporting_Airline="DL", ArrDelay="0"),
        flight_row(Reporting_Airline="DL", ArrDelay="-3"),
        flight_row(
            Reporting_Airline="UA", Cancelled="1",
            DepDelay=None, ArrDelay=None,
        ),
    ])

    rows = {
        r.airline: r
        for r in build_airline_performance(flights).collect()
    }

    assert rows["DL"].punctuality_rank == 1
    assert rows["AA"].punctuality_rank == 2
    assert rows["UA"].punctuality_rank == 3
    assert rows["UA"].arrival_delay_rate_pct is None
    assert {r.airlines_in_month for r in rows.values()} == {3}


def test_rolling_7_days(spark):
    flights = process_flights(spark, [
        flight_row(
            FlightDate=f"2025-01-{day:02d}",
            ArrDelay="20" if day == 1 else "0",
        )
        for day in range(1, 9)
    ])

    rows = {
        r.flight_date.day: r
        for r in build_daily_trends(flights).collect()
    }

    assert rows[1].days_in_window == 1
    assert rows[1].arrival_delay_rate_7d_pct == 100.0

    assert rows[7].days_in_window == 7
    assert rows[7].arrival_delay_rate_7d_pct == pytest.approx(14.29)

    assert rows[8].days_in_window == 7
    assert rows[8].arrival_delay_rate_7d_pct == 0.0
    assert rows[8].cancellation_rate_7d_pct == 0.0


def test_gold_totals_with_window_columns(spark):
    flights = process_flights(spark, [
        flight_row(Reporting_Airline="AA"),
        flight_row(Reporting_Airline="DL", Cancelled="1",
                   DepDelay=None, ArrDelay=None),
    ])

    validate_gold(build_airline_performance(flights), 2, "airline")
    validate_gold(build_daily_trends(flights), 2, "daily")


def test_quality_history(tmp_path):
    path = tmp_path / "silver" / "quality_history.jsonl"

    for run_id, rejected in [("run-1", 10), ("run-2", 50)]:
        report = gate_report(rejected_records=rejected)
        report.update(
            silver_run_id=run_id,
            created_utc="2025-01-01T00:00:00+00:00",
            code_version="abc123",
            duplicates_removed=0,
        )
        report["quality_gate"] = quality_gate(report, GATE_LIMITS)
        save_history(path, report)

    lines = [json.loads(line) for line in path.read_text().splitlines()]

    assert [line["silver_run_id"] for line in lines] == ["run-1", "run-2"]
    assert [line["gate_status"] for line in lines] == ["PASS", "FAIL"]
    assert lines[1]["rejected_pct"] == 5.0
    assert lines[0]["code_version"] == "abc123"


# SILVER: BEFORE/AFTER COLUMN PROFILE

def test_column_quality_counts(spark):
    rows = [
        flight_row(),
        flight_row(DepDelay="abc"),       
        flight_row(ArrDelay=""),       
    ]
    bronze = bronze_frame(spark, rows).agg(*bronze_null_counts()).first()

    silver = (
        process_flights(spark, rows)
        .agg(*silver_null_counts())
        .first()
    )

    profile = column_quality(bronze.asDict(), silver.asDict())

    assert profile["DepDelay"]["silver_column"] == "departure_delay"
    assert profile["DepDelay"]["bronze_unparseable"] == 1
    assert profile["DepDelay"]["silver_missing"] == 1   
    assert profile["ArrDelay"]["bronze_missing"] == 1
    assert profile["Origin"]["bronze_unparseable"] == 0   
    assert profile["FlightDate"]["bronze_missing"] == 0


def test_all_limits_checked():
    from clean import QUALITY_LIMITS

    result = quality_gate(gate_report(), QUALITY_LIMITS)

    assert result["status"] == "PASS"
    assert {key.split("_", 1)[1] for key in QUALITY_LIMITS} <= set(result["measures"])


def test_rolling_window_calendar_days(spark):
    flights = process_flights(spark, [
        flight_row(FlightDate=f"2025-01-{day:02d}", ArrDelay="20")
        for day in (1, 2)
    ] + [flight_row(FlightDate="2025-01-09", ArrDelay="0")])

    rows = {r.flight_date.day: r for r in build_daily_trends(flights).collect()}

    assert rows[2].days_in_window == 2
    assert rows[9].days_in_window == 1
    assert rows[9].arrival_delay_rate_7d_pct == 0.0


def test_mixed_silver_runs(spark):
    single = spark.createDataFrame([("run-a",), ("run-a",)], ["_silver_run_id"])
    mixed = spark.createDataFrame([("run-a",), ("run-b",)], ["_silver_run_id"])
    missing = spark.createDataFrame([("run-a",), (None,)], "_silver_run_id string")

    check_silver_run_id(single, "run-a")

    for df in (mixed, missing):
        with pytest.raises(RuntimeError, match="other runs"):
            check_silver_run_id(df, "run-a")


def test_airline_lag_and_ytd(spark):
    def flight(date, late, cancelled="0"):
        year, month = int(date[:4]), int(date[5:7])
        return flight_row(
            FlightDate=date,
            _source_file=f"flights_{year}_{month}.csv",
            ArrDelay=None if cancelled == "1" else ("30" if late else "0"),
            DepDelay=None if cancelled == "1" else "0",
            Cancelled=cancelled,
        )

    flights = process_flights(spark, [
        flight("2024-12-05", late=True),
        flight("2024-12-06", late=False, cancelled="1"),
        flight("2025-01-05", late=True),
        flight("2025-01-06", late=False),
        flight("2025-01-07", late=False, cancelled="1"),
        flight("2025-02-05", late=False),
        flight("2025-02-06", late=False, cancelled="1"),
        flight("2025-04-05", late=True),
    ])

    rows = {
        (r.year, r.month): r
        for r in build_airline_performance(flights).collect()
    }

    assert rows[(2025, 1)].previous_month_delay_rate_pct == 100.0
    assert rows[(2025, 1)].delay_rate_change_pts == -50.0
    assert rows[(2025, 1)].cancellations_ytd == 1

    assert rows[(2025, 2)].previous_month_delay_rate_pct == 50.0
    assert rows[(2025, 2)].cancellations_ytd == 2

    assert rows[(2025, 4)].previous_month_delay_rate_pct is None
    assert rows[(2025, 4)].delay_rate_change_pts is None
    assert rows[(2025, 4)].cancellations_ytd == 2

    assert rows[(2024, 12)].previous_month_delay_rate_pct is None
    assert rows[(2024, 12)].cancellations_ytd == 1
