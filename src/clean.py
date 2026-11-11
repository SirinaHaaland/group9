import json
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from publish import code_version, publish_run, write_json

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
AIRPORT_OVERRIDES = ROOT / "config/reference/airport_overrides.csv"

FLIGHT_COLUMNS = [
    "FlightDate",
    "Reporting_Airline",
    "Flight_Number_Reporting_Airline",
    "Origin",
    "Dest",
    "CRSDepTime",
    "DepDelay",
    "ArrDelay",
    "Cancelled",
    "Diverted",
    "Distance",
    "CRSArrTime",
    "CRSElapsedTime",
    "DepTime",
    "ArrTime",
]

LINEAGE_COLUMNS = [
    "_source_file",
    "_ingestion_time",
    "_run_id",
]

FLIGHT_KEY = [
    "flight_date",
    "airline",
    "flight_number",
    "origin",
    "destination",
    "scheduled_departure",
]

MIN_DELAY_MIN = -240
MAX_DELAY_MIN = 7200        
MAX_DISTANCE_MILES = 6000

FILE_PERIOD = r"_(\d{4})_(\d{1,2})\.csv$"

QUALITY_LIMITS = {
    "max_rejected_pct": 1.0,
    "max_unmatched_airport_pct": 0.0,
    "max_time_mismatch_pct": 0.1,
    "max_identity_conflicts": 0,
    "max_missing_utc": 0,
    "min_time_check_coverage_pct": 99.0,
}


def prepare_flights(df):
    required = FLIGHT_COLUMNS + LINEAGE_COLUMNS
    missing = set(required) - set(df.columns)
    if missing:
        raise ValueError(f"Missing source columns: {sorted(missing)}")

    df = df.select(*FLIGHT_COLUMNS, *LINEAGE_COLUMNS)

    df = df.select(
        F.to_date("FlightDate").alias("flight_date"),
        F.upper(F.trim("Reporting_Airline")).alias("airline"),
        F.col("Flight_Number_Reporting_Airline")
            .cast("int").alias("flight_number"),
        F.upper(F.trim("Origin")).alias("origin"),
        F.upper(F.trim("Dest")).alias("destination"),
        F.col("CRSDepTime").cast("int").alias("scheduled_departure"),
        F.col("DepDelay").cast("double").alias("departure_delay"),
        F.col("ArrDelay").cast("double").alias("arrival_delay"),
        F.col("Cancelled").cast("int").alias("cancelled"),
        F.col("Diverted").cast("int").alias("diverted"),
        F.col("Distance").cast("double").alias("distance_miles"),
        F.col("CRSArrTime").cast("int").alias("scheduled_arrival"),
        F.col("CRSElapsedTime").cast("double").alias("scheduled_elapsed_min"),
        F.col("DepTime").cast("int").alias("actual_departure"),
        F.col("ArrTime").cast("int").alias("actual_arrival"),
        F.col("_source_file"),
        F.col("_ingestion_time"),
        F.col("_run_id")
    )

    df = df.withColumn(
        "flight_status",
        F.when(F.col("cancelled") == 1, "cancelled")
         .when(F.col("diverted") == 1, "diverted")
         .otherwise("completed")
    )

    rejection_rules = [
        (
            F.col("flight_date").isNull(),
            "invalid_flight_date"
        ),
        (
            F.col("airline").isNull() | (F.length("airline") == 0),
            "missing_airline"
        ),
        (
            F.col("flight_number").isNull(),
            "invalid_flight_number"
        ),
        (
            F.col("origin").isNull() | (F.length("origin") == 0),
            "missing_origin"
        ),
        (
            F.col("destination").isNull()
            | (F.length("destination") == 0),
            "missing_destination"
        ),
        (
            F.col("scheduled_departure").isNull()
            | ~(
                F.col("scheduled_departure").between(0, 2400)
                & ((F.col("scheduled_departure") % 100) < 60)
            ),
            "invalid_scheduled_departure"
        ),
        (
            F.col("distance_miles").isNull()
            | (F.col("distance_miles") <= 0),
            "invalid_distance"
        ),
        (
            F.col("cancelled").isNull()
            | ~F.col("cancelled").isin(0, 1),
            "invalid_cancelled"
        ),
        (
            F.col("diverted").isNull()
            | ~F.col("diverted").isin(0, 1),
            "invalid_diverted"
        ),
        (
            (F.col("cancelled") == 1)
            & (F.col("diverted") == 1),
            "conflicting_flight_status"
        ),
        (
            (F.col("flight_status") == "completed")
            & F.col("departure_delay").isNull(),
            "missing_departure_delay"
        ),
        (
            (F.col("flight_status") == "completed")
            & F.col("arrival_delay").isNull(),
            "missing_arrival_delay"
        ),
        (
            F.col("origin") == F.col("destination"),
            "same_origin_destination"
        ),
        (
            ~F.col("origin").rlike("^[A-Z0-9]{3}$")
            | ~F.col("destination").rlike("^[A-Z0-9]{3}$"),
            "invalid_airport_code"
        ),
        (
            F.col("distance_miles") > MAX_DISTANCE_MILES,
            "distance_out_of_range"
        ),
        (
            ~F.col("departure_delay").between(MIN_DELAY_MIN, MAX_DELAY_MIN)
            | ~F.col("arrival_delay").between(MIN_DELAY_MIN, MAX_DELAY_MIN),
            "delay_out_of_range"
        ),
        (
            (F.year("flight_date")
             != F.regexp_extract("_source_file", FILE_PERIOD, 1).cast("int"))
            | (F.month("flight_date")
               != F.regexp_extract("_source_file", FILE_PERIOD, 2).cast("int")),
            "date_outside_source_period"
        ),
    ]

    df = df.withColumn(
        "reject_reason",
        F.array_compact(
            F.array(*[
                F.when(F.coalesce(condition, F.lit(False)), F.lit(reason))
                for condition, reason in rejection_rules
            ])
        )
    )

    df = df.withColumn(
        "is_valid",
        F.size(F.col("reject_reason")) == 0
    )

    df = (
        df.withColumn("year", F.year("flight_date"))
          .withColumn("month", F.month("flight_date"))
          .withColumn("day_of_week", F.dayofweek("flight_date"))
          .withColumn(
              "departure_hour",
              F.floor(F.col("scheduled_departure") / 100) % 24
          )
          .withColumn(
              "arrival_delayed_15",
              F.when(
                  F.col("flight_status") == "completed",
                  (F.col("arrival_delay") >= 15).cast("int")
              )
          )
    )

    return df


def prepare_airports(df):

    required = ["IATA", "Name", "Country", "Latitude", "Longitude", "TzDatabaseTimezone"]
    missing = set(required) - set(df.columns)
    if missing:
        raise ValueError(f"Missing airport columns: {sorted(missing)}")

    airports = df.select(
        F.upper(F.trim("IATA")).alias("iata"),
        F.trim("Name").alias("airport_name"),
        F.trim("Country").alias("country"),
        F.col("Latitude").cast("double").alias("latitude"),
        F.col("Longitude").cast("double").alias("longitude"),
        F.when(
            F.col("TzDatabaseTimezone").isin("\\N", ""),
            F.lit(None)
        ).otherwise(
            F.trim("TzDatabaseTimezone")
        ).alias("timezone")
    )

    airports = airports.filter(
        F.col("iata").rlike("^[A-Z0-9]{3}$")
        & F.col("airport_name").isNotNull()
        & F.col("latitude").between(-90, 90)
        & F.col("longitude").between(-180, 180)
    ).dropDuplicates()

    unique_codes = (
        airports.groupBy("iata")
        .count()
        .filter(F.col("count") == 1)
        .select("iata")
    )

    return airports.join(unique_codes, "iata", "inner")


def add_airport_overrides(spark, airport_lookup):
    if not AIRPORT_OVERRIDES.is_file():
        raise FileNotFoundError(
            f"Missing airport overrides: {AIRPORT_OVERRIDES}"
        )

    overrides = (
        spark.read
        .option("header", True)
        .csv(str(AIRPORT_OVERRIDES))
        .select(
            F.upper(F.trim("iata")).alias("iata"),
            F.trim("airport_name").alias("airport_name"),
            F.trim("country").alias("country"),
            F.col("latitude").cast("double").alias("latitude"),
            F.col("longitude").cast("double").alias("longitude"),
            F.trim("timezone").alias("timezone"),
        )
    )

    invalid = overrides.filter(
        F.col("iata").isNull()
        | ~F.col("iata").rlike("^[A-Z0-9]{3}$")
        | F.col("airport_name").isNull()
        | (F.length("airport_name") == 0)
        | F.col("latitude").isNull()
        | ~F.col("latitude").between(-90, 90)
        | F.col("longitude").isNull()
        | ~F.col("longitude").between(-180, 180)
        | F.col("timezone").isNull()
        | (F.length("timezone") == 0)
    )

    if invalid.count() > 0:
        raise ValueError("Invalid airport override records")

    duplicate_codes = (
        overrides.groupBy("iata")
        .count()
        .filter(F.col("count") > 1)
    )

    if duplicate_codes.count() > 0:
        raise ValueError("Duplicate airport override codes")

    allowed_replacements = {"BIH"}

    replaced = {
        row["iata"]
        for row in overrides.join(
            airport_lookup.select("iata"),
            "iata",
            "inner"
        ).select("iata").collect()
    }

    unexpected = replaced - allowed_replacements

    if unexpected:
        raise ValueError(
            f"Unexpected airport override conflicts: {sorted(unexpected)}"
        )

    if replaced:
        print(f"Replaced airport references: {sorted(replaced)}")

    original = airport_lookup.join(
        overrides.select("iata"),
        "iata",
        "left_anti"
    )

    return original.unionByName(overrides)



def enrich_flights(flights, airports):
    origin_airports = airports.select(
        F.col("iata").alias("origin"),
        F.col("airport_name").alias("origin_airport_name"),
        F.col("country").alias("origin_country"),
        F.col("latitude").alias("origin_latitude"),
        F.col("longitude").alias("origin_longitude"),
        F.col("timezone").alias("origin_tz")
    )

    destination_airports = airports.select(
        F.col("iata").alias("destination"),
        F.col("airport_name").alias("destination_airport_name"),
        F.col("country").alias("destination_country"),
        F.col("latitude").alias("destination_latitude"),
        F.col("longitude").alias("destination_longitude"),
        F.col("timezone").alias("destination_tz")
    )

    return (
        flights.join(F.broadcast(origin_airports), "origin", "left")
               .join(
                   F.broadcast(destination_airports),
                   "destination",
                   "left"
               )
    )



def hhmm_to_minutes(column):
    c = F.col(column)
    return (
        F.floor(c / 100) * 60
        + (c % 100)
    )


def add_minutes(timestamp, minutes):
    return F.timestamp_seconds(
        F.unix_timestamp(timestamp)
        + (minutes * 60).cast("long")
    )


def local_hhmm(timestamp, timezone_column):
    local = F.from_utc_timestamp(timestamp, F.col(timezone_column))
    return F.hour(local) * 100 + F.minute(local)


def add_utc_times(df):
    completed = F.col("flight_status") == "completed"
    departed = F.col("flight_status") != "cancelled"

    df = (
        df.withColumn(
            "scheduled_departure_local",
            add_minutes(
                F.col("flight_date").cast("timestamp"),
                hhmm_to_minutes("scheduled_departure")
            )
        )
        .withColumn(
            "scheduled_departure_utc",
            F.to_utc_timestamp(
                "scheduled_departure_local",
                F.col("origin_tz")
            )
        )
        .withColumn(
            "scheduled_arrival_utc",
            F.when(
                F.col("scheduled_elapsed_min") > 0,
                add_minutes(
                    F.col("scheduled_departure_utc"),
                    F.col("scheduled_elapsed_min")
                )
            )
        )
        .withColumn(
            "actual_departure_utc",
            F.when(
                departed,
                add_minutes(
                    F.col("scheduled_departure_utc"),
                    F.col("departure_delay")
                )
            )
        )
        .withColumn(
            "actual_arrival_utc",
            F.when(
                completed,
                add_minutes(
                    F.col("scheduled_arrival_utc"),
                    F.col("arrival_delay")
                )
            )
        )
        .withColumn(
            "scheduled_elapsed_valid",
            F.coalesce(
                F.col("scheduled_elapsed_min") > 0,
                F.lit(False)
            )
        )
    )

    return (
        df.withColumn(
            "scheduled_arrival_consistent",
            local_hhmm(
                "scheduled_arrival_utc",
                "destination_tz"
            ) == (F.col("scheduled_arrival") % 2400)
        )
        .withColumn(
            "actual_departure_consistent",
            local_hhmm(
                "actual_departure_utc",
                "origin_tz"
            ) == (F.col("actual_departure") % 2400)
        )
        .withColumn(
            "actual_arrival_consistent",
            local_hhmm(
                "actual_arrival_utc",
                "destination_tz"
            ) == (F.col("actual_arrival") % 2400)
        )
    )


CHECKED_COLUMNS = {
    "FlightDate": ("flight_date", "date"),
    "Reporting_Airline": ("airline", "text"),
    "Flight_Number_Reporting_Airline": ("flight_number", "number"),
    "Origin": ("origin", "text"),
    "Dest": ("destination", "text"),
    "CRSDepTime": ("scheduled_departure", "number"),
    "DepTime": ("actual_departure", "number"),
    "DepDelay": ("departure_delay", "number"),
    "CRSArrTime": ("scheduled_arrival", "number"),
    "ArrTime": ("actual_arrival", "number"),
    "ArrDelay": ("arrival_delay", "number"),
    "Cancelled": ("cancelled", "number"),
    "Diverted": ("diverted", "number"),
    "CRSElapsedTime": ("scheduled_elapsed_min", "number"),
    "Distance": ("distance_miles", "number"),
}


def bronze_null_counts():
    counts = []

    for raw, (_, kind) in CHECKED_COLUMNS.items():
        value = F.trim(F.col(raw))
        missing = F.col(raw).isNull() | (value == "")
        counts.append(F.sum(missing.cast("int")).alias(f"{raw}__missing"))

        if kind != "text":
            parsed = F.to_date(value) if kind == "date" else value.cast("double")
            counts.append(
                F.sum((~missing & parsed.isNull()).cast("int"))
                .alias(f"{raw}__unparseable")
            )

    return counts


def silver_null_counts():
    return [
        F.sum(F.col(silver).isNull().cast("int")).alias(f"{silver}__missing")
        for silver, _ in CHECKED_COLUMNS.values()
    ]


def column_quality(bronze_counts, silver_counts):
    return {
        raw: {
            "silver_column": silver,
            "bronze_missing": bronze_counts[f"{raw}__missing"],
            "bronze_unparseable": bronze_counts.get(f"{raw}__unparseable", 0),
            "silver_missing": silver_counts[f"{silver}__missing"],
        }
        for raw, (silver, _) in CHECKED_COLUMNS.items()
    }


def drop_duplicates(flights):
    return flights.dropDuplicates(FLIGHT_COLUMNS)


def count_duplicate_keys(df):
    return (
        df.groupBy(*FLIGHT_KEY)
        .count()
        .filter(F.col("count") > 1)
        .agg(F.coalesce(F.sum(F.col("count") - 1), F.lit(0)).alias("n"))
        .first()["n"]
    )


def percent(count, total):
    return 100 * count / total if total else 0.0


def quality_gate(report, limits):
    measures = {
        "rejected_pct": percent(
            report["rejected_records"], report["source_records"]
        ),
        "unmatched_airport_pct": percent(
            report["unmatched_origin_records"]
            + report["unmatched_destination_records"],
            report["accepted_records"],
        ),
        "time_mismatch_pct": percent(
            report["scheduled_arrival_mismatches"]
            + report["actual_departure_mismatches"]
            + report["actual_arrival_mismatches"],
            report["scheduled_arrival_checked"]
            + report["actual_departure_checked"]
            + report["actual_arrival_checked"],
        ),
        "identity_conflicts": report["identity_conflicts"],
        "missing_utc": report["missing_utc"],
        "time_check_coverage_pct": percent(
            report["scheduled_arrival_checked"], report["accepted_records"]
        ),
    }

    failures = []

    for key, limit in limits.items():
        kind, name = key.split("_", 1)
        value = measures[name]

        if kind == "max" and value > limit:
            failures.append(f"{name}={value:.4f} > {limit}")
        elif kind == "min" and value < limit:
            failures.append(f"{name}={value:.4f} < {limit}")

    if report["source_records"] == 0:
        failures.append("no source records")

    return {
        "status": "FAIL" if failures else "PASS",
        "measures": measures,
        "limits": limits,
        "failures": failures,
    }


def save_history(path, report):
    gate = report["quality_gate"]
    entry = {
        "created_utc": report["created_utc"],
        "silver_run_id": report["silver_run_id"],
        "code_version": report["code_version"],
        "gate_status": gate["status"],
        "failures": gate["failures"],
        "source_records": report["source_records"],
        "accepted_records": report["accepted_records"],
        "rejected_records": report["rejected_records"],
        "duplicates_removed": report["duplicates_removed"],
        **gate["measures"],
    }

    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("a") as f:
        f.write(json.dumps(entry) + "\n")


def run(data_dir=None):
    data_dir = Path(data_dir or DATA_DIR)
    bronze = data_dir / "bronze"
    silver = data_dir / "silver"
    start_time = time.time()

    spark = (
        SparkSession.builder
        .master("local[4]")
        .appName("DAT535-Silver")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    prepared = None

    try:
        flights = spark.read.parquet(
            str(bronze / "flights")
        )
        airports = spark.read.parquet(
            str(bronze / "airports")
        )

        bronze_counts = flights.agg(
            F.count("*").alias("rows"), *bronze_null_counts()
        ).first().asDict()
        original_count = bronze_counts["rows"]

        silver_run_id = str(uuid.uuid4())
        processed_at = datetime.now(UTC).isoformat()
        run_dir = silver / "runs" / silver_run_id

        prepared = (
            prepare_flights(drop_duplicates(flights))
            .withColumn("_processed_at", F.lit(processed_at))
            .withColumn("_silver_run_id", F.lit(silver_run_id))
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        rejected = prepared.filter(~F.col("is_valid"))
        accepted = prepared.filter(F.col("is_valid"))

        rejected_count = rejected.count()
        accepted_count = accepted.count()
        duplicates_removed = original_count - (accepted_count + rejected_count)
        identity_conflicts = count_duplicate_keys(prepared)

        airport_lookup = prepare_airports(airports)
        airport_lookup = add_airport_overrides(spark, airport_lookup)

        enriched = add_utc_times(
            enrich_flights(accepted, airport_lookup)
        )

        (
            enriched.drop("is_valid", "reject_reason")
            .repartition("year", "month")
            .write.mode("errorifexists")
            .partitionBy("year", "month")
            .parquet(str(run_dir / "flights"))
        )

        written_flights = spark.read.parquet(str(run_dir / "flights"))

        def count_if(condition):
            return F.sum(condition.cast("int"))

        metrics = written_flights.agg(
            *silver_null_counts(),
            F.count("*").alias("enriched_records"),
            count_if(F.col("origin_airport_name").isNull()).alias("unmatched_origin_records"),
            count_if(F.col("destination_airport_name").isNull()).alias("unmatched_destination_records"),
            count_if(F.col("scheduled_departure_utc").isNull()).alias("missing_utc"),
            count_if(~F.col("scheduled_arrival_consistent")).alias("scheduled_arrival_mismatches"),
            count_if(~F.col("actual_departure_consistent")).alias("actual_departure_mismatches"),
            count_if(~F.col("actual_arrival_consistent")).alias("actual_arrival_mismatches"),
            count_if(
                F.col("scheduled_elapsed_min").isNull() | (F.col("scheduled_elapsed_min") <= 0)
            ).alias("invalid_scheduled_elapsed"),
            F.count("scheduled_arrival_consistent").alias("scheduled_arrival_checked"),
            F.count("actual_departure_consistent").alias("actual_departure_checked"),
            F.count("actual_arrival_consistent").alias("actual_arrival_checked"),
        ).first().asDict()

        if accepted_count != metrics["enriched_records"]:
            raise RuntimeError(
                "Airport enrichment changed the flight row count!"
            )

        (
            rejected.write.mode("errorifexists")
            .parquet(str(run_dir / "rejected"))
        )

        (
            airport_lookup.write.mode("errorifexists")
            .parquet(str(run_dir / "airports"))
        )

        reason_counts = (
            rejected
            .select(F.explode("reject_reason").alias("reason"))
            .groupBy("reason")
            .count()
            .collect()
        )

        rejection_summary = {
            row["reason"]: row["count"]
            for row in reason_counts
        }

        report = {
            "silver_run_id": silver_run_id,
            "code_version": code_version(),
            "created_utc": datetime.now(UTC).isoformat(),
            "source_records": original_count,
            "duplicates_removed": duplicates_removed,
            "identity_conflicts": identity_conflicts,
            "accepted_records": accepted_count,
            "rejected_records": rejected_count,
            "rejection_reasons": rejection_summary,
            **{k: v for k, v in metrics.items() if "__" not in k},
            "before_after": column_quality(bronze_counts, metrics),
        }
        report["quality_gate"] = quality_gate(report, QUALITY_LIMITS)

        write_json(run_dir / "quality_report.json", report)
        save_history(silver / "quality_history.jsonl", report)

        print("\n=== SILVER QUALITY REPORT ===")
        print(json.dumps(report, indent=2))

        if report["quality_gate"]["status"] != "PASS":
            raise RuntimeError(
                f"Silver quality gate failed: {report['quality_gate']['failures']} "
                f"(run {silver_run_id} not published)"
            )

        publish_run(
            silver, silver_run_id, "quality_report.json",
            code_version=report["code_version"],
            accepted_records=accepted_count,
        )


        print("\nSilver processing complete.")

    finally:
        if prepared is not None:
            prepared.unpersist()
        spark.stop()

    return {
        "status": "success",
        "report": report,
        "elapsed_time": time.time() - start_time,
    }


if __name__ == "__main__":
    run()
