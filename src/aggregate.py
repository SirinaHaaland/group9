import json
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from publish import code_version, current_run, publish_run, write_json

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"


def performance_metrics(df, group_columns):
    completed = F.col("flight_status") == "completed"

    result = df.groupBy(*group_columns).agg(
        F.count("*").alias("total_flights"),
        F.sum(
            F.when(completed, 1).otherwise(0)
        ).alias("completed_flights"),
        F.sum(
            F.when(F.col("flight_status") == "cancelled", 1)
             .otherwise(0)
        ).alias("cancelled_flights"),
        F.sum(
            F.when(F.col("flight_status") == "diverted", 1)
             .otherwise(0)
        ).alias("diverted_flights"),
        F.avg(
            F.when(completed, F.col("arrival_delay"))
        ).alias("avg_arrival_delay_minutes"),
        F.avg(
            F.when(completed, F.col("departure_delay"))
        ).alias("avg_departure_delay_minutes"),
        F.sum(
            F.when(
                completed & (F.col("arrival_delay") >= 15), 1
            ).otherwise(0)
        ).alias("arrival_delayed_15_flights"),
    )

    return (
        result.withColumn(
            "cancellation_rate_pct",
            F.round(
                F.col("cancelled_flights")
                / F.col("total_flights") * 100, 2
            )
        ).withColumn(
            "arrival_delay_rate_pct",
            F.when(
                F.col("completed_flights") > 0,
                F.round(
                    F.col("arrival_delayed_15_flights")
                    / F.col("completed_flights") * 100, 2
                )
            )
        )
    )


def build_airline_performance(df):
    metrics = performance_metrics(df, ["year", "month", "airline"])
    metrics.createOrReplaceTempView("airline_month_metrics")

    result = df.sparkSession.sql("""
        SELECT
            *,
            RANK() OVER (
                PARTITION BY year, month
                ORDER BY arrival_delay_rate_pct ASC NULLS LAST
            ) AS punctuality_rank,
            COUNT(*) OVER (PARTITION BY year, month) AS airlines_in_month,
            LAG(year * 12 + month) OVER airline_months AS _previous_period,
            LAG(arrival_delay_rate_pct) OVER airline_months AS _previous_rate,
            SUM(cancelled_flights) OVER (
                PARTITION BY airline, year
                ORDER BY month
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS cancellations_ytd
        FROM airline_month_metrics
        WINDOW airline_months AS (PARTITION BY airline ORDER BY year, month)
    """)

    consecutive = F.col("_previous_period") == F.col("year") * 12 + F.col("month") - 1
    previous_rate = F.when(consecutive, F.col("_previous_rate"))

    return (
        result
        .withColumn("previous_month_delay_rate_pct", previous_rate)
        .withColumn(
            "delay_rate_change_pts",
            F.round(F.col("arrival_delay_rate_pct") - previous_rate, 2),
        )
        .drop("_previous_period", "_previous_rate")
    )


def build_airport_performance(df):
    return performance_metrics(
        df, ["year", "month", "origin", "origin_airport_name"]
    )


def build_daily_trends(df):
    metrics = performance_metrics(df, ["flight_date"])
    metrics.createOrReplaceTempView("daily_metrics")

    return df.sparkSession.sql("""
        SELECT
            *,
            ROUND(
                100 * SUM(arrival_delayed_15_flights) OVER last_7_days
                / SUM(completed_flights) OVER last_7_days, 2
            ) AS arrival_delay_rate_7d_pct,
            ROUND(
                100 * SUM(cancelled_flights) OVER last_7_days
                / SUM(total_flights) OVER last_7_days, 2
            ) AS cancellation_rate_7d_pct,
            COUNT(*) OVER last_7_days AS days_in_window
        FROM daily_metrics
        WINDOW last_7_days AS (
            ORDER BY datediff(flight_date, DATE'1970-01-01')
            RANGE BETWEEN 6 PRECEDING AND CURRENT ROW
        )
    """)


def source_metrics(df):

    row = df.agg(
        F.count("*").alias("total_flights"),
        F.sum(
            F.when(F.col("flight_status") == "completed", 1)
             .otherwise(0)
        ).alias("completed_flights"),
        F.sum(
            F.when(F.col("flight_status") == "cancelled", 1)
             .otherwise(0)
        ).alias("cancelled_flights"),
        F.sum(
            F.when(F.col("flight_status") == "diverted", 1)
             .otherwise(0)
        ).alias("diverted_flights"),
        F.sum(
            F.when(
                (F.col("flight_status") == "completed")
                & (F.col("arrival_delay") >= 15), 1
            ).otherwise(0)
        ).alias("arrival_delayed_15_flights"),
    ).first()

    return {field: row[field] for field in row.asDict()}


def validate_gold(df, expected, name):
    if isinstance(expected, int):
        expected = {"total_flights": expected}

    metrics = [
        "total_flights",
        "completed_flights",
        "cancelled_flights",
        "diverted_flights",
        "arrival_delayed_15_flights",
    ]

    actual = df.agg(*[
        F.sum(F.col(metric)).alias(metric)
        for metric in metrics
    ]).first().asDict()

    for metric, value in expected.items():
        if metric in metrics and actual[metric] != value:
            raise RuntimeError(
                f"{name}: {metric} mismatch: "
                f"expected {value}, got {actual[metric]}"
            )

    invalid = df.filter(
        ~(
            F.col("total_flights")
            == F.col("completed_flights")
            + F.col("cancelled_flights")
            + F.col("diverted_flights")
        )
        | ~F.col("cancellation_rate_pct").between(0, 100)
        | (
            (F.col("completed_flights") > 0)
            & (
                F.col("arrival_delay_rate_pct").isNull()
                | ~F.col("arrival_delay_rate_pct").between(0, 100)
            )
        )
        | (
            (F.col("completed_flights") == 0)
            & F.col("arrival_delay_rate_pct").isNotNull()
        )
        | (
            F.col("arrival_delayed_15_flights")
            > F.col("completed_flights")
        )
    ).count()

    if invalid:
        raise RuntimeError(
            f"{name}: {invalid} invalid aggregate rows"
        )

    print(f"VALIDATED: {name} ({actual['total_flights']:,} flights)")
    return actual


def load_silver_report(silver):
    run_dir = current_run(silver)
    report = json.loads((run_dir / "quality_report.json").read_text())

    if report.get("quality_gate", {}).get("status") != "PASS":
        raise RuntimeError(f"Silver run {run_dir.name} did not pass its quality gate")

    if report.get("silver_run_id") != run_dir.name:
        raise RuntimeError(
            f"Silver report belongs to run {report.get('silver_run_id')}, "
            f"not the published run {run_dir.name}"
        )

    return run_dir, report


def check_silver_run_id(flights, run_id):
    found = [
        row["_silver_run_id"]
        for row in flights.select("_silver_run_id").distinct().collect()
    ]

    if found != [run_id]:
        raise RuntimeError(
            f"Silver has rows from other runs: {found}, expected {run_id}"
        )


def run(data_dir=None):
    data_dir = Path(data_dir or DATA_DIR)
    silver = data_dir / "silver"
    gold = data_dir / "gold"
    start_time = time.time()

    silver_run, _report = load_silver_report(silver)
    silver_run_id = silver_run.name

    spark = (
        SparkSession.builder
        .master("local[4]")
        .appName("DAT535-Gold")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    datasets = {}

    try:
        flights = spark.read.parquet(str(silver_run / "flights"))
        check_silver_run_id(flights, silver_run_id)

        expected = source_metrics(flights)

        print(f"\nSilver records: {expected['total_flights']:,}")

        datasets = {
            name: df.cache()
            for name, df in {
                "airline_performance": build_airline_performance(flights),
                "airport_performance": build_airport_performance(flights),
                "daily_trends": build_daily_trends(flights),
            }.items()
        }

        for name, df in datasets.items():
            validate_gold(df, expected, name)

        run_id = str(uuid.uuid4())
        run_dir = gold / "runs" / run_id
        run_dir.mkdir(parents=True, exist_ok=False)

        outputs = {}

        for name, df in datasets.items():
            output = run_dir / name

            df.write.mode("errorifexists").parquet(str(output))

            written_table = spark.read.parquet(str(output))
            actual = validate_gold(written_table, expected, name)

            outputs[name] = {
                "path": str(output.resolve()),
                "aggregate_rows": written_table.count(),
                "metrics": actual,
            }

            print(f"Saved: {output}")

        manifest = {
            "run_id": run_id,
            "silver_run_id": silver_run_id,
            "code_version": code_version(),
            "created_utc": datetime.now(UTC).isoformat(),
            "source_metrics": expected,
            "datasets": outputs,
        }

        write_json(run_dir / "manifest.json", manifest)
        publish_run(
            gold, run_id, "manifest.json",
            silver_run_id=silver_run_id,
            code_version=manifest["code_version"],
        )

        print("\nGold processing complete.")
        print(f"Published run: {run_id}")

    finally:
        for df in datasets.values():
            df.unpersist()
        spark.stop()

    return {
        "status": "success",
        "run_id": run_id,
        "datasets": outputs,
        "elapsed_time": time.time() - start_time,
    }


if __name__ == "__main__":
    run()
