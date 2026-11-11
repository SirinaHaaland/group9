import contextlib
import io
import os
import statistics
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from publish import current_run

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = Path(os.getenv("DAT535_OUTPUT_DIR") or ROOT / "data")
PERF_DIR = (Path(os.environ["DAT535_OUTPUT_DIR"]) if os.getenv("DAT535_OUTPUT_DIR") else ROOT) / "logs" / "performance"


def plan(df):
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        df.explain(mode="formatted")
    return buffer.getvalue()


def median_time(df, repeats=5):
    df.write.format("noop").mode("overwrite").save()  # warm-up
    times = []
    for _ in range(repeats):
        start = time.perf_counter()
        df.write.format("noop").mode("overwrite").save()
        times.append(time.perf_counter() - start)
    return statistics.median(times)


def plan_line(text, word):
    return next((line.strip(" :+-") for line in text.splitlines() if word in line), "not found")


def join_flights(flights, airports, use_broadcast):
    origin = airports.select(F.col("iata").alias("origin"), F.col("airport_name").alias("origin_name"))
    destination = airports.select(
        F.col("iata").alias("destination"), F.col("airport_name").alias("destination_name")
    )
    if use_broadcast:
        origin, destination = F.broadcast(origin), F.broadcast(destination)
    return flights.join(origin, "origin", "left").join(destination, "destination", "left")


def main():
    spark = (
        SparkSession.builder
        .master("local[4]")
        .appName("DAT535-Query-Plans")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        silver_run = current_run(DATA_DIR / "silver")
        silver = spark.read.parquet(str(silver_run / "flights"))
        airports = spark.read.parquet(str(silver_run / "airports")).cache()
        airports.count()

        flights = silver.select("flight_date", "airline", "origin", "destination", "arrival_delay")

        broadcast = join_flights(flights, airports, use_broadcast=True)
        broadcast_plan = plan(broadcast)
        broadcast_seconds = median_time(broadcast)
        broadcast_rows = broadcast.count()

        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
        spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")
        sort_merge = join_flights(flights, airports, use_broadcast=False)
        sort_merge_plan = plan(sort_merge)
        sort_merge_seconds = median_time(sort_merge)
        sort_merge_rows = sort_merge.count()
        spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")
        spark.conf.unset("spark.sql.adaptive.autoBroadcastJoinThreshold")

        if broadcast_rows != sort_merge_rows:
            raise RuntimeError(f"join results differ: {broadcast_rows} vs {sort_merge_rows}")

        july = silver.filter(F.col("month") == 7).agg(F.avg("arrival_delay"))
        july_plan = plan(july)

        narrow_plan = plan(silver.select("airline", "arrival_delay"))

        one_airline = silver.filter(F.col("airline") == "HA").agg(F.avg("arrival_delay"))
        one_airline_plan = plan(one_airline)
        all_airlines_seconds = median_time(silver.agg(F.avg("arrival_delay")))
        one_airline_seconds = median_time(one_airline)
    finally:
        spark.stop()

    summary = f"""DAT535 - Spark query optimization
Data: BTS On-Time Performance 2025, Silver run {silver_run.name}
Flights: {broadcast_rows:,}
Times: median of 5 runs after 1 warm-up

1. BROADCAST JOIN (airport names onto flights, origin + destination)
   with broadcast:    {plan_line(broadcast_plan, "BroadcastHashJoin")}  {broadcast_seconds:.2f} s
   without broadcast: {plan_line(sort_merge_plan, "SortMergeJoin")}  {sort_merge_seconds:.2f} s
   same result: {broadcast_rows:,} rows in both

2. PARTITION PRUNING (filter month = 7)
   {plan_line(july_plan, "PartitionFilters")}

3. COLUMN PRUNING (select 2 of {len(silver.columns)} Silver columns)
   {plan_line(narrow_plan, "ReadSchema")}

4. FILTER PUSHDOWN (filter airline = 'HA', not a partition column)
   {plan_line(one_airline_plan, "PushedFilters")}
   all airlines {all_airlines_seconds:.2f} s, HA only {one_airline_seconds:.2f} s
   no speed-up here, probably because Silver is not sorted by airline (row-group skipping is limited)
"""

    PERF_DIR.mkdir(parents=True, exist_ok=True)
    (PERF_DIR / "query_summary.txt").write_text(summary)
    plans = {
        "broadcast join": broadcast_plan,
        "sort-merge join": sort_merge_plan,
        "month = 7": july_plan,
        "airline = 'HA'": one_airline_plan,
    }
    with (PERF_DIR / "query_plans.txt").open("w") as f:
        for name, text in plans.items():
            f.write(f"=== {name} ===\n{text}\n")
    print(summary)


if __name__ == "__main__":
    main()
