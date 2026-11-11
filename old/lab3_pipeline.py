#!/usr/bin/env python3
"""
DAT535 Lab 3: Advanced Spark & Production Patterns Pipeline
==============================================================

This pipeline demonstrates:
- Window functions (ranking, lag/lead, running totals)
- Partitioning strategies
- Caching and memory management
- All join types plus broadcast join optimization
- Query optimization techniques (filter/column pushdown, execution plans)
- UDFs vs pandas UDFs vs built-in functions
- Structured Streaming basics (windowed aggregation)
- Production patterns (incremental processing, data quality, SCD Type 2)

This pipeline does NOT generate its own data. It loads the Silver-layer dataset
produced by Lab 2 (lab2_pipeline.py) from ~/spark-lab-data/shared/silver/events,
so both labs analyze the exact same e-commerce clickstream dataset.

Usage:
    python lab3_pipeline.py

    # Or via run_pipeline.py (Lab 2 must run first):
    python run_pipeline.py lab3
"""

import logging
import os
import sys
import time
from importlib.util import find_spec

if find_spec("findspark") is not None:
    import findspark
else:
    findspark = None

if findspark is not None:
    findspark.init()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

if find_spec("pyspark") is None:
    logger.error("PySpark import failed: package 'pyspark' is not installed")
    sys.exit(1)

try:
    from pyspark import StorageLevel
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        avg,
        broadcast,
        col,
        count,
        dense_rank,
        desc,
        lag,
        lead,
        ntile,
        pandas_udf,
        rank,
        round as spark_round,
        row_number,
        sum as spark_sum,
        udf,
        when,
        window as tumbling_window,
    )
    from pyspark.sql.types import DoubleType, StringType
    from pyspark.sql.window import Window
except ImportError as e:
    logger.error(f"PySpark import failed: {e}")
    sys.exit(1)


class Lab3Pipeline:
    """Advanced Spark & Production Patterns Pipeline - Lab 3"""

    def __init__(self, base_dir: str = None, shared_dir: str = None):
        """Initialize the pipeline."""
        self.base_dir = base_dir or os.path.expanduser("~/spark-lab-data/lab3")
        self.shared_dir = shared_dir or os.path.expanduser("~/spark-lab-data/shared")
        self.silver_path = f"{self.shared_dir}/silver/events"
        self.spark = None
        self.events_df = None

    def create_spark_session(self) -> SparkSession:
        """Create Spark session with performance-oriented configuration."""
        logger.info("Creating Spark session with performance config...")

        self.spark = SparkSession.builder \
            .appName("DAT535-Lab3-AdvancedSpark") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.sql.shuffle.partitions", "8") \
            .config("spark.sql.parquet.compression.codec", "snappy") \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("WARN")
        logger.info(f"Spark version: {self.spark.version}")

        return self.spark

    def load_shared_data(self):
        """Load the Silver-layer dataset produced by Lab 2."""
        logger.info(f"Loading shared Silver dataset from {self.silver_path}...")

        if not os.path.exists(self.silver_path):
            raise FileNotFoundError(
                f"Could not find {self.silver_path}. Run Lab 2 (lab2_pipeline.py or "
                "'python run_pipeline.py lab2') first - it generates and saves the "
                "shared dataset used by this pipeline."
            )

        self.events_df = self.spark.read.parquet(self.silver_path)
        logger.info(f"Loaded {self.events_df.count()} Silver events from Lab 2")

        return self.events_df

    def run_window_functions(self):
        """Demonstrate ranking, lag/lead, and running-total window functions."""
        logger.info("=" * 50)
        logger.info("WINDOW FUNCTIONS")
        logger.info("=" * 50)

        results = {}

        purchases_df = self.events_df.filter(
            (col("event_type") == "purchase") & (col("total_amount").isNotNull())
        )
        results['total_purchases'] = purchases_df.count()

        user_window = Window.partitionBy("user_id").orderBy(desc("total_amount"))
        ranked_purchases = purchases_df.select(
            col("user_id"), col("event_id"), col("total_amount"), col("category"),
            row_number().over(user_window).alias("row_num"),
            rank().over(user_window).alias("rank"),
            dense_rank().over(user_window).alias("dense_rank"),
            ntile(4).over(user_window).alias("quartile"),
        )
        results['top_purchases_count'] = ranked_purchases.filter(col("row_num") <= 3).count()

        date_window = Window.orderBy("event_date")
        rolling_7day = Window.orderBy("event_date").rowsBetween(-6, 0)
        daily_sales = purchases_df.groupBy("event_date").agg(
            spark_sum("total_amount").alias("daily_revenue"),
            count("*").alias("num_orders")
        ).orderBy("event_date")
        daily_metrics = daily_sales \
            .withColumn("cumulative_revenue", spark_sum("daily_revenue").over(date_window)) \
            .withColumn("7day_avg_revenue", spark_round(avg("daily_revenue").over(rolling_7day), 2))
        results['daily_metrics_rows'] = daily_metrics.count()

        session_window = Window.partitionBy("user_id").orderBy("event_timestamp")
        user_activity = self.events_df.select(
            col("user_id"), col("event_timestamp"), col("event_type"),
            lag("event_type", 1).over(session_window).alias("prev_event"),
            lead("event_type", 1).over(session_window).alias("next_event"),
        )
        purchase_transitions = user_activity.filter(col("event_type") == "purchase") \
            .groupBy("prev_event").agg(count("*").alias("num_purchases")) \
            .orderBy(desc("num_purchases")).collect()
        results['purchase_transitions'] = {
            row['prev_event']: row['num_purchases'] for row in purchase_transitions[:5]
        }

        logger.info(f"Window functions results: {results}")
        return results

    def run_partitioning_demo(self):
        """Compare no-partition vs date-partition vs multi-level partition strategies."""
        logger.info("=" * 50)
        logger.info("PARTITIONING STRATEGIES")
        logger.info("=" * 50)

        results = {}
        partition_base = f"{self.base_dir}/partitioned_data"
        os.makedirs(partition_base, exist_ok=True)

        sample_date = self.events_df.select("event_date").first()["event_date"]
        sample_country = self.events_df.select("country").first()["country"]

        no_partition_path = f"{partition_base}/no_partition"
        start_time = time.time()
        self.events_df.coalesce(4).write.mode("overwrite").parquet(no_partition_path)
        results['no_partition_write_time'] = time.time() - start_time

        start_time = time.time()
        results['no_partition_result'] = self.spark.read.parquet(no_partition_path) \
            .filter(col("event_date") == sample_date).count()
        results['no_partition_query_time'] = time.time() - start_time

        date_partition_path = f"{partition_base}/date_partition"
        start_time = time.time()
        self.events_df.write.mode("overwrite").partitionBy("event_date").parquet(date_partition_path)
        results['date_partition_write_time'] = time.time() - start_time

        start_time = time.time()
        results['date_partition_result'] = self.spark.read.parquet(date_partition_path) \
            .filter(col("event_date") == sample_date).count()
        results['date_partition_query_time'] = time.time() - start_time

        multi_partition_path = f"{partition_base}/multi_partition"
        self.events_df.write.mode("overwrite") \
            .partitionBy("event_date", "country").parquet(multi_partition_path)
        start_time = time.time()
        results['multi_partition_result'] = self.spark.read.parquet(multi_partition_path) \
            .filter((col("event_date") == sample_date) & (col("country") == sample_country)).count()
        results['multi_partition_query_time'] = time.time() - start_time

        logger.info(f"Partitioning results: {results}")
        return results

    def run_caching_demo(self):
        """Compare cached vs non-cached repeated actions on the same DataFrame."""
        logger.info("=" * 50)
        logger.info("CACHING & MEMORY MANAGEMENT")
        logger.info("=" * 50)

        results = {}

        complex_df = self.events_df \
            .filter(col("event_type").isin(["purchase", "add_to_cart", "page_view"])) \
            .withColumn("is_purchase", when(col("event_type") == "purchase", 1).otherwise(0)) \
            .withColumn("hour_bucket",
                        when(col("event_hour") < 6, "night")
                        .when(col("event_hour") < 12, "morning")
                        .when(col("event_hour") < 18, "afternoon")
                        .otherwise("evening"))

        start_time = time.time()
        complex_df.groupBy("category").agg(count("*")).collect()
        complex_df.groupBy("device").agg(spark_sum("is_purchase")).collect()
        complex_df.groupBy("hour_bucket").agg(avg("price")).collect()
        results['no_cache_time'] = time.time() - start_time

        cached_df = complex_df.cache()
        start_time = time.time()
        cached_df.groupBy("category").agg(count("*")).collect()
        cache_time = time.time() - start_time

        start_time = time.time()
        cached_df.groupBy("device").agg(spark_sum("is_purchase")).collect()
        cached_df.groupBy("hour_bucket").agg(avg("price")).collect()
        subsequent_time = time.time() - start_time

        results['cache_first_query_time'] = cache_time
        results['cache_subsequent_time'] = subsequent_time
        results['total_cached_time'] = cache_time + subsequent_time
        results['speedup'] = round(results['no_cache_time'] / results['total_cached_time'], 2) \
            if results['total_cached_time'] > 0 else 1

        cached_df.unpersist()

        # Storage level demo
        large_cached_df = self.events_df.persist(StorageLevel.MEMORY_AND_DISK)
        large_cached_df.count()
        large_cached_df.unpersist()
        results['storage_level_demo'] = "MEMORY_AND_DISK"

        logger.info(f"Caching results: {results}")
        return results

    def run_joins_demo(self):
        """Demonstrate every join type plus broadcast join optimization."""
        logger.info("=" * 50)
        logger.info("JOINS: ALL TYPES + BROADCAST OPTIMIZATION")
        logger.info("=" * 50)

        results = {}

        category_details = self.spark.createDataFrame([
            ("Electronics", "Tech", 0.08),
            ("Clothing", "Fashion", 0.05),
            ("Books", "Media", 0.0),
            ("Home", "Lifestyle", 0.06),
            ("Sports", "Active", 0.05),
            ("Beauty", "Personal", 0.07),
        ], ["category", "department", "tax_rate"])

        purchase_events = self.events_df.filter(
            (col("event_type") == "purchase") & (col("category").isNotNull())
        )

        for join_type in ["inner", "left", "right", "full", "left_semi", "left_anti"]:
            joined = purchase_events.join(category_details, on="category", how=join_type)
            results[f"{join_type}_join_count"] = joined.count()

        start_time = time.time()
        regular_join = purchase_events.join(category_details, on="category", how="left")
        results['regular_join_count'] = regular_join.count()
        results['regular_join_time'] = time.time() - start_time

        start_time = time.time()
        broadcast_join = purchase_events.join(broadcast(category_details), on="category", how="left")
        results['broadcast_join_count'] = broadcast_join.count()
        results['broadcast_join_time'] = time.time() - start_time

        results['join_speedup'] = round(
            results['regular_join_time'] / results['broadcast_join_time'], 2
        ) if results['broadcast_join_time'] > 0 else 1

        logger.info(f"Join results: {results}")
        return results

    def run_optimization_demo(self):
        """Demonstrate filter pushdown and column pruning."""
        logger.info("=" * 50)
        logger.info("QUERY OPTIMIZATION")
        logger.info("=" * 50)

        results = {}

        category_details = self.spark.createDataFrame([
            ("Electronics", "Tech", 0.08), ("Clothing", "Fashion", 0.05),
            ("Books", "Media", 0.0), ("Home", "Lifestyle", 0.06),
            ("Sports", "Active", 0.05), ("Beauty", "Personal", 0.07),
        ], ["category", "department", "tax_rate"])

        start_time = time.time()
        late_filter = self.events_df \
            .join(broadcast(category_details), on="category", how="left") \
            .filter(col("country") == "US") \
            .filter(col("event_type") == "purchase") \
            .count()
        results['late_filter_time'] = time.time() - start_time
        results['late_filter_count'] = late_filter

        start_time = time.time()
        early_filter = self.events_df \
            .filter((col("country") == "US") & (col("event_type") == "purchase")) \
            .join(broadcast(category_details), on="category", how="left") \
            .count()
        results['early_filter_time'] = time.time() - start_time
        results['early_filter_count'] = early_filter

        results['filter_speedup'] = round(
            results['late_filter_time'] / results['early_filter_time'], 2
        ) if results['early_filter_time'] > 0 else 1

        logger.info(f"Optimization results: {results}")
        return results

    def run_udf_demo(self):
        """Compare built-in functions vs row UDFs vs pandas (vectorized) UDFs."""
        logger.info("=" * 50)
        logger.info("UDFS VS BUILT-IN FUNCTIONS")
        logger.info("=" * 50)

        results = {}
        purchase_events = self.events_df.filter(
            (col("event_type") == "purchase") & (col("total_amount").isNotNull())
        )

        start_time = time.time()
        purchase_events.withColumn(
            "price_tier",
            when(col("total_amount") >= 200, "high")
            .when(col("total_amount") >= 50, "medium")
            .otherwise("low")
        ).count()
        results['builtin_time'] = time.time() - start_time

        def price_tier_udf(amount):
            if amount is None:
                return "unknown"
            if amount >= 200:
                return "high"
            if amount >= 50:
                return "medium"
            return "low"

        price_tier = udf(price_tier_udf, StringType())
        start_time = time.time()
        purchase_events.withColumn("price_tier", price_tier(col("total_amount"))).count()
        results['python_udf_time'] = time.time() - start_time

        try:
            import pandas as pd

            @pandas_udf(DoubleType())
            def apply_discount(amount: pd.Series) -> pd.Series:
                return amount * 0.9

            start_time = time.time()
            purchase_events.withColumn("discounted_amount", apply_discount(col("total_amount"))).count()
            results['pandas_udf_time'] = time.time() - start_time
        except Exception as e:
            logger.warning(f"pandas UDF demo skipped/failed: {e}")
            results['pandas_udf_time'] = None

        logger.info(f"UDF comparison results: {results}")
        return results

    def run_streaming_demo(self):
        """Minimal Structured Streaming demo: windowed aggregation over a rate source."""
        logger.info("=" * 50)
        logger.info("STRUCTURED STREAMING (windowed aggregation demo)")
        logger.info("=" * 50)

        results = {}
        try:
            stream_df = self.spark.readStream.format("rate").option("rowsPerSecond", 5).load()

            windowed_counts = stream_df \
                .withWatermark("timestamp", "10 seconds") \
                .groupBy(tumbling_window(col("timestamp"), "5 seconds")) \
                .agg(count("*").alias("event_count"))

            query = windowed_counts.writeStream \
                .outputMode("update") \
                .format("memory") \
                .queryName("windowed_stream_demo") \
                .start()

            time.sleep(12)
            query.stop()

            row_count = self.spark.sql("SELECT * FROM windowed_stream_demo").count()
            results['streaming_windows_captured'] = row_count
            logger.info(f"Captured {row_count} streaming windows")
        except (TimeoutError, RuntimeError, OSError) as e:
            logger.warning(f"Structured Streaming demo skipped/failed: {e}")
            results['streaming_windows_captured'] = None

        return results

    def run_production_patterns(self):
        """Demonstrate incremental processing, data quality checks, and SCD Type 2."""
        logger.info("=" * 50)
        logger.info("PRODUCTION PATTERNS")
        logger.info("=" * 50)

        results = {}

        last_processed = "2024-01-15T00:00:00"
        total_rows = self.events_df.count()
        incremental_rows = self.events_df.filter(col("event_timestamp") > last_processed).count()
        results['total_records'] = total_rows
        results['incremental_records'] = incremental_rows
        results['incremental_ratio'] = round(incremental_rows / total_rows * 100, 1) if total_rows else 0

        critical_columns = ['event_id', 'user_id', 'event_timestamp', 'event_type']
        null_report = self.events_df.select([
            spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(f"{c}_nulls")
            for c in critical_columns
        ]).collect()[0]
        quality_passed = all(
            null_report[f"{c}_nulls"] / total_rows < 0.01 for c in critical_columns
        )
        results['data_quality_passed'] = quality_passed

        dup_count = total_rows - self.events_df.select('event_id').distinct().count()
        results['duplicates'] = dup_count

        user_dimension = self.spark.createDataFrame([
            (1, "John Doe", "basic", "2024-01-01", "2024-01-15", False),
            (1, "John Doe", "premium", "2024-01-15", "9999-12-31", True),
            (2, "Jane Smith", "premium", "2024-01-01", "9999-12-31", True),
        ], ["user_id", "name", "tier", "valid_from", "valid_to", "is_current"])
        results['scd2_dimension_rows'] = user_dimension.count()

        logger.info(f"Production patterns results: {results}")
        return results

    def run(self):
        """Execute the complete Lab 3 pipeline."""
        logger.info("=" * 60)
        logger.info("LAB 3: ADVANCED SPARK & PRODUCTION PATTERNS PIPELINE")
        logger.info("=" * 60)

        start_time = time.time()

        try:
            self.create_spark_session()
            self.load_shared_data()

            os.makedirs(self.base_dir, exist_ok=True)

            window_results = self.run_window_functions()
            partition_results = self.run_partitioning_demo()
            caching_results = self.run_caching_demo()
            joins_results = self.run_joins_demo()
            optimization_results = self.run_optimization_demo()
            udf_results = self.run_udf_demo()
            streaming_results = self.run_streaming_demo()
            production_results = self.run_production_patterns()

            elapsed_time = time.time() - start_time

            logger.info("=" * 60)
            logger.info("PIPELINE COMPLETE")
            logger.info("=" * 60)
            logger.info(f"Elapsed time: {elapsed_time:.2f} seconds")

            return {
                'status': 'success',
                'window_functions': window_results,
                'partitioning': partition_results,
                'caching': caching_results,
                'joins': joins_results,
                'optimization': optimization_results,
                'udfs': udf_results,
                'streaming': streaming_results,
                'production_patterns': production_results,
                'elapsed_time': elapsed_time,
            }

        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise

        finally:
            if self.spark:
                self.spark.stop()
                logger.info("Spark session stopped")


def main():
    """Main entry point."""
    pipeline = Lab3Pipeline()
    results = pipeline.run()

    print("\n" + "=" * 60)
    print("LAB 3 PIPELINE RESULTS")
    print("=" * 60)
    print(f"Status: {results['status']}")
    print(f"Window Functions - Purchases: {results['window_functions']['total_purchases']}")
    print(f"Caching Speedup: {results['caching']['speedup']}x")
    print(f"Broadcast Join Speedup: {results['joins']['join_speedup']}x")
    print(f"Data Quality Passed: {results['production_patterns']['data_quality_passed']}")
    print(f"Elapsed time: {results['elapsed_time']:.2f}s")
    print("=" * 60)

    return 0 if results['status'] == 'success' else 1


if __name__ == "__main__":
    sys.exit(main())
