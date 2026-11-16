# Project 

This project implements a batch data engineering pipeline using Spark and the Medallion Architecture.

We process approx. 7mill US domestic flight records from the BTS On-Time Performance dataset.
The goal is to create reliable analytical datasets for comparing airline punctuality, airport performance, cancellations, and delay trends over time.

# Data sources

BTS On-Time Performance: Monthly CSV files containing flight dates, carriers, airports, delays, cancellations, and other operational information.
OpenFlights: Airport reference data, including airport names, locations, and time zones.
Airport overrides: Verified corrections for missing or inconsistent airport references.

Source: https://www.transtats.bts.gov/


# Pipeline

## Bronze - src/ingest.py

Ingests monthly CSV files into Parquet.
Preserves source values and adds lineage metadata.
Tracks file checksums, schema information, and ingestion manifests.

## Silver - src/clean.py

Standardizes data types and column names.
Validates flight records and handles duplicates
Separates accepted and rejected records with rejection reasons.
Enriches flights with airport information using broadcast joins.
Calculates UTC timestamps and time consistency flags.
Applies quality gates before publishing.

## Gold - src/aggregate.py

Produces three analytical datasets:

airline_performance: monthly airline punctuality and cancellation metrics
airport_performance: monthly departure-airport performance
daily_trends: daily statistics and seven day rolling delay and cancellation rates.

Gold totals are validated against silver to ensure consistency.

# Setup

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt


# Run the complete pipeline 
DAT535_YEARS=2025 python run_pipeline.py all

# Run tests
python -m pytest -q

# Run Spark performance analysis
python src/explain_plans.py

# Update the notebook/results
jupyter nbconvert --to notebook --execute --inplace notebooks/results.ipynb
```

# Data quality and results

bronze records: 7 001 619
Silver accepted: 7 001 618
Rejected records: 1
Unmatched airport records: 0
Scheduled arrival time mismatches: 213
Silver quality gate: PASS

Silver and gold use versioned run directories and _READY.json pointers so that failed runs do not replace previously published valid datasets.

# Spark Optimizations

Broadcast joins for small airport reference tables
Parquet column pruning and predicate pushdown
Partition pruning for monthly flight data
Caching of intermediate Silver transformations
Execution plan inspection and benchmark comparisons

Example: a airport-join benchmark completed in less time using broadcast joins vs. using sort-merge joins.

# Testing and Deployment

The project uses pytest for validation and pipeline tests

Github Actions support automated checks and pipeline execution on a VM, with separate development and production configurations.
Run logs, quality records, and result manifests provide evidence of successful processing.