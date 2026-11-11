import hashlib
import json
import logging
import re
import shutil
import time
import urllib.error
import urllib.request
import uuid
import zipfile
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from publish import code_version, write_json

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SOURCE_DIR = PROJECT_ROOT / "data" / "raw"
DATA_DIR = PROJECT_ROOT / "data"
BRONZE_DIR = DATA_DIR / "bronze"
METADATA_DIR = BRONZE_DIR / "manifests"
EXPECTED_MONTHS = set(range(1, 13))

REQUIRED_COLUMNS = [
    "FlightDate",
    "Reporting_Airline",
    "Flight_Number_Reporting_Airline",
    "Origin",
    "Dest",
    "CRSDepTime",
    "DepTime",
    "DepDelay",
    "CRSArrTime",
    "ArrTime",
    "ArrDelay",
    "Cancelled",
    "Diverted",
    "CRSElapsedTime",
    "Distance",
]

BTS_ZIP = (
    "On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{month}.zip"
)
BTS_URL = "https://transtats.bts.gov/PREZIP/" + BTS_ZIP
AIRPORTS_URL = (
    "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat"
)


def schema_hash(columns):
    return hashlib.sha256("|".join(columns).encode()).hexdigest()[:16]


def validate_columns(columns, source_name):
    missing = [c for c in REQUIRED_COLUMNS if c not in columns]

    if missing:
        raise ValueError(
            f"{source_name} is missing columns: {missing}"
        )


def file_checksum(path):
    sha256 = hashlib.sha256()

    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            sha256.update(chunk)

    return sha256.hexdigest()


def get_period(filename):
    match = re.search(r"_(\d{4})_(\d{1,2})\.csv$", filename)

    if not match:
        raise ValueError(f"Cannot identify reporting period: {filename}")

    year, month = int(match.group(1)), int(match.group(2))

    if not 1 <= month <= 12:
        raise ValueError(f"Invalid month: {month}")

    return year, month


def verify_monthly_delivery(source_files, year, allow_partial=False):
    by_month = {}
    unexpected = []

    for path in source_files:
        file_year, month = get_period(path.name)

        if file_year != year:
            unexpected.append(path.name)
            continue

        by_month.setdefault(month, []).append(path.name)

    if allow_partial and by_month:
        expected_months = set(range(1, max(by_month) + 1))
    else:
        expected_months = EXPECTED_MONTHS

    missing = sorted(expected_months - set(by_month))

    duplicates = {
        str(month): filenames
        for month, filenames in by_month.items()
        if len(filenames) > 1
    }

    if missing or duplicates or unexpected:
        status = "INCOMPLETE"
    elif expected_months == EXPECTED_MONTHS:
        status = "COMPLETE"
    else:
        status = "PARTIAL"

    receipt = {
        "year": year,
        "expected_months": sorted(expected_months),
        "expected_count": len(expected_months),
        "received_months": sorted(by_month),
        "received_count": len(by_month),
        "missing_months": missing,
        "duplicate_months": duplicates,
        "unexpected_files": unexpected,
        "status": status,
    }

    return receipt


def find_csv_files(source_dir, years=None):
    found = {}

    for folder in sorted(source_dir.iterdir()):
        if not (folder.is_dir() and folder.name.isdigit()):
            continue

        year = int(folder.name)

        if years and year not in years:
            continue

        files = sorted(folder.glob("On_Time_Reporting_Carrier_*.csv"))

        if files:
            found[year] = files

    missing_years = set(years or []) - set(found)

    if missing_years:
        raise FileNotFoundError(
            f"No source files for years {sorted(missing_years)} in {source_dir}"
        )

    if not found:
        raise FileNotFoundError(f"No BTS CSV files in {source_dir}/<year>/")

    return found


def download(url, target):
    if target.exists():
        return True

    target.parent.mkdir(parents=True, exist_ok=True)
    partial = target.with_name(target.name + ".part")

    try:
        with urllib.request.urlopen(url, timeout=120) as response, \
                partial.open("wb") as f:
            shutil.copyfileobj(response, f)
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return False
        raise

    partial.replace(target)
    logger.info(f"Downloaded: {target.name}")
    return True


def download_missing(source_dir, years):
    for year in years:
        year_dir = source_dir / str(year)
        year_dir.mkdir(parents=True, exist_ok=True)

        present = {
            get_period(p.name)[1]
            for p in year_dir.glob("On_Time_Reporting_Carrier_*.csv")
        }

        for month in sorted(EXPECTED_MONTHS - present):
            zip_path = year_dir / BTS_ZIP.format(year=year, month=month)

            if not download(BTS_URL.format(year=year, month=month), zip_path):
                logger.info(f"{year}-{month:02d} not published yet")
                break

            with zipfile.ZipFile(zip_path) as archive:
                for member in archive.namelist():
                    name = Path(member).name
                    if name.endswith(".csv"):
                        with archive.open(member) as src, \
                                (year_dir / name).open("wb") as dst:
                            shutil.copyfileobj(src, dst)

    download(AIRPORTS_URL, source_dir / "openflights_airports.dat")



def ingest_file(spark, source_file, bronze_dir=None, metadata_dir=None):
    bronze_dir = bronze_dir or BRONZE_DIR
    metadata_dir = metadata_dir or METADATA_DIR
    year, month = get_period(source_file.name)

    output_path = (
        bronze_dir
        / "flights"
        / f"period={year}-{month:02d}"
    )

    manifest_path = (
        metadata_dir / f"flights_{year}_{month:02d}.json"
    )

    checksum = file_checksum(source_file)

    if manifest_path.exists():
        with manifest_path.open() as f:
            previous = json.load(f)

        if (
            previous.get("checksum_sha256") == checksum
            and previous.get("status") == "SUCCESS"
            and output_path.is_dir()
        ):
            print(f"SKIPPED: {source_file.name} (already ingested)")
            return

        raise RuntimeError(
            f"Existing ingestion differs or output is missing: "
            f"{source_file.name}. Investigate before rerunning."
        )

    if output_path.exists():
        raise RuntimeError(
            f"Output already exists without a manifest: {output_path}"
        )

    run_id = str(uuid.uuid4())
    ingestion_time = datetime.now(UTC).isoformat()

    print(f"\nIngesting: {source_file.name}")
    print(f"Reporting period: {year}-{month:02d}")
    print(f"Run ID: {run_id}")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(str(source_file))
    )

    validate_columns(df.columns, source_file.name)

    row_count = df.count()
    source_column_count = len(df.columns)

    bronze_df = (
        df.withColumn("_source_file", F.lit(source_file.name))
        .withColumn("_ingestion_time", F.lit(ingestion_time))
        .withColumn("_run_id", F.lit(run_id))
    )

    bronze_df.write.mode("errorifexists").parquet(
        str(output_path)
    )

    metadata = {
        "dataset": "bts_flights",
        "source_file": source_file.name,
        "source_path": str(source_file.resolve()),
        "source_size_bytes": source_file.stat().st_size,
        "checksum_sha256": checksum,
        "year": year,
        "month": month,
        "ingestion_time_utc": ingestion_time,
        "run_id": run_id,
        "row_count": row_count,
        "source_column_count": source_column_count,
        "schema_hash": schema_hash(df.columns),
        "bronze_path": str(output_path.resolve()),
        "code_version": code_version(),
        "status": "SUCCESS",
    }

    write_json(manifest_path, metadata)

    print(f"SUCCESS: {row_count:,} records ingested")
    print(f"Columns: {source_column_count}")
    print(f"Output: {output_path}")
    print(f"Manifest: {manifest_path}")



def ingest_airports(spark, source_dir=None, bronze_dir=None):
    source_dir = source_dir or PROJECT_ROOT / "data/raw"
    bronze_dir = bronze_dir or PROJECT_ROOT / "data/bronze"

    source = source_dir / "openflights_airports.dat"
    output = bronze_dir / "airports"
    manifest = bronze_dir / "manifests" / "airports.json"

    if not source.exists():
        raise FileNotFoundError(f"Airport file not found: {source}")

    checksum = file_checksum(source)

    if manifest.exists():
        with manifest.open() as f:
            previous = json.load(f)

        if (
            previous.get("checksum_sha256") == checksum
            and previous.get("status") == "SUCCESS"
            and output.is_dir()
        ):
            print("SKIPPED: Airports already ingested")
            return

        raise RuntimeError(
            "Airport data changed or ingestion state is inconsistent."
        )

    if output.exists():
        raise RuntimeError(
            f"Airport output exists without a manifest: {output}"
        )

    columns = [
        "AirportID", "Name", "City", "Country",
        "IATA", "ICAO", "Latitude", "Longitude",
        "Altitude", "Timezone", "DST",
        "TzDatabaseTimezone", "Type", "Source"
    ]

    df = (
        spark.read
        .option("header", "false")
        .option("mode", "FAILFAST")
        .csv(str(source))
    )

    if len(df.columns) != len(columns):
        raise ValueError(
            f"Expected {len(columns)} airport columns, "
            f"found {len(df.columns)}"
        )

    df = df.toDF(*columns)

    run_id = str(uuid.uuid4())
    ingestion_time = datetime.now(UTC).isoformat()

    bronze_df = (
        df.withColumn("_source_file", F.lit(source.name))
          .withColumn("_ingestion_time", F.lit(ingestion_time))
          .withColumn("_run_id", F.lit(run_id))
    )

    bronze_df.write.mode("errorifexists").parquet(str(output))

    row_count = spark.read.parquet(str(output)).count()

    metadata = {
        "dataset": "openflights_airports",
        "source_file": source.name,
        "checksum_sha256": checksum,
        "ingestion_time_utc": ingestion_time,
        "run_id": run_id,
        "row_count": row_count,
        "source_column_count": len(columns),
        "code_version": code_version(),
        "bronze_path": str(output.resolve()),
        "status": "SUCCESS"
    }

    write_json(manifest, metadata)

    print(f"SUCCESS: {row_count:,} airport records ingested")
    print(f"Output: {output}")


def run(source_dir=None, data_dir=None, years=None, partial_years=None):
    source_dir = Path(source_dir or SOURCE_DIR)
    bronze_dir = Path(data_dir or DATA_DIR) / "bronze"
    metadata_dir = bronze_dir / "manifests"
    start_time = time.time()

    if years:
        download_missing(source_dir, years)

    sources = find_csv_files(source_dir, years)
    partial = (
        set(partial_years) if partial_years is not None
        else {datetime.now(UTC).year}
    )

    receipts = {}

    for year, files in sources.items():
        receipt = verify_monthly_delivery(
            files, year, allow_partial=(year in partial)
        )
        receipt["checked_utc"] = datetime.now(UTC).isoformat()
        receipts[year] = receipt

        if receipt["status"] == "INCOMPLETE":
            write_json(metadata_dir / f"delivery_{year}.json", receipt)
            raise RuntimeError(
                f"Incomplete delivery for {year}: "
                f"missing={receipt['missing_months']}, "
                f"duplicates={receipt['duplicate_months']}, "
                f"unexpected={receipt['unexpected_files']}"
            )

    spark = (
        SparkSession.builder
        .appName("DAT535-Bronze-Ingestion")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        ingest_airports(spark, source_dir, bronze_dir)

        for year, files in sources.items():
            for source_file in files:
                ingest_file(spark, source_file, bronze_dir, metadata_dir)

            receipt = receipts[year]
            manifests = [
                json.loads(
                    (metadata_dir / f"flights_{year}_{month:02d}.json").read_text()
                )
                for month in receipt["received_months"]
            ]

            receipt["source_records"] = sum(m["row_count"] for m in manifests)

            versions = {}
            for m in manifests:
                key = m.get("schema_hash", "not recorded")
                versions.setdefault(key, []).append(m["month"])

            receipt["schema_versions"] = versions

            if len(versions) > 1:
                logger.warning(f"{year}: source schema changed between months: {versions}")

            receipt["code_version"] = code_version()

            write_json(metadata_dir / f"delivery_{year}.json", receipt)

            print(f"\n=== BRONZE DELIVERY RECEIPT {year} ===")
            print(json.dumps(receipt, indent=2))
    finally:
        spark.stop()

    return {
        "status": "success",
        "receipts": receipts,
        "elapsed_time": time.time() - start_time,
    }


if __name__ == "__main__":
    run()
