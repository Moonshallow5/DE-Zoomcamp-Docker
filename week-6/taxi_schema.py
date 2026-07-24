"""
Convert NYC green/yellow taxi CSVs to Parquet with explicit schemas.

Based on Zoomcamp notebook:
https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/06-batch/code/05_taxi_schema.ipynb

Layout:
  data/raw/{green|yellow}/{year}/{month}/...csv.gz
  data/pq/{green|yellow}/{year}/{month}/...parquet
"""

from __future__ import annotations

import argparse
import os
import sys
import urllib.request
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
HADOOP_HOME = BASE_DIR / "hadoop"
WINUTILS_DIR = HADOOP_HOME / "bin"
WINUTILS_EXE = WINUTILS_DIR / "winutils.exe"
HADOOP_DLL = WINUTILS_DIR / "hadoop.dll"
WINUTILS_BASE = (
    "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.5/bin"
)

DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PQ_DIR = DATA_DIR / "pq"

DOWNLOAD_BASE = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"


def setup_hadoop_home() -> None:
    WINUTILS_DIR.mkdir(parents=True, exist_ok=True)
    downloads = {
        WINUTILS_EXE: f"{WINUTILS_BASE}/winutils.exe",
        HADOOP_DLL: f"{WINUTILS_BASE}/hadoop.dll",
    }
    for path, url in downloads.items():
        if path.exists():
            continue
        print(f"Downloading {path.name} for Windows Spark...")
        urllib.request.urlretrieve(url, path)

    os.environ["HADOOP_HOME"] = str(HADOOP_HOME)
    os.environ["hadoop.home.dir"] = str(HADOOP_HOME)
    os.environ["PATH"] = str(WINUTILS_DIR) + os.pathsep + os.environ.get("PATH", "")


def setup_python_workers() -> None:
    python_exe = sys.executable
    os.environ["PYSPARK_PYTHON"] = python_exe
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_exe


setup_hadoop_home()
setup_python_workers()

from pyspark.sql import SparkSession, types


GREEN_SCHEMA = types.StructType(
    [
        types.StructField("VendorID", types.IntegerType(), True),
        types.StructField("lpep_pickup_datetime", types.TimestampType(), True),
        types.StructField("lpep_dropoff_datetime", types.TimestampType(), True),
        types.StructField("store_and_fwd_flag", types.StringType(), True),
        types.StructField("RatecodeID", types.IntegerType(), True),
        types.StructField("PULocationID", types.IntegerType(), True),
        types.StructField("DOLocationID", types.IntegerType(), True),
        types.StructField("passenger_count", types.IntegerType(), True),
        types.StructField("trip_distance", types.DoubleType(), True),
        types.StructField("fare_amount", types.DoubleType(), True),
        types.StructField("extra", types.DoubleType(), True),
        types.StructField("mta_tax", types.DoubleType(), True),
        types.StructField("tip_amount", types.DoubleType(), True),
        types.StructField("tolls_amount", types.DoubleType(), True),
        types.StructField("ehail_fee", types.DoubleType(), True),
        types.StructField("improvement_surcharge", types.DoubleType(), True),
        types.StructField("total_amount", types.DoubleType(), True),
        types.StructField("payment_type", types.IntegerType(), True),
        types.StructField("trip_type", types.IntegerType(), True),
        types.StructField("congestion_surcharge", types.DoubleType(), True),
    ]
)

YELLOW_SCHEMA = types.StructType(
    [
        types.StructField("VendorID", types.IntegerType(), True),
        types.StructField("tpep_pickup_datetime", types.TimestampType(), True),
        types.StructField("tpep_dropoff_datetime", types.TimestampType(), True),
        types.StructField("passenger_count", types.IntegerType(), True),
        types.StructField("trip_distance", types.DoubleType(), True),
        types.StructField("RatecodeID", types.IntegerType(), True),
        types.StructField("store_and_fwd_flag", types.StringType(), True),
        types.StructField("PULocationID", types.IntegerType(), True),
        types.StructField("DOLocationID", types.IntegerType(), True),
        types.StructField("payment_type", types.IntegerType(), True),
        types.StructField("fare_amount", types.DoubleType(), True),
        types.StructField("extra", types.DoubleType(), True),
        types.StructField("mta_tax", types.DoubleType(), True),
        types.StructField("tip_amount", types.DoubleType(), True),
        types.StructField("tolls_amount", types.DoubleType(), True),
        types.StructField("improvement_surcharge", types.DoubleType(), True),
        types.StructField("total_amount", types.DoubleType(), True),
        types.StructField("congestion_surcharge", types.DoubleType(), True),
    ]
)

SCHEMAS = {
    "green": GREEN_SCHEMA,
    "yellow": YELLOW_SCHEMA,
}


def download_month(taxi_type: str, year: int, month: int) -> Path:
    """Download one monthly CSV.gz into data/raw/{type}/{year}/{month}/."""
    filename = f"{taxi_type}_tripdata_{year}-{month:02d}.csv.gz"
    url = f"{DOWNLOAD_BASE}/{taxi_type}/{filename}"
    raw_month_dir = RAW_DIR / taxi_type / str(year) / f"{month:02d}"
    raw_month_dir.mkdir(parents=True, exist_ok=True)
    dest = raw_month_dir / filename

    if dest.exists():
        print(f"  already have {dest.name}")
        return dest

    print(f"  downloading {url}")
    try:
        urllib.request.urlretrieve(url, dest)
    except Exception as exc:
        if dest.exists():
            dest.unlink()
        raise RuntimeError(f"Failed to download {url}: {exc}") from exc
    return dest


def convert_month(
    spark: SparkSession, taxi_type: str, year: int, month: int
) -> None:
    input_path = RAW_DIR / taxi_type / str(year) / f"{month:02d}"
    output_path = PQ_DIR / taxi_type / str(year) / f"{month:02d}"
    success = output_path / "_SUCCESS"

    if not input_path.exists():
        print(f"  skip convert: missing input {input_path}")
        return

    if success.exists():
        print(f"  already converted -> {output_path}")
        return

    print(f"  converting CSV -> parquet: {output_path}")
    df = (
        spark.read.option("header", "true")
        .schema(SCHEMAS[taxi_type])
        .csv(str(input_path))
    )
    df.repartition(4).write.mode("overwrite").parquet(str(output_path))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download taxi CSVs and convert them to Parquet with schemas."
    )
    parser.add_argument(
        "--taxi-types",
        nargs="+",
        default=["green", "yellow"],
        choices=["green", "yellow"],
        help="Which taxi types to process",
    )
    parser.add_argument(
        "--years",
        nargs="+",
        type=int,
        default=[2021],
        help="Years to process (default: 2021)",
    )
    parser.add_argument(
        "--months",
        nargs="+",
        type=int,
        default=[1],
        help="Months 1-12 (default: 1 only; use 1 2 ... 12 for full year)",
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Only convert existing files under data/raw",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("taxi-schema")
        .config("spark.ui.enabled", "true")
        .config("spark.ui.port", "4040")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"Spark version: {spark.version}")
    print("Spark UI: http://localhost:4040")

    for taxi_type in args.taxi_types:
        for year in args.years:
            for month in args.months:
                print(f"processing {taxi_type} {year}/{month}")
                try:
                    if not args.skip_download:
                        download_month(taxi_type, year, month)
                    convert_month(spark, taxi_type, year, month)
                except Exception as exc:
                    print(f"  ERROR: {exc}")

    # Quick check of one converted dataset if present
    sample = PQ_DIR / "green" / "2021" / "01"
    if (sample / "_SUCCESS").exists():
        df = spark.read.parquet(str(sample))
        print("\nSample green 2021/01 schema:")
        df.printSchema()
        df.show(5, truncate=False)
        print(f"Rows: {df.count()}")

    print("\nOpen Spark UI: http://localhost:4040")
    input("Press Enter to stop Spark...")
    spark.stop()


if __name__ == "__main__":
    main()
