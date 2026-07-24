"""
Union green + yellow Parquet trips and compute monthly revenue with Spark SQL.

Based on Zoomcamp notebook:
https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/06-batch/code/06_spark_sql.ipynb

Requires Parquet from taxi_schema.py under:
  data/pq/green/*/*
  data/pq/yellow/*/*
"""

from __future__ import annotations

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

PQ_DIR = BASE_DIR / "data" / "pq"
REPORT_DIR = BASE_DIR / "data" / "report" / "revenue"


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

from pyspark.sql import SparkSession, functions as F


def main() -> None:
    green_glob = str(PQ_DIR / "green" / "*" / "*")
    yellow_glob = str(PQ_DIR / "yellow" / "*" / "*")

    if not (PQ_DIR / "green").exists() or not (PQ_DIR / "yellow").exists():
        raise FileNotFoundError(
            "Missing data/pq/green or data/pq/yellow. "
            "Run taxi_schema.py first, e.g.\n"
            "  uv run python taxi_schema.py --years 2021 --months 1"
        )

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("spark-sql-taxi")
        .config("spark.ui.enabled", "true")
        .config("spark.ui.port", "4040")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"Spark version: {spark.version}")
    print("Spark UI: http://localhost:4040")

    # Read all green / yellow parquet months
    df_green = spark.read.parquet(green_glob)
    df_yellow = spark.read.parquet(yellow_glob)

    # Align datetime column names
    df_green = df_green.withColumnRenamed(
        "lpep_pickup_datetime", "pickup_datetime"
    ).withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")

    df_yellow = df_yellow.withColumnRenamed(
        "tpep_pickup_datetime", "pickup_datetime"
    ).withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")

    # Keep only columns present in both datasets
    yellow_columns = set(df_yellow.columns)
    common_columns = [col for col in df_green.columns if col in yellow_columns]
    print(f"Common columns ({len(common_columns)}): {common_columns}")

    df_green_sel = df_green.select(common_columns).withColumn(
        "service_type", F.lit("green")
    )
    df_yellow_sel = df_yellow.select(common_columns).withColumn(
        "service_type", F.lit("yellow")
    )

    df_trips_data = df_green_sel.unionAll(df_yellow_sel)

    print("\nTrips by service_type:")
    df_trips_data.groupBy("service_type").count().show()

    # Spark SQL temp view (createOrReplaceTempView is the modern API)
    df_trips_data.createOrReplaceTempView("trips_data")

    spark.sql(
        """
        SELECT
            service_type,
            count(1) AS trip_count
        FROM trips_data
        GROUP BY service_type
        """
    ).show()

    df_result = spark.sql(
        """
        SELECT
            -- Revenue grouping
            PULocationID AS revenue_zone,
            date_trunc('month', pickup_datetime) AS revenue_month,
            service_type,

            -- Revenue calculation
            SUM(fare_amount) AS revenue_monthly_fare,
            SUM(extra) AS revenue_monthly_extra,
            SUM(mta_tax) AS revenue_monthly_mta_tax,
            SUM(tip_amount) AS revenue_monthly_tip_amount,
            SUM(tolls_amount) AS revenue_monthly_tolls_amount,
            SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
            SUM(total_amount) AS revenue_monthly_total_amount,
            SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

            -- Additional calculations
            AVG(passenger_count) AS avg_monthly_passenger_count,
            AVG(trip_distance) AS avg_monthly_trip_distance
        FROM trips_data
        GROUP BY 1, 2, 3
        """
    )

    print("\nRevenue report sample:")
    df_result.show(10, truncate=False)

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    df_result.coalesce(1).write.mode("overwrite").parquet(str(REPORT_DIR))
    print(f"Wrote revenue report to {REPORT_DIR}")

    print("\nOpen Spark UI: http://localhost:4040")
    input("Press Enter to stop Spark...")
    spark.stop()


if __name__ == "__main__":
    main()
