import os
import sys
import urllib.request
from pathlib import Path

# Must be set BEFORE importing pyspark / creating SparkSession on Windows.
BASE_DIR = Path(__file__).resolve().parent
HADOOP_HOME = BASE_DIR / "hadoop"
WINUTILS_DIR = HADOOP_HOME / "bin"
WINUTILS_EXE = WINUTILS_DIR / "winutils.exe"
HADOOP_DLL = WINUTILS_DIR / "hadoop.dll"

WINUTILS_BASE = (
    "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.5/bin"
)


def setup_hadoop_home() -> None:
    """Spark parquet writes on Windows need winutils.exe via HADOOP_HOME."""
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
    """Force Spark workers to use this venv Python (not Windows Store Python)."""
    python_exe = sys.executable
    os.environ["PYSPARK_PYTHON"] = python_exe
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_exe


setup_hadoop_home()
setup_python_workers()

from pyspark.sql import SparkSession, functions as F, types

CSV_PATH = BASE_DIR / "fhvhv_tripdata_2021-01.csv.gz"
PARQUET_PATH = BASE_DIR / "fhvhv" / "2021" / "01"
DATA_URL = (
    "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/"
    "fhvhv/fhvhv_tripdata_2021-01.csv.gz"
)


def download_data() -> None:
    if CSV_PATH.exists():
        return

    print(f"Downloading the large dataset to {CSV_PATH} ...")
    urllib.request.urlretrieve(DATA_URL, CSV_PATH)


def base_id_expr():
    """Same logic as the Zoomcamp UDF, but pure Spark SQL (no Python workers)."""
    num = F.substring(F.col("dispatching_base_num"), 2, 10).cast("int")
    hex_part = F.lower(F.lpad(F.hex(num), 3, "0"))
    return (
        F.when(num % 7 == 0, F.concat(F.lit("s/"), hex_part))
        .when(num % 3 == 0, F.concat(F.lit("a/"), hex_part))
        .otherwise(F.concat(F.lit("e/"), hex_part))
    )


def main() -> None:
    download_data()

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("fhvhv-taxi")
        .config("spark.ui.enabled", "true")
        .config("spark.ui.port", "4040")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    ui_url = spark.sparkContext.uiWebUrl or "http://localhost:4040"
    print(f"Spark version: {spark.version}")
    print(f"Using Python: {sys.executable}")
    print(f"Spark UI (use this): http://localhost:4040")
    print(f"Spark UI (reported): {ui_url}")

    schema = types.StructType(
        [
            types.StructField("hvfhs_license_num", types.StringType(), True),
            types.StructField("dispatching_base_num", types.StringType(), True),
            types.StructField("pickup_datetime", types.TimestampType(), True),
            types.StructField("dropoff_datetime", types.TimestampType(), True),
            types.StructField("PULocationID", types.IntegerType(), True),
            types.StructField("DOLocationID", types.IntegerType(), True),
            types.StructField("SR_Flag", types.StringType(), True),
        ]
    )

    # Skip rebuild if parquet already exists from a previous successful run.
    success_marker = PARQUET_PATH / "_SUCCESS"
    if not success_marker.exists():
        trips = (
            spark.read.option("header", "true")
            .schema(schema)
            .csv(str(CSV_PATH))
        )
        trips.printSchema()
        print("Writing parquet (this can take a while)...")
        trips.repartition(24).write.mode("overwrite").parquet(str(PARQUET_PATH))
        print(f"Wrote parquet data to {PARQUET_PATH}")
    else:
        print(f"Using existing parquet at {PARQUET_PATH}")

    parquet_trips = spark.read.parquet(str(PARQUET_PATH))

    result = (
        parquet_trips.filter(F.col("hvfhs_license_num") == "HV0003")
        .withColumn("pickup_date", F.to_date("pickup_datetime"))
        .withColumn("dropoff_date", F.to_date("dropoff_datetime"))
        .withColumn("base_id", base_id_expr())
        .select(
            "base_id",
            "pickup_date",
            "dropoff_date",
            "PULocationID",
            "DOLocationID",
        )
    )
    result.show(20, truncate=False)

    print("\nOpen Spark UI now: http://localhost:4040")
    input("Press Enter here to stop Spark and close the UI...")
    spark.stop()


if __name__ == "__main__":
    main()
