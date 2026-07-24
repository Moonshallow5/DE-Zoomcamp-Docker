import urllib.request
from pathlib import Path

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.master("local[*]")
    .appName("test")
    .getOrCreate()
)

print(f"Spark version: {spark.version}")

# NYC TLC taxi zone lookup (same file used in Zoomcamp)
url = (
    "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/"
    "taxi_zone_lookup.csv"
)
csv_path = Path(__file__).parent / "taxi_zone_lookup.csv"

if not csv_path.exists():
    print(f"Downloading {url}")
    urllib.request.urlretrieve(url, csv_path)

df = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv(str(csv_path))
)

df.printSchema()
df.show(10, truncate=False)
print(f"Rows: {df.count()}")

# Write out as parquet (overwrite so re-runs don't fail on an existing folder)
df.write.mode("overwrite").parquet("zones")
print("Wrote parquet to ./zones")

# Keep the app alive so you can open the Spark UI at http://localhost:4040
input("Spark UI at http://localhost:4040 - press Enter to stop...")

