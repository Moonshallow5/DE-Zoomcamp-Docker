"""@bruin

name: ingestion.trips
type: python
image: python:3.11

connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: pickup_datetime
    type: timestamp
    description: When the meter was engaged
  - name: dropoff_datetime
    type: timestamp
    description: When the meter was disengaged
  - name: pickup_location_id
    type: integer
    description: TLC taxi zone where the meter was engaged
  - name: dropoff_location_id
    type: integer
    description: TLC taxi zone where the meter was disengaged
  - name: fare_amount
    type: float
    description: Time-and-distance fare from the meter
  - name: payment_type
    type: integer
    description: Numeric payment type code
  - name: taxi_type
    type: string
    description: Taxi color type (yellow or green)
  - name: extracted_at
    type: timestamp
    description: Timestamp when the record was extracted

@bruin"""

import json
import os
from datetime import datetime, timezone

import pandas as pd
from dateutil.relativedelta import relativedelta

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

DATETIME_COLUMNS = {
    "yellow": ("tpep_pickup_datetime", "tpep_dropoff_datetime"),
    "green": ("lpep_pickup_datetime", "lpep_dropoff_datetime"),
}


def normalize_columns(df: pd.DataFrame, taxi_type: str) -> pd.DataFrame:
    pickup_col, dropoff_col = DATETIME_COLUMNS[taxi_type]
    rename_map = {
        pickup_col: "pickup_datetime",
        dropoff_col: "dropoff_datetime",
        "PULocationID": "pickup_location_id",
        "DOLocationID": "dropoff_location_id",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
    df["taxi_type"] = taxi_type
    df["extracted_at"] = datetime.now(timezone.utc)
    return df


def materialize():
    start_date = os.environ["BRUIN_START_DATE"]
    end_date = os.environ["BRUIN_END_DATE"]
    taxi_types = json.loads(os.environ.get("BRUIN_VARS", "{}")).get(
        "taxi_types", ["yellow"]
    )

    start_dt = datetime.fromisoformat(start_date)
    end_dt = datetime.fromisoformat(end_date)

    frames = []
    current_dt = start_dt.replace(day=1)

    while current_dt < end_dt:
        year = current_dt.year
        month = current_dt.month

        for taxi_type in taxi_types:
            url = f"{BASE_URL}/{taxi_type}_tripdata_{year}-{month:02d}.parquet"
            try:
                df = pd.read_parquet(url)
                df = normalize_columns(df, taxi_type)
                frames.append(df)
                print(f"Loaded {len(df)} rows for {taxi_type} {year}-{month:02d}")
            except Exception as exc:
                print(f"Warning: failed to load {url}: {exc}")

        current_dt += relativedelta(months=1)

    if not frames:
        return pd.DataFrame(
            columns=[
                "pickup_datetime",
                "dropoff_datetime",
                "pickup_location_id",
                "dropoff_location_id",
                "fare_amount",
                "payment_type",
                "taxi_type",
                "extracted_at",
            ]
        )

    return pd.concat(frames, ignore_index=True)
