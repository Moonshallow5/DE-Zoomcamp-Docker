#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
import click


@click.command()
@click.option("--pg-user", default="root", help="PostgreSQL user")
@click.option("--pg-pass", default="root", help="PostgreSQL password")
@click.option("--pg-host", default="localhost", help="PostgreSQL host")
@click.option("--pg-port", default=5433, type=int, help="PostgreSQL port")
@click.option("--pg-db", default="ny_taxi", help="PostgreSQL database name")
@click.option("--target-table", default="green_taxi_trips", help="Target table name")
@click.option("--year", default=2025, type=int, help="Year of the data to ingest")
@click.option("--month", default=11, type=int, help="Month of the data to ingest")
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table, year, month):
    chunksize = 100000
    url = (
        "https://d37ci6vzurychx.cloudfront.net/trip-data/"
        f"green_tripdata_{year}-{month:02d}.parquet"
    )

    engine = create_engine(
        f"postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
    )

    print(f"Reading parquet: {url}")
    df = pd.read_parquet(url)
    print(f"Loaded {len(df)} rows")

    # Create empty table from schema, then append in chunks
    df.head(0).to_sql(name=target_table, con=engine, if_exists="replace", index=False)
    print("Table created")

    for i in range(0, len(df), chunksize):
        chunk = df.iloc[i : i + chunksize]
        chunk.to_sql(name=target_table, con=engine, if_exists="append", index=False)
        print("Inserted:", len(chunk))


if __name__ == "__main__":
    run()
