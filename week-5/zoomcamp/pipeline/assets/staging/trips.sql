/* @bruin

name: staging.trips
type: duckdb.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

columns:
  - name: pickup_datetime
    type: timestamp
    description: When the meter was engaged
    primary_key: true
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: When the meter was disengaged
    checks:
      - name: not_null
  - name: pickup_location_id
    type: integer
    description: TLC taxi zone where the meter was engaged
    checks:
      - name: not_null
  - name: dropoff_location_id
    type: integer
    description: TLC taxi zone where the meter was disengaged
    checks:
      - name: not_null
  - name: fare_amount
    type: float
    description: Time-and-distance fare from the meter
    checks:
      - name: non_negative
  - name: taxi_type
    type: string
    description: Taxi color type (yellow or green)
    checks:
      - name: not_null
  - name: payment_type_name
    type: string
    description: Human-readable payment type from lookup

custom_checks:
  - name: row_count_greater_than_zero
    description: Staging table should contain rows for the processed window
    query: |
      SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END
      FROM staging.trips
    value: 1

@bruin */

SELECT
    t.pickup_datetime,
    t.dropoff_datetime,
    t.pickup_location_id,
    t.dropoff_location_id,
    t.fare_amount,
    t.taxi_type,
    p.payment_type_name
FROM ingestion.trips t
LEFT JOIN ingestion.payment_lookup p
    ON t.payment_type = p.payment_type_id
WHERE t.pickup_datetime >= '{{ start_datetime }}'
  AND t.pickup_datetime < '{{ end_datetime }}'
  AND t.pickup_datetime IS NOT NULL
  AND t.dropoff_datetime IS NOT NULL
  AND t.pickup_location_id IS NOT NULL
  AND t.dropoff_location_id IS NOT NULL
  AND t.fare_amount >= 0
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY
        t.pickup_datetime,
        t.dropoff_datetime,
        t.pickup_location_id,
        t.dropoff_location_id,
        t.fare_amount
    ORDER BY t.extracted_at DESC
) = 1
