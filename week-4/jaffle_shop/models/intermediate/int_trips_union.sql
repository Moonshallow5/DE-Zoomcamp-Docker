with green_trips as (
    select * from {{ ref('stg_green_tripdata_partitioned_clustered') }}
),
yellow_trips as (
    select * from {{ ref('stg_yellow_tripdata_partitioned_clustered') }}
),
trips_union as (
    select * from green_trips
    union all
    select * from yellow_trips
)
select * from trips_union
