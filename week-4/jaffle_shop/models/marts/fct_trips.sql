with trips as (
    select * from {{ ref('int_trips_union') }}
),

trips_with_id as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'vendor_id',
            'pickup_datetime',
            'dropoff_datetime',
            'pickup_location_id',
            'dropoff_location_id',
            'passenger_count',
            'trip_distance',
            'fare_amount',
            'total_amount',
            'payment_type'
        ]) }} as trip_id,
        vendor_id,
        rate_code_id,
        pickup_location_id,
        dropoff_location_id,
        pickup_datetime,
        dropoff_datetime,
        store_and_fwd_flag,
        passenger_count,
        trip_distance,
        trip_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        ehail_fee,
        improvement_surcharge,
        total_amount,
        payment_type as payment_type_id,
        {{ get_payment_type_description('payment_type') }} as payment_type
    from trips
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by trip_id
            order by pickup_datetime
        ) as duplicate_rank
    from trips_with_id
)

select
    trip_id,
    vendor_id,
    rate_code_id,
    pickup_location_id,
    dropoff_location_id,
    pickup_datetime,
    dropoff_datetime,
    store_and_fwd_flag,
    passenger_count,
    trip_distance,
    trip_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    total_amount,
    payment_type_id,
    payment_type
from deduplicated
where duplicate_rank = 1
