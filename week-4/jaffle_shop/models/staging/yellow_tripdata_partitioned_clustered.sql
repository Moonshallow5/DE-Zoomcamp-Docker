select * from {{ source('raw_data', 'yellow_tripdata_partitioned_clustered') }} limit 10;
