{{ config(alias="stg_weather_city_daily") }}

select
    city_slug,
    city_name,
    latitude::double as latitude,
    longitude::double as longitude,
    observation_date::date as observation_date,
    temp_max_c::double as temp_max_c,
    temp_min_c::double as temp_min_c,
    ingested_at_utc::timestamp as ingested_at_utc,
    pipeline_run_id,
    try_cast(dt as date) as partition_date
from read_parquet(
    '{{ var("raw_weather_glob") }}',
    hive_partitioning := true,
    union_by_name := true
)
where city_slug is not null
