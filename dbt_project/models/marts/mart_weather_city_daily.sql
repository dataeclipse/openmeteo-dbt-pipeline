{{ config(alias="mart_weather_city_daily") }}

select
    observation_date,
    city_slug,
    city_name,
    avg(temp_max_c) as avg_high_c,
    avg(temp_min_c) as avg_low_c,
    max(ingested_at_utc) as last_ingested_at
from {{ ref("stg_weather_city_daily") }}
where partition_date is null or partition_date > date '1970-01-02'
group by 1, 2, 3
