{{
    config(
        materialized='table',
        schema='marts'
    )
}}

with latest_shows as (
    select
        *,
        row_number() over (
            partition by tvmaze_id 
            order by loaded_at desc
        ) as rn
    from {{ ref('stg_tv_shows') }}
)

select
    load_id,
    tvmaze_id,
    url,
    name,
    type,
    language,
    genres,
    status,
    runtime,
    average_runtime,
    premiered,
    ended,
    official_site,
    schedule_time,
    schedule_days,
    rating_average,
    weight,
    network_id,
    network_name,
    network_country_name,
    network_country_code,
    network_country_timezone,
    web_channel,
    dvd_country,
    externals_imdb,
    externals_thetvdb,
    externals_tvrage,
    image_medium,
    image_original,
    summary,
    updated,
    links_self_href,
    links_previousepisode_href,
    source_file,
    loaded_at,
    current_timestamp as processed_at
from latest_shows
where rn = 1

