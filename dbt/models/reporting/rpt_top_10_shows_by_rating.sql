{{
    config(
        materialized='table',
        schema='reporting'
    )
}}

select
    name as show_name,
    rating_average,
    genres,
    status,
    network_name,
    premiered,
    ended,
    tvmaze_id,
    url,
    language,
    type
from {{ ref('dim_tv_shows_current') }}
where rating_average is not null
order by rating_average desc nulls last
limit 10

