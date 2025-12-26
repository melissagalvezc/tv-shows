{{
    config(
        materialized='table',
        schema='reporting'
    )
}}

with ranked_shows as (
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
        type,
        row_number() over (order by rating_average desc nulls last) as ranking
    from {{ ref('dim_tv_shows_current') }}
    where rating_average is not null
)

select
    ranking,
    show_name,
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
from ranked_shows
order by ranking
limit 10

