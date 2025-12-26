{{
    config(
        materialized='table',
        schema='reporting'
    )
}}

with ranked_shows as (
    select
        network_name,
        name as show_name,
        rating_average,
        genres,
        status,
        tvmaze_id,
        url,
        premiered,
        ended,
        row_number() over (
            partition by network_name 
            order by rating_average desc nulls last
        ) as rn
    from {{ ref('dim_tv_shows_current') }}
    where network_name is not null
        and rating_average is not null
)

select
    network_name,
    show_name,
    rating_average,
    genres,
    status,
    tvmaze_id,
    url,
    premiered,
    ended
from ranked_shows
where rn = 1
order by rating_average desc nulls last

