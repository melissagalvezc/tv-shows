{{
    config(
        materialized='table',
        schema='staging'
    )
}}

with raw_data as (
    select
        id,
        show_data,
        source_file,
        loaded_at
    from raw.tv_shows_raw
)

select
    id as load_id,
    (show_data->>'id')::integer as tvmaze_id,
    show_data->>'url' as url,
    show_data->>'name' as name,
    show_data->>'type' as type,
    show_data->>'language' as language,
    (show_data->>'genres')::jsonb as genres,
    show_data->>'status' as status,
    (show_data->>'runtime')::integer as runtime,
    (show_data->>'averageruntime')::integer as average_runtime,
    (show_data->>'premiered')::date as premiered,
    (show_data->>'ended')::date as ended,
    show_data->>'officialsite' as official_site,
    show_data->'schedule'->>'time' as schedule_time,
    (show_data->'schedule'->>'days')::jsonb as schedule_days,
    case 
        when show_data->'rating' is not null 
        then (show_data->'rating'->>'average')::decimal 
        else null 
    end as rating_average,
    (show_data->>'weight')::integer as weight,
    case 
        when show_data->'network' is not null 
        then (show_data->'network'->>'id')::integer 
        else null 
    end as network_id,
    show_data->'network'->>'name' as network_name,
    show_data->'network'->'country'->>'name' as network_country_name,
    show_data->'network'->'country'->>'code' as network_country_code,
    show_data->'network'->'country'->>'timezone' as network_country_timezone,
    case 
        when show_data->'webchannel' is not null 
        then show_data->'webchannel'->>'name' 
        else null 
    end as web_channel,
    show_data->>'dvdcountry' as dvd_country,
    case 
        when show_data->'externals' is not null 
        then show_data->'externals'->>'imdb' 
        else null 
    end as externals_imdb,
    case 
        when show_data->'externals' is not null 
        then (show_data->'externals'->>'thetvdb')::integer 
        else null 
    end as externals_thetvdb,
    case 
        when show_data->'externals' is not null 
        then (show_data->'externals'->>'tvrage')::integer 
        else null 
    end as externals_tvrage,
    show_data->'image'->>'medium' as image_medium,
    show_data->'image'->>'original' as image_original,
    show_data->>'summary' as summary,
    (show_data->>'updated')::bigint as updated,
    show_data->'_links'->'self'->>'href' as links_self_href,
    show_data->'_links'->'previousepisode'->>'href' as links_previousepisode_href,
    source_file,
    loaded_at
from raw_data

