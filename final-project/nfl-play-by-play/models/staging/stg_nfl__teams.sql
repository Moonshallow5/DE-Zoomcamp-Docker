with source as (
    select * from {{ source('nfl_raw', 'teams') }}
)

select
    cast(season as int64) as season,
    cast(teamId as int64) as team_id,
    upper(trim(abbr)) as team_abbr,
    trim(cityState) as city_state,
    trim(fullName) as team_name,
    trim(nick) as nickname,
    trim(conferenceAbbr) as conference,
    trim(divisionAbbr) as division
from source
