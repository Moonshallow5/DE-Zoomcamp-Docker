with source as (
    select * from {{ source('nfl_raw', 'players') }}
)

select
    cast(nflId as int64) as nfl_id,
    cast(season as int64) as season,
    cast(teamId as int64) as team_id,
    nullif(trim(gsisId), '') as gsis_id,
    trim(displayName) as player_name,
    trim(firstName) as first_name,
    trim(lastName) as last_name,
    trim(status) as player_status,
    trim(positionGroup) as position_group,
    trim(position) as position,
    trim(collegeName) as college_name,
    cast(weight as float64) as weight,
    cast(birthDate as date) as birth_date,
    cast(jerseyNumber as int64) as jersey_number
from source
