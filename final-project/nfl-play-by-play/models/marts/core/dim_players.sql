select
    {{ dbt_utils.generate_surrogate_key(['season', 'nfl_id', 'team_id']) }} as player_season_team_key,
    nfl_id,
    season,
    team_id,
    gsis_id,
    player_name,
    first_name,
    last_name,
    player_status,
    position_group,
    position,
    college_name,
    weight,
    birth_date,
    jersey_number
from {{ ref('stg_nfl__players') }}
