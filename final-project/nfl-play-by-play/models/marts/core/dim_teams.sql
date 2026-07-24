select
    {{ dbt_utils.generate_surrogate_key(['season', 'team_id']) }} as team_season_key,
    season,
    team_id,
    team_abbr,
    city_state,
    team_name,
    nickname,
    conference,
    division
from {{ ref('stg_nfl__teams') }}
