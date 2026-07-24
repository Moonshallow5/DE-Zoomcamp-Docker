{{ config(
    partition_by={"field": "game_date", "data_type": "date", "granularity": "day"},
    cluster_by=["season", "possession_team", "defensive_team"]
) }}

with plays as (
    select * from {{ ref('stg_nfl__plays') }}
),

teams as (
    select * from {{ ref('stg_nfl__teams') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['p.game_id', 'p.play_id']) }} as play_key,
    p.*,
    offense.team_id as offense_team_id,
    offense.team_name as offense_team_name,
    offense.conference as offense_conference,
    offense.division as offense_division,
    defense.team_id as defense_team_id,
    defense.team_name as defense_team_name,
    defense.conference as defense_conference,
    defense.division as defense_division,
    coalesce(p.pass_attempt, 0) = 1 or coalesce(p.rush_attempt, 0) = 1 as is_scrimmage_play,
    coalesce(p.interception, 0) + coalesce(p.fumble_lost, 0) as turnovers
from plays p
left join teams offense
    on p.season = offense.season
   and p.possession_team = offense.team_abbr
left join teams defense
    on p.season = defense.season
   and p.defensive_team = defense.team_abbr
