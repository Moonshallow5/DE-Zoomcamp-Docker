{{ config(
    partition_by={"field": "week_start_date", "data_type": "date", "granularity": "day"},
    cluster_by=["season", "team_abbr", "play_type"]
) }}

select
    date_trunc(game_date, week(monday)) as week_start_date,
    season,
    week,
    possession_team as team_abbr,
    offense_team_name as team_name,
    play_type,
    count(*) as plays,
    sum(coalesce(yards_gained, 0)) as yards_gained,
    avg(epa) as avg_epa,
    sum(coalesce(touchdown, 0)) as touchdowns,
    sum(turnovers) as turnovers
from {{ ref('fct_plays') }}
where is_scrimmage_play
  and possession_team is not null
group by 1, 2, 3, 4, 5, 6
