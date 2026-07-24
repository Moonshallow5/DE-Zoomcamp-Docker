with team_plays as (
    select *
    from {{ ref('fct_plays') }}
    where is_scrimmage_play
      and possession_team is not null
)

select
    season,
    possession_team as team_abbr,
    offense_team_name as team_name,
    offense_conference as conference,
    offense_division as division,
    count(*) as offensive_plays,
    sum(coalesce(pass_attempt, 0)) as pass_attempts,
    sum(coalesce(rush_attempt, 0)) as rush_attempts,
    safe_divide(sum(coalesce(pass_attempt, 0)), count(*)) as pass_rate,
    sum(coalesce(yards_gained, 0)) as total_yards,
    avg(epa) as avg_epa_per_play,
    sum(coalesce(touchdown, 0)) as touchdowns,
    sum(turnovers) as turnovers
from team_plays
group by 1, 2, 3, 4, 5
