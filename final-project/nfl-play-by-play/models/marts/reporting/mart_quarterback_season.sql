with player_lookup as (
    select
        season,
        gsis_id,
        any_value(player_name) as player_name,
        any_value(position) as position
    from {{ ref('dim_players') }}
    where gsis_id is not null
    group by 1, 2
),

passing_plays as (
    select *
    from {{ ref('fct_plays') }}
    where pass_attempt = 1
      and passer_gsis_id is not null
)

select
    p.season,
    p.passer_gsis_id,
    coalesce(pl.player_name, p.passer_player_name) as quarterback_name,
    p.possession_team as team_abbr,
    p.offense_team_name as team_name,
    count(*) as pass_attempts,
    sum(coalesce(p.complete_pass, 0)) as completions,
    safe_divide(sum(coalesce(p.complete_pass, 0)), count(*)) as completion_rate,
    sum(coalesce(p.yards_gained, 0)) as passing_play_yards,
    sum(coalesce(p.pass_touchdown, 0)) as passing_touchdowns,
    sum(coalesce(p.interception, 0)) as interceptions,
    avg(p.epa) as avg_epa_per_attempt,
    sum(coalesce(p.epa, 0)) as total_epa
from passing_plays p
left join player_lookup pl
    on p.season = pl.season
   and p.passer_gsis_id = pl.gsis_id
group by 1, 2, 3, 4, 5
having count(*) >= 50
