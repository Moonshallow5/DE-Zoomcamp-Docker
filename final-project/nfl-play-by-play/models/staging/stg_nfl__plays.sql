with source as (
    select * from {{ source('nfl_raw', 'plays') }}
)

select
    cast(game_id as int64) as game_id,
    cast(play_id as int64) as play_id,
    cast(game_date as date) as game_date,
    cast(season as int64) as season,
    cast(week as int64) as week,
    season_type,
    upper(home_team) as home_team,
    upper(away_team) as away_team,
    upper(posteam) as possession_team,
    upper(defteam) as defensive_team,
    posteam_type as possession_team_type,
    cast(qtr as int64) as quarter,
    cast(down as int64) as down,
    cast(ydstogo as int64) as yards_to_go,
    cast(yardline_100 as int64) as yardline_100,
    cast(goal_to_go as int64) as goal_to_go,
    play_type,
    `desc` as play_description,
    cast(yards_gained as int64) as yards_gained,
    cast(epa as float64) as epa,
    cast(wpa as float64) as wpa,
    cast(wp as float64) as win_probability,
    cast(pass_attempt as int64) as pass_attempt,
    cast(rush_attempt as int64) as rush_attempt,
    cast(complete_pass as int64) as complete_pass,
    cast(incomplete_pass as int64) as incomplete_pass,
    cast(interception as int64) as interception,
    cast(fumble_lost as int64) as fumble_lost,
    cast(touchdown as int64) as touchdown,
    cast(pass_touchdown as int64) as pass_touchdown,
    cast(rush_touchdown as int64) as rush_touchdown,
    cast(total_home_score as int64) as total_home_score,
    cast(total_away_score as int64) as total_away_score,
    nullif(passer_player_id, '') as passer_gsis_id,
    passer_player_name,
    nullif(rusher_player_id, '') as rusher_gsis_id,
    rusher_player_name,
    nullif(receiver_player_id, '') as receiver_gsis_id,
    receiver_player_name
from source
where game_id is not null
  and play_id is not null
