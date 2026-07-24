-- A dbt data test passes when this query returns zero rows.
select
    game_id,
    play_id,
    count(*) as row_count
from {{ ref('stg_nfl__plays') }}
group by 1, 2
having count(*) > 1
