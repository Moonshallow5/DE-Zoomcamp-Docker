# NFL play-by-play dbt project

This project transforms the raw BigQuery `plays`, `players`, and `teams` tables
into tested, documented models for a Looker Studio dashboard.

## Model layers

```text
BigQuery raw tables
        |
        v
models/staging/        Rename columns, select useful fields, basic cleanup
        |
        v
models/marts/core/     Reusable dimensions and enriched play fact
        |
        v
models/marts/reporting Dashboard-ready aggregates
```

### Grain and joins

- `fct_plays`: one row per `(game_id, play_id)`.
- `dim_teams`: one row per `(season, team_id)`.
- `dim_players`: one row per `(season, nfl_id, team_id)`.
- Plays join teams with `(season, team abbreviation)`.
- Play participant IDs join `players.gsisId`, not `players.nflId`.

## Setup

Raw sources (already loaded):

- `de-zoomcamp-501902.demo_dataset.plays` (partitioned by `game_date`)
- `de-zoomcamp-501902.demo_dataset.players`
- `de-zoomcamp-501902.demo_dataset.teams`

Profile: add `nfl_play_by_play` from `profiles.yml.example` into `~/.dbt/profiles.yml`
(already configured on this machine to use the Zoomcamp service-account key).

dbt writes models into:

- `nfl_dbt_staging`
- `nfl_dbt_marts`
- `nfl_dbt_reporting`

## Run

From this directory:

```powershell
dbt deps
dbt debug
dbt build
dbt docs generate
dbt docs serve
```

Useful development commands:

```powershell
dbt run --select staging
dbt run --select +fct_plays
dbt build --select marts.reporting
dbt test
dbt show --select mart_team_season_performance --limit 10
```

`dbt build` is the normal CI-style command: it runs models and their tests in
dependency order. Prefer it over running models and tests separately.

## Dashboard sources and suggested charts

### `mart_team_season_performance`

- Ranked bar: average EPA per play by team
- Scatter: pass rate vs average EPA, sized by offensive plays
- Scorecards: total yards, touchdowns, turnovers
- Controls: season, conference, division

### `mart_quarterback_season`

- Ranked table: QB, attempts, completion rate, total EPA, interceptions
- Scatter: completion rate vs average EPA per attempt
- Controls: season and team

### `mart_weekly_play_trends`

- Time series: weekly average EPA
- Stacked area: pass vs run play counts
- Time series: touchdowns and turnovers
- Controls: season, team, play type

Start with one categorical chart and one temporal chart, then add filters and
scorecards. Connect Looker Studio to the reporting models rather than the
306-column raw `plays` table.

## Why these conventions matter

- Sources describe tables owned by ingestion.
- Staging models are thin and normally materialized as views.
- Core marts establish stable business grains and relationships.
- Reporting marts pre-aggregate dashboard metrics for simpler, cheaper queries.
- YAML files document columns and attach automated data-quality tests.
- `ref()` and `source()` create dbt's lineage graph and safe build order.
