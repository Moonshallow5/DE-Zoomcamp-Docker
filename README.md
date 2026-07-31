# Data Engineering Zoomcamp

Coursework from the [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp), plus a final project.

## Weeks

| Week | Focus |
|------|--------|
| **1** | Terraform — initializing GCP components (project setup, infrastructure as code) |
| **2** | Kestra — ingesting Parquet files and loading them into GCS |
| **3** | BigQuery — partitioned tables and clustering / indexes (work done in the BigQuery console; no local folder) |
| **4** | dbt — basic transforms using boilerplate project code (`week-4/jaffle_shop`) |
| **5** | Bruin — ingestion and processing pipelines (`week-5/zoomcamp`) |
| **6** | Spark — simple PySpark exercises on taxi data (`week-6`) |
| **7** | Kafka — basics of streaming with producers/consumers (`week-7`) |

## Final project

[`final-project/nfl-play-by-play`](final-project/nfl-play-by-play) — end-to-end pipeline on NFL play-by-play data:

1. Extract NFL data (teams, players, plays) and land it in **GCS**
2. Load into **BigQuery** (including a partitioned `plays` table)
3. Transform with **dbt** (staging → dimensions/facts → reporting marts)
4. Build a **Looker Studio** dashboard from the reporting tables

Reporting models used for the dashboard:

- `mart_team_season_performance`
- `mart_quarterback_season`
- `mart_weekly_play_trends`

### Dashboard export

Looker Studio snapshot (PDF):

[`final-project/nfl-play-by-play/nfl-play-by-play.pdf`](final-project/nfl-play-by-play/nfl-play-by-play.pdf)

Charts included:

- QB bar chart — `avg_epa_per_attempt` by quarterback (`mart_quarterback_season`)
- Team bar chart — `avg_epa_per_play` by team (`mart_team_season_performance`)
- Weekly time series — EPA over time (`mart_weekly_play_trends`)

Flow Image:


<img width="800" height="200" alt="dde-zoomcamp" src="https://github.com/user-attachments/assets/2a14f430-5ab3-4c79-9123-9aba79cd9bca" />

