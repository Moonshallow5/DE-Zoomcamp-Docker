/* @bruin

name: dataset.player_stats
type: duckdb.sql

materialization:
  type: table

depends:
  - dataset.players

columns:
  - name: name
    type: string
    description: the column contains the player names
    checks:
      - name: not_null
      - name: unique
  - name: player_count
    type: integer
    description: the number of players with the given name
    checks:
      - name: not_null
      - name: positive

custom_checks:
  - name: row count is greater than 0
    query: SELECT COUNT(*) > 0 FROM dataset.player_stats
    value: 1

@bruin */

SELECT name, COUNT(*) AS player_count
FROM dataset.players
GROUP BY name
