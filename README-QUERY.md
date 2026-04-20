## Purpose of `query_analytics.sql`

`query_analytics.sql` is a diagnostic query for PostgreSQL 15.x that provides an overview of SQL statement behavior based on `pg_stat_statements`.  
It helps you answer questions such as:

- which queries consume the most total execution time
- which queries are slow on average
- which queries generate the highest I/O (shared/local/temp blocks)
- where cache efficiency is low
- which queries suffer from excessive planning overhead

The query operates at the cluster level and is not tied to any specific schema.

---

## Data source

The primary data source is the `pg_stat_statements` view, which aggregates statistics about SQL planning and execution:

- number of calls (`calls`)
- total and mean execution time (`total_exec_time`, `mean_exec_time`)
- total and mean planning time (`total_plan_time`, `mean_plan_time`)
- number of returned rows (`rows`)
- buffer activity: `shared_blks_*`, `local_blks_*`, `temp_blks_*`
- block I/O times (`blk_read_time`, `blk_write_time`)

Additional metadata is taken from:

- `pg_roles` → `user_name`
- `pg_database` → `database_name`

---

## High‑level structure

The query is built as a chain of CTEs (WITH clauses):

1. `query_base`
2. `query_enriched`
3. `query_ranked`
4. `query_diagnostics`
5. `query_summary`
6. `top_queries`
7. final `SELECT` with `UNION ALL`

Each layer adds its own aggregation and derived metrics.

---

## 1. `query_base`

The base layer reads raw data from `pg_stat_statements`:

- pulls:
    - identifiers: `userid`, `dbid`, `queryid`
    - raw query text (`query`)
    - planning and execution times
    - block counters and I/O times
- joins:
    - `pg_roles` for `user_name`
    - `pg_database` for `database_name`
- filters out:
    - system databases (`template0`, `template1`)
    - queries against `pg_stat_statements` itself (to avoid self‑noise)

The result is raw query statistics with user/database context.

---

## 2. `query_enriched`

This step adds derived metrics and simple classification:

- `normalized_query_preview`
    - query text with whitespace normalized and truncated to 500 characters
    - makes result sets easier to read in tables/dashboards

- workload metrics:
    - `total_shared_block_access` — total shared buffer accesses
    - `total_local_block_access` — total local buffer accesses
    - `total_temp_block_activity` — total temp block activity

- cache efficiency:
    - `shared_cache_hit_ratio_percent` = shared hits / (shared hits + reads)

- per‑call averages:
    - `avg_exec_time_ms` — average execution time per call
    - `avg_plan_time_ms` — average planning time per call
    - `avg_rows_per_call` — average rows returned per call
    - `avg_shared_reads_per_call` — average shared reads per call
    - `avg_temp_written_per_call` — average temp blocks written per call

- relative time breakdown:
    - `planning_to_execution_percent` — share of total time spent on planning
    - `read_io_time_percent` / `write_io_time_percent` — share of time spent in block reads/writes

- query type:
    - `query_type` — simple classification based on leading keyword:
        - `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `VACUUM`, `ANALYZE`, `CREATE`, `ALTER`, `DROP`, `OTHER`

This transforms low‑level counters into a more readable profile for each `queryid`.

---

## 3. `query_ranked`

This layer computes ranks along key axes:

- `rank_total_exec_time` — by total execution time
- `rank_mean_exec_time` — by mean execution time
- `rank_calls` — by number of calls
- `rank_shared_reads` — by shared block reads
- `rank_temp_written` — by temp blocks written

Ranks are useful when you want to quickly find “top offenders”.

---

## 4. `query_diagnostics`

Here the query performs classification and generates recommendations.

### `diagnostic_flag`

Each query is assigned one of several diagnostic flags:

- `FREQUENT_AND_SLOW`
    - high call count (`calls >= 1000`) combined with high mean latency (`mean_exec_time >= 50 ms`)

- `SLOW_QUERY`
    - high `mean_exec_time` (≥ 500 ms) even if not called frequently

- `HEAVY_SHARED_READ`
    - large volume of shared block reads (`shared_blks_read >= 100000`)

- `TEMP_SPILL_RISK`
    - significant temporary block writes (`temp_blks_written >= 10000`)

- `LOW_CACHE_EFFICIENCY`
    - low shared cache hit ratio (< 90%) with non‑trivial read volume

- `PLANNING_OVERHEAD`
    - planning time > 30% of `total_exec_time` and `calls > 100`

- `NORMAL`
    - none of the above conditions are met

### `recommendation`

Based on the flag, a short textual recommendation is produced:

- `FREQUENT_AND_SLOW` → review execution plan and indexing strategy
- `SLOW_QUERY` → investigate plan, join strategy, filters, indexes
- `HEAVY_SHARED_READ` → check access pattern and index coverage
- `TEMP_SPILL_RISK` → review `work_mem`, sort/hash operations and plan structure
- `LOW_CACHE_EFFICIENCY` → examine access locality, table sizes, memory pressure
- `PLANNING_OVERHEAD` → look at plan reuse and statement normalization (e.g. parameterization)
- otherwise → “No immediate performance risk detected.”

---

## 5. `query_summary`

An aggregate cluster‑wide summary:

- `query_count` — number of distinct queries (queryids)
- `flagged_query_count` — how many queries have a diagnostic flag other than `NORMAL`
- `total_calls` — total number of calls across all queries
- `total_exec_time_ms` — total execution time (ms) across all queries
- `avg_mean_exec_time_ms` — average of `mean_exec_time` across queries
- `total_shared_blks_read` / `total_shared_blks_hit` — cumulative shared buffer reads/hits
- `total_temp_blks_written` — total temp blocks written
- `max_mean_exec_time` / `max_total_exec_time` — the worst queries by mean and total time

This row gives you a quick sense of the overall workload and pain level.

---

## 6. `top_queries`

Selects the top‑N queries (default 100) from `query_diagnostics`:

- ordered primarily by `total_exec_time`
- ties broken by `calls`

This is the main working set for detailed performance analysis.

---

## 7. Final result

The final `SELECT` returns a single result set with two logical sections, distinguished by the `section` column:

- `section = 'summary'`
    - single aggregate row from `query_summary`

- `section = 'query'`
    - up to 100 rows from `top_queries`, each representing one query, with:
        - database, user, `queryid`, `query_type`
        - execution and planning metrics
        - I/O metrics
        - diagnostic flags and recommendations
        - shortened query text (`normalized_query_preview`)

Some columns are meaningful only for `summary`, others only for `query`. Non‑applicable fields are `NULL` so that the schema stays uniform.

---

## Typical usage scenarios

- Quickly identify which queries dominate total execution time.
- Find single very slow queries (high `mean_exec_time`).
- Detect queries that heavily stress disk I/O (shared/temp).
- Spot planning overhead issues (high `planning_to_execution_percent`).
- Prepare reports for developers listing problematic queries with flags and recommendations.

---

## How to read key fields

- `diagnostic_flag` / `recommendation` — first signal and hint where to look.
- `calls` + `mean_exec_time` — distinguish “frequent & slow” from “rare but catastrophic”.
- `shared_blks_read` / `shared_blks_hit` and `shared_cache_hit_ratio_percent` — cache efficiency.
- `temp_blks_written` — heavy sorts/hashes, often a sign to revisit `work_mem` and query plans.
- `planning_to_execution_percent` — if high, planning overhead is significant and you may benefit from parameterization and plan reuse.
