## Overview

The query is designed to collect extended statistics and diagnostics for tables and indexes in the `dspc` schema in PostgreSQL.  
It is built as a series of CTE subqueries that aggregate statistical, technical, and operational metrics for further analysis, optimization, and database health monitoring.

---

## Query structure

The query is based on four coordinated common table expressions (CTEs):

- `table_stats` – table-level statistics  
- `index_stats` – index-level statistics  
- `bloat_stats` – table bloat estimation  
- `lock_stats` – current lock status  

The final `SELECT` joins these datasets into a single result.

---

## `table_stats`

Data is taken from PostgreSQL system catalogs and statistics views:

- `pg_class`, `pg_namespace` – table and schema metadata  
- `pg_stat_all_tables`, `pg_statio_all_tables` – runtime statistics on table usage and I/O  

Filtering is limited to “live” relational tables (type `r`) in the `dspc` schema (excluding system/maintenance schemas).

Collected metrics include:

- `schema_name` – schema name  
- `table_name` – table name  
- `table_oid` – object OID  
- `total_bytes` – total table size including indexes and TOAST  
- `table_bytes` – heap size only  
- `approx_row_count` – approximate row count (PostgreSQL estimate)  
- `table_pages` – number of heap pages  
- `seq_scans` – number of sequential (full) scans  
- `seq_tuples_read` – tuples read by sequential scans  
- `index_scans` – number of index scans  
- `index_tuples_fetched` – tuples fetched via indexes  
- `n_tup_ins`, `n_tup_upd`, `n_tup_del` – inserts, updates, deletes  
- `n_live_tup`, `n_dead_tup` – live vs dead tuples  
- `n_tup_hot_upd` – HOT updates  
- `n_mod_since_analyze` – modifications since last `ANALYZE`  
- `last_vacuum`, `last_autovacuum`, `last_analyze`, `last_autoanalyze` – timestamps of maintenance operations  
- `vacuum_count`, `autovacuum_count`, `analyze_count`, `autoanalyze_count` – maintenance counters  
- `heap_blocks_read` / `heap_blocks_hit` – heap I/O (reads vs cache hits)  
- `idx_blocks_read` / `idx_blocks_hit` – index I/O  
- `toast_blocks_read` / `toast_blocks_hit` – TOAST I/O  

---

## `index_stats`

Built on top of `table_stats`, `pg_index`, `pg_class`, `pg_stat_all_indexes`, `pg_statio_all_indexes`.

Analyzed fields:

- `index_name` – index name  
- `index_oid` – index OID  
- `is_unique` / `is_primary` – uniqueness and primary key flag  
- `is_valid` / `is_ready` – index validity and readiness  
- `index_bytes` – index size  
- `index_scans` – total number of index scans  
- `index_tuples_read` / `index_tuples_fetched` – tuples read/fetched via index  
- `idx_blocks_read` / `idx_blocks_hit` – index I/O counters  
- `usage_status` – index usage status (`USED` if there were scans, otherwise `UNUSED`)  

---

## `bloat_stats`

Estimates table bloat using dead tuple counts:

- `estimated_bloat_percent` – percentage of dead tuples relative to all tuples  
- `estimated_bloat_bytes` – corresponding volume in bytes  

These metrics are a heuristic indicator of when `VACUUM` may be required.

---

## `lock_stats`

Aggregates lock information from `pg_locks` for tables in the target set:

- `table_name` – affected table  
- `active_locks` – number of active locks  
- `lock_modes` – list of lock modes (`RowExclusiveLock` and others)  

---

## Final `SELECT`

The final query combines all metrics and produces one row per table–index pair.

For each table it shows:

- `schema_name`, `table_name` – object identity  
- Human‑readable sizes and page count  
- `table_trow_count` – estimated row count  
- `seq_scans`, `seq_tuples_read` – full scan statistics  
- `table_index_scans`, `table_index_fetches` – index scan statistics  
- `inserts`, `updates`, `deletes`, `hot_updates` – DML activity  
- `live_tuples`, `dead_tuples`  
- `dead_tuple_ratio_percent` – dead/live tuple ratio  
- `changes_since_analyze` – modifications since last `ANALYZE`  
- `last_vacuum` / `last_autovacuum` / `last_analyze` / `last_autoanalyze`  
- `vacuum_count`, `autovacuum_count`, `analyze_count`, `autoanalyze_count`  
- `heap_blocks_read` / `heap_blocks_hit` – table I/O activity  
- `heap_cache_hit_ratio_percent` – table cache hit ratio  
- `estimated_bloat_size`, `estimated_bloat_percent` – bloat estimation  
- `active_locks_count`, `active_lock_modes` – lock status  

For each index it shows:

- `index_name`  
- `index_type` (`PRIMARY` / `UNIQUE` / `REGULAR`)  
- `index_status` (`READY` / `INVALID`)  
- `index_size`  
- `idx_scans`, `idx_tuples_read`, `idx_tuples_fetched`, `index_usage_status`  
- `idx_blocks_read` / `idx_blocks_hit`, `index_cache_hit_ratio_percent`  
- `index_selectivity_ratio` – selectivity (fetched/read)  
- `index_size_percent` – index size as a percentage of total table size  

Additionally, it includes:

- `recommendation` – automated DBA hint based on detected patterns (e.g. create index, drop unused index, run `VACUUM`/`ANALYZE`).  

---

## Diagnostic logic

Recommendations are derived from simple rules:

- If sequential scans significantly outnumber index scans → `CONSIDER_INDEX`  
- If an index is large but unused → `UNUSED_INDEX`  
- If bloat exceeds 20% → `NEEDS_VACUUM`  
- If modifications exceed 10% of live tuples → `NEEDS_ANALYZE`  
- If no issues are detected → `OK`  

---

## Sorting

Result rows are sorted by:

1. Total table size (descending)  
2. Table name  
3. Index size (descending)  

---

## Purpose

This query provides a comprehensive audit of table, index, and lock state for the `dspc` schema.  
It can be used as a technical report for systematic analysis of:

- maintenance quality (`VACUUM` / `ANALYZE`),  
- performance characteristics (scan patterns, cache hit ratios),  
- storage layout and bloat,  
- index usefulness and selectivity.
