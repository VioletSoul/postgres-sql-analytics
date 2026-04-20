## Purpose of `wal_analytics.sql`

`wal_analytics.sql` is a diagnostic query for PostgreSQL 15.x that provides a holistic view of the cluster’s WAL subsystem.  
It helps you quickly understand:

- how much WAL has been generated since the last stats reset
- the current WAL generation rate
- how WAL archiving is working
- whether there are replication problems (replica lag)
- whether replication slots are retaining too much WAL and risking disk exhaustion

The query is not tied to any specific schema or database and analyzes server‑level state.

---

## Data sources

The report uses standard PostgreSQL system views:

- `pg_stat_wal` – WAL generation statistics
- `pg_stat_archiver` – WAL archiver status
- `pg_stat_replication` – streaming replication status and lag per standby
- `pg_replication_slots` – replication slot state and retained WAL

---

## High‑level structure

The query is built as a series of CTEs (WITH clauses), each responsible for its own part of the picture.

### 1. `wal_stat`

Takes a snapshot of current WAL statistics:

- current timestamp and `stats_reset` moment
- total number of WAL records (`wal_records`)
- number of full page images (`wal_fpi`)
- total size of generated WAL in bytes (`wal_bytes`)
- counters related to WAL buffers and write/sync operations

These are raw cumulative counters that PostgreSQL maintains since the last stats reset.

### 2. `wal_rate`

Based on `wal_stat` it computes:

- time elapsed since `stats_reset`
- average WAL generation rate in bytes per second (`wal_bytes_per_second`)

This helps assess the current write intensity and plan disk space for `pg_wal`.

### 3. `wal_archiver`

Collects WAL archiver status:

- number of successfully archived WAL segments (`archived_count`)
- number of failed archive attempts (`failed_count`)
- last successfully archived segment and timestamp (`last_archived_wal`, `last_archived_time`)
- last failed segment and timestamp (`last_failed_wal`, `last_failed_time`)

These metrics show whether archiving is working reliably or there are unresolved errors.

### 4. `wal_replication` and `wal_replication_summary`

`wal_replication` reads, for each standby:

- `application_name`, client address
- connection `state` and `sync_state` (sync/async)
- LSN‑based lag in bytes: `sent_lag_bytes`, `write_lag_bytes`, `flush_lag_bytes`, `replay_lag_bytes`
- when available, time‑based lags: `write_lag`, `flush_lag`, `replay_lag`

`wal_replication_summary` aggregates these per‑replica metrics:

- total replica count
- how many are not in `streaming` state
- counts of synchronous and asynchronous replicas
- maximum `replay`, `write`, and `flush` lag in bytes across all standbys

This allows you to see at a glance whether any replicas are significantly behind.

### 5. `wal_slots` and `wal_slots_summary`

`wal_slots` analyzes replication slots:

- `slot_name`, `slot_type` (`physical` / `logical`)
- whether the slot is `active`
- `wal_status` (`normal`, `reserved`, `extended`, `lost`, etc.)
- amount of WAL retained by the slot that cannot yet be removed (`retained_wal_bytes`)

`wal_slots_summary` aggregates this information:

- total slot count
- number of inactive slots
- counts of slots in `lost`, `reserved`, `extended` states
- maximum retained WAL volume per slot

This helps detect situations where a forgotten slot is accumulating gigabytes of WAL.

### 6. `summary` – global overview and risk flags

The `summary` CTE merges all previous CTEs and produces:

- key numerical indicators (total WAL, rate, number of replicas/slots, lag values)
- aggregated lag metrics
- several boolean risk flags:

    - `archiver_has_recent_errors` – there are recent archive errors not followed by a successful run
    - `replication_lag_risk` – maximum replication lag exceeds a configured byte threshold
    - `slot_retention_risk` – WAL retained by at least one slot exceeds a configured byte threshold

---

## Final result set

The final `SELECT` returns a single table, with rows grouped by the `section` column:

- `section = 'summary'` – a single aggregated row with a global cluster overview
- `section = 'replica'` – one row per replica, with its individual lag and state
- `section = 'slot'` – one row per replication slot, with retained WAL and status

The `name` column contains:

- `NULL` for `summary`
- `application_name` for `replica` rows
- `slot_name` for `slot` rows

Some columns are populated only for `summary` (archiver metrics, global counters); for replica and slot rows those fields are `NULL`.  
This provides a **single, uniform schema** while still logically separating the three types of information.

---

## Typical usage scenarios

- Quick WAL and replication health check during incidents
- Regular manual audits of cluster state (e.g. after upgrades or major config changes)
- As a data source for dashboards: the `section` column makes it easy to split the result into multiple panels
- Evaluating the risk of disk exhaustion due to replication lag or slots retaining WAL

---

## How to read the key fields

- `wal_generated_total` (`summary`) – total WAL generated since `stats_reset`
- `wal_bytes_per_second` / `wal_rate_human` – average WAL generation rate; useful for capacity planning
- `max_replay_lag`, `max_write_lag`, `max_flush_lag` – maximum lag across all replicas; growth indicates replication or I/O issues
- `max_retained_wal` (`summary`) and `wal_generated_total` for `section = 'slot'` – amount of WAL held by slots; shows whether slot management needs attention
- `archiver_has_recent_errors`, `replication_lag_risk`, `slot_retention_risk` – high‑level flags pointing to where to investigate first
