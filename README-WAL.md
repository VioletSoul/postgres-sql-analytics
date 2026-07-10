![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15.5-336791?style=flat&logo=postgresql&logoColor=white)
![SQL](https://img.shields.io/badge/Language-SQL-blue)
![Scope](https://img.shields.io/badge/Scope-cluster--level-success)
![Focus](https://img.shields.io/badge/Focus-WAL%20%7C%20Replication%20%7C%20Archiving-orange)
![Diagnostics](https://img.shields.io/badge/Diagnostics-health%20%7C%20lag%20%7C%20retention-critical)

![Repo Size](https://img.shields.io/github/repo-size/VioletSoul/postgres-sql-analytics)
![Code Size](https://img.shields.io/github/languages/code-size/VioletSoul/postgres-sql-analytics)
[![Stars](https://img.shields.io/github/stars/VioletSoul/postgres-sql-analytics.svg?style=social)](https://github.com/VioletSoul/postgres-sql-analytics)
[![Last Commit](https://img.shields.io/github/last-commit/VioletSoul/postgres-sql-analytics.svg)](https://github.com/VioletSoul/postgres-sql-analytics/commits/main)

# PostgreSQL WAL Analytics & Health Assessment

## Purpose of `wal_analytics.sql`

`wal_analytics.sql` is a production-oriented diagnostic query for PostgreSQL 15.x that provides a holistic view of the cluster’s WAL subsystem and operational health.

![EikYLhgpxgjC0a0k.png](EikYLhgpxgjC0a0k.png)

It helps quickly understand:

- how much WAL has been generated since the last statistics reset
- current WAL generation rate
- WAL archiving reliability
- replication health and standby lag
- replication slot retention risks
- checkpoint activity pressure
- overall WAL subsystem health status

The query is not tied to any specific schema or database and analyzes server-level PostgreSQL statistics.

---

# Features

The current version provides:

- WAL generation metrics
- WAL archive success ratio
- Replication lag analysis
- Replication slot retention monitoring
- Checkpoint pressure detection
- Configurable warning and critical thresholds
- Severity-based health checks
- Overall health score calculation
- Automated operational recommendations

---

# Data sources

The report uses standard PostgreSQL statistics views:

- `pg_stat_wal` – WAL generation statistics
- `pg_stat_archiver` – WAL archiver status and archive success ratio
- `pg_stat_replication` – streaming replication status and standby lag
- `pg_replication_slots` – replication slot state and retained WAL
- `pg_stat_bgwriter` – checkpoint activity statistics

---

# High-level structure

The query is built as a sequence of CTEs (`WITH` clauses), where each stage collects and enriches a specific part of the WAL subsystem.

---

## 1. `config`

Contains configurable operational thresholds used by the health engine.

Examples:

- replication lag warning threshold
- replication lag critical threshold
- replication slot retention warning threshold
- replication slot retention critical threshold
- archive success ratio limits

Thresholds can be adjusted depending on environment requirements.

---

## 2. `wal_stat`

Takes a snapshot of current WAL statistics:

- current timestamp
- `stats_reset` moment
- total WAL records (`wal_records`)
- full page images (`wal_fpi`)
- generated WAL size (`wal_bytes`)
- WAL buffer pressure counters
- WAL write and sync counters

These are cumulative PostgreSQL counters maintained since the last statistics reset.

---

## 3. `wal_rate`

Calculates WAL generation velocity:

- time elapsed since `stats_reset`
- average WAL generation rate in bytes per second

This metric helps estimate:

- WAL storage requirements
- workload intensity
- potential `pg_wal` growth

---

## 4. `wal_archiver`

Analyzes WAL archive health.

Collected metrics:

- successful archive count (`archived_count`)
- failed archive attempts (`failed_count`)
- last successful archive information
- last failed archive information
- archive success ratio

Example:
```
Archive success ratio: 100%
Failed archives: 0
```
A decreasing archive success ratio may indicate:

- archive command failures
- storage problems
- permission issues
- unavailable archive destinations

---

## 5. `checkpoint_stats`

Collects checkpoint-related metrics from:

- `pg_stat_bgwriter`

Analyzed values:

- timed checkpoints
- requested checkpoints
- checkpoint write time
- checkpoint sync time
- checkpoint buffer activity

The query detects abnormal checkpoint pressure patterns.

---

## 6. `replication`

Analyzes every connected standby.

Collected information:

- replica name
- client address
- replication state
- synchronous/asynchronous mode
- replay lag
- write lag
- flush lag

Lag is calculated using WAL LSN differences.

This allows detection of:

- slow replicas
- network delays
- replay bottlenecks
- standby failures

---

## 7. `replication_slots`

Analyzes replication slot usage.

Collected fields:

- slot name
- slot type
- active state
- WAL status
- retained WAL volume

Large WAL retention may indicate:

- inactive consumers
- broken logical replication pipelines
- disconnected applications

---

# Health engine

The diagnostic layer evaluates every subsystem and assigns a severity level:

| Severity | Meaning |
|----------|---------|
| OK | Healthy state |
| INFO | Informational condition |
| WARNING | Requires attention |
| CRITICAL | Immediate investigation required |

Checks include:

- replication lag
- WAL archive health
- replication slot retention
- checkpoint pressure

Example:
```
Replication lag:
OK

Archiver:
OK

Slot retention:
WARNING
```
---

# Health score

The query calculates an overall WAL subsystem health score.

Example:
```
Score:
100

Status:
HEALTHY
```
The score is automatically reduced when problems are detected:

- WARNING conditions decrease the score
- CRITICAL conditions apply larger penalties

This provides a quick operational overview suitable for dashboards or manual checks.

---

# Recommendations

The query generates automated DBA hints based on detected conditions.

Examples:
```
Replication lag requires attention.

Recommendation:
Monitor standby performance and network latency.
```
Examples:
```
Replication slot retention detected.

Recommendation:
Review inactive slots and WAL consumers.
```
---

# Final result set

The final output is grouped by the `section` column.

## `health`

Overall cluster WAL status:

- health score
- health state

---

## `check`

Individual diagnostic results:

- check name
- severity
- description

---

## `recommendation`

Operational suggestions generated from detected conditions.

---

## `summary`

Global WAL subsystem overview:

- total WAL generated
- WAL generation rate
- archive statistics
- archive success ratio
- replica count
- maximum replication lag
- slot statistics
- maximum retained WAL

---

## `replica`

One row per standby:

- application name
- replication state
- synchronization mode
- replay lag

---

## `slot`

One row per replication slot:

- slot name
- active state
- retained WAL volume

---

# Typical usage scenarios

- Quick WAL health check during incidents
- PostgreSQL upgrade validation
- Replication troubleshooting
- Monitoring WAL growth
- Detecting archive failures
- Identifying replication slot risks
- Feeding monitoring dashboards

The unified output structure makes it easy to split results into separate monitoring panels.

---

# How to read key fields

## WAL generation

`wal_generated_total`

Total WAL generated since the last statistics reset.

---

## WAL rate

`wal_bytes_per_second`

Average WAL generation speed.

Useful for:

- capacity planning
- estimating WAL storage growth

---

## Replication

`max_replay_lag`

Maximum replay delay among replicas.

Increasing values may indicate:

- standby resource limitations
- network issues
- replication bottlenecks

---

## Replication slots

`max_retained_wal`

Largest WAL volume retained by a replication slot.

High values may require:

- slot cleanup
- consumer recovery
- replication pipeline investigation

---

## Archive health

`archive_success_ratio`

Percentage of successful WAL archive operations.

A decreasing ratio indicates archive subsystem problems.

---

# Compatibility

Tested with:
```
PostgreSQL 15.5
```
The query uses only built-in PostgreSQL statistics views and does not require additional extensions.