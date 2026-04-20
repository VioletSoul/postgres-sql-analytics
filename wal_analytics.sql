/*
  wal_analytics.sql
  PostgreSQL 15.5

  Comprehensive WAL diagnostics:
    - WAL generation stats (pg_stat_wal)
    - Archiver status (pg_stat_archiver)
    - Replication lag per standby (pg_stat_replication)
    - WAL retention per replication slot (pg_replication_slots)
    - Derived metrics: WAL rate, max lag, risk flags
*/

WITH
-- 1. Base WAL metrics
wal_stat AS (
    SELECT
        now()                              AS ts,
        stats_reset,
        wal_records,
        wal_fpi,
        wal_bytes,
        wal_buffers_full,
        wal_write,
        wal_sync
    FROM pg_stat_wal
),

-- 2. WAL generation rate (bytes per second)
wal_rate AS (
    SELECT
        ws.ts,
        ws.stats_reset,
        ws.wal_bytes,
        EXTRACT(EPOCH FROM (ws.ts - ws.stats_reset)) AS seconds_since_reset,
        CASE
            WHEN ws.ts > ws.stats_reset
                THEN ws.wal_bytes::numeric
    / GREATEST(EXTRACT(EPOCH FROM (ws.ts - ws.stats_reset)), 1)
    ELSE NULL
END AS wal_bytes_per_second
    FROM wal_stat ws
),

-- 3. Archiver status
wal_archiver AS (
    SELECT
        archived_count,
        failed_count,
        last_archived_wal,
        last_archived_time,
        last_failed_wal,
        last_failed_time,
        stats_reset AS archiver_stats_reset
    FROM pg_stat_archiver
),

-- 4. Replication lag per standby
wal_replication AS (
    SELECT
        application_name,
        client_addr,
        state,
        sync_state,
        sent_lsn,
        write_lsn,
        flush_lsn,
        replay_lsn,
        pg_wal_lsn_diff(pg_current_wal_lsn(), sent_lsn)   AS sent_lag_bytes,
        pg_wal_lsn_diff(pg_current_wal_lsn(), write_lsn)  AS write_lag_bytes,
        pg_wal_lsn_diff(pg_current_wal_lsn(), flush_lsn)  AS flush_lag_bytes,
        pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn) AS replay_lag_bytes,
        write_lag,
        flush_lag,
        replay_lag
    FROM pg_stat_replication
),

wal_replication_summary AS (
    SELECT
        COUNT(*)                                              AS replica_count,
        SUM(CASE WHEN state <> 'streaming' THEN 1 ELSE 0 END) AS non_streaming_replicas,
        SUM(CASE WHEN sync_state = 'sync'  THEN 1 ELSE 0 END) AS sync_replicas,
        SUM(CASE WHEN sync_state = 'async' THEN 1 ELSE 0 END) AS async_replicas,
        MAX(replay_lag_bytes)                                 AS max_replay_lag_bytes,
        MAX(write_lag_bytes)                                  AS max_write_lag_bytes,
        MAX(flush_lag_bytes)                                  AS max_flush_lag_bytes
    FROM wal_replication
),

-- 5. Replication slots and retained WAL
wal_slots AS (
    SELECT
        slot_name,
        slot_type,
        active,
        wal_status,
        restart_lsn,
        confirmed_flush_lsn,
        pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS retained_wal_bytes
    FROM pg_replication_slots
),

wal_slots_summary AS (
    SELECT
        COUNT(*)                                             AS slot_count,
        SUM(CASE WHEN NOT active THEN 1 ELSE 0 END)          AS inactive_slots,
        SUM(CASE WHEN wal_status = 'lost' THEN 1 ELSE 0 END) AS lost_slots,
        SUM(CASE WHEN wal_status = 'reserved' THEN 1 ELSE 0 END) AS reserved_slots,
        SUM(CASE WHEN wal_status = 'extended' THEN 1 ELSE 0 END) AS extended_slots,
        MAX(retained_wal_bytes)                              AS max_retained_wal_bytes
    FROM wal_slots
),

-- 6. Global summary and risk flags
summary AS (
    SELECT
        ws.ts,
        ws.stats_reset,
        wr.seconds_since_reset,
        wr.wal_bytes,
        wr.wal_bytes_per_second,
        wa.archived_count,
        wa.failed_count,
        wa.last_archived_wal,
        wa.last_archived_time,
        wa.last_failed_wal,
        wa.last_failed_time,
        wrs.replica_count,
        wrs.non_streaming_replicas,
        wrs.sync_replicas,
        wrs.async_replicas,
        wrs.max_replay_lag_bytes,
        wrs.max_write_lag_bytes,
        wrs.max_flush_lag_bytes,
        wss.slot_count,
        wss.inactive_slots,
        wss.lost_slots,
        wss.reserved_slots,
        wss.extended_slots,
        wss.max_retained_wal_bytes,
        CASE
            WHEN wa.failed_count > 0
                 AND wa.last_failed_time IS NOT NULL
                 AND (wa.last_archived_time IS NULL
                      OR wa.last_failed_time > wa.last_archived_time)
                THEN TRUE
            ELSE FALSE
        END AS archiver_has_recent_errors,
        CASE
            WHEN wrs.max_replay_lag_bytes IS NOT NULL
                 AND wrs.max_replay_lag_bytes > 1024 * 1024 * 1024
                THEN TRUE
            ELSE FALSE
        END AS replication_lag_risk,
        CASE
            WHEN wss.max_retained_wal_bytes IS NOT NULL
                 AND wss.max_retained_wal_bytes > 5 * 1024 * 1024 * 1024
                THEN TRUE
            ELSE FALSE
        END AS slot_retention_risk
    FROM wal_stat ws
    JOIN wal_rate wr                  ON wr.ts = ws.ts
    LEFT JOIN wal_archiver wa         ON TRUE
    LEFT JOIN wal_replication_summary wrs ON TRUE
    LEFT JOIN wal_slots_summary wss        ON TRUE
)

-- 7. Final output: summary + per‑replica + per‑slot
SELECT
    'summary'::text                               AS section,
    NULL::text                                    AS name,
    to_char(s.ts, 'YYYY-MM-DD HH24:MI:SS')        AS ts,
    pg_size_pretty(s.wal_bytes)                   AS wal_generated_total,
    round(s.wal_bytes_per_second)::bigint         AS wal_bytes_per_second,
    pg_size_pretty(s.wal_bytes_per_second)        AS wal_rate_human,
    s.archived_count,
    s.failed_count,
    s.last_archived_wal,
    s.last_archived_time,
    s.last_failed_wal,
    s.last_failed_time,
    s.replica_count,
    s.non_streaming_replicas,
    s.sync_replicas,
    s.async_replicas,
    pg_size_pretty(s.max_replay_lag_bytes)        AS max_replay_lag,
    pg_size_pretty(s.max_write_lag_bytes)         AS max_write_lag,
    pg_size_pretty(s.max_flush_lag_bytes)         AS max_flush_lag,
    s.slot_count,
    s.inactive_slots,
    s.lost_slots,
    s.reserved_slots,
    s.extended_slots,
    pg_size_pretty(s.max_retained_wal_bytes)      AS max_retained_wal,
    s.archiver_has_recent_errors,
    s.replication_lag_risk,
    s.slot_retention_risk
FROM summary s

UNION ALL

SELECT
    'replica'::text                               AS section,
    wr.application_name                           AS name,
    NULL                                          AS ts,
    pg_size_pretty(wr.replay_lag_bytes)           AS wal_generated_total,
    wr.replay_lag_bytes::bigint                   AS wal_bytes_per_second,
    pg_size_pretty(wr.write_lag_bytes)            AS wal_rate_human,
    NULL::bigint                                  AS archived_count,
    NULL::bigint                                  AS failed_count,
    NULL::text                                    AS last_archived_wal,
    NULL::timestamp                               AS last_archived_time,
    NULL::text                                    AS last_failed_wal,
    NULL::timestamp                               AS last_failed_time,
    NULL::bigint                                  AS replica_count,
    NULL::bigint                                  AS non_streaming_replicas,
    NULL::bigint                                  AS sync_replicas,
    NULL::bigint                                  AS async_replicas,
    pg_size_pretty(wr.replay_lag_bytes)           AS max_replay_lag,
    pg_size_pretty(wr.write_lag_bytes)            AS max_write_lag,
    pg_size_pretty(wr.flush_lag_bytes)            AS max_flush_lag,
    NULL::bigint                                  AS slot_count,
    NULL::bigint                                  AS inactive_slots,
    NULL::bigint                                  AS lost_slots,
    NULL::bigint                                  AS reserved_slots,
    NULL::bigint                                  AS extended_slots,
    NULL::text                                    AS max_retained_wal,
    NULL::boolean                                 AS archiver_has_recent_errors,
    NULL::boolean                                 AS replication_lag_risk,
    NULL::boolean                                 AS slot_retention_risk
FROM wal_replication wr

UNION ALL

SELECT
    'slot'::text                                  AS section,
    ws.slot_name                                  AS name,
    NULL                                          AS ts,
    pg_size_pretty(ws.retained_wal_bytes)         AS wal_generated_total,
    ws.retained_wal_bytes::bigint                 AS wal_bytes_per_second,
    pg_size_pretty(ws.retained_wal_bytes)         AS wal_rate_human,
    NULL::bigint                                  AS archived_count,
    NULL::bigint                                  AS failed_count,
    NULL::text                                    AS last_archived_wal,
    NULL::timestamp                               AS last_archived_time,
    NULL::text                                    AS last_failed_wal,
    NULL::timestamp                               AS last_failed_time,
    NULL::bigint                                  AS replica_count,
    NULL::bigint                                  AS non_streaming_replicas,
    NULL::bigint                                  AS sync_replicas,
    NULL::bigint                                  AS async_replicas,
    NULL::text                                    AS max_replay_lag,
    NULL::text                                    AS max_write_lag,
    NULL::text                                    AS max_flush_lag,
    NULL::bigint                                  AS slot_count,
    NULL::bigint                                  AS inactive_slots,
    NULL::bigint                                  AS lost_slots,
    NULL::bigint                                  AS reserved_slots,
    NULL::bigint                                  AS extended_slots,
    NULL::text                                    AS max_retained_wal,
    NULL::boolean                                 AS archiver_has_recent_errors,
    NULL::boolean                                 AS replication_lag_risk,
    NULL::boolean                                 AS slot_retention_risk
FROM wal_slots ws
ORDER BY
    section,
    name NULLS FIRST;
