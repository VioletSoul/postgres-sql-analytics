/*
  wal_analytics_v2.sql
  PostgreSQL 15.5

  WAL diagnostics and health assessment:
    - WAL generation metrics
    - Archiver status
    - Archive success ratio
    - Checkpoint statistics
    - Replication lag
    - Replication slots retention
    - Severity levels
    - Health score
    - Recommendations
*/

WITH

    config AS (
        SELECT
            1024::bigint * 1024 * 1024          AS replication_warning_bytes,
            10::bigint * 1024 * 1024 * 1024     AS replication_critical_bytes,

            5::bigint * 1024 * 1024 * 1024      AS slot_warning_bytes,
            20::bigint * 1024 * 1024 * 1024     AS slot_critical_bytes,

            0.95::numeric                       AS archive_warning_ratio,
            0.80::numeric                       AS archive_critical_ratio
    ),

    wal_stat AS (
        SELECT
            now() AS ts,
            stats_reset,
            wal_records,
            wal_fpi,
            wal_bytes,
            wal_buffers_full,
            wal_write,
            wal_sync
        FROM pg_stat_wal
    ),

    wal_rate AS (
        SELECT
            ws.*,
            EXTRACT(
                    EPOCH FROM (ws.ts - ws.stats_reset)
            ) AS seconds_since_reset,

            CASE
                WHEN ws.ts > ws.stats_reset THEN
                    ws.wal_bytes::numeric /
    GREATEST(
    EXTRACT(EPOCH FROM (ws.ts - ws.stats_reset)),
    1
    )
    ELSE NULL
END AS wal_bytes_per_second

    FROM wal_stat ws
),

wal_archiver AS (
    SELECT
        archived_count,
        failed_count,
        last_archived_wal,
        last_archived_time,
        last_failed_wal,
        last_failed_time,
        stats_reset,

        CASE
            WHEN archived_count + failed_count > 0 THEN
                archived_count::numeric /
                (archived_count + failed_count)
            ELSE NULL
        END AS archive_success_ratio

    FROM pg_stat_archiver
),

checkpoint_stats AS (
    SELECT
        checkpoints_timed,
        checkpoints_req,
        checkpoint_write_time,
        checkpoint_sync_time,
        buffers_checkpoint,
        buffers_clean,
        maxwritten_clean,
        buffers_backend,
        buffers_backend_fsync,
        buffers_alloc
    FROM pg_stat_bgwriter
),

replication AS (
    SELECT
        application_name,
        client_addr,
        state,
        sync_state,

        sent_lsn,
        write_lsn,
        flush_lsn,
        replay_lsn,

        pg_wal_lsn_diff(
            pg_current_wal_lsn(),
            sent_lsn
        ) AS sent_lag_bytes,

        pg_wal_lsn_diff(
            pg_current_wal_lsn(),
            write_lsn
        ) AS write_lag_bytes,

        pg_wal_lsn_diff(
            pg_current_wal_lsn(),
            flush_lsn
        ) AS flush_lag_bytes,

        pg_wal_lsn_diff(
            pg_current_wal_lsn(),
            replay_lsn
        ) AS replay_lag_bytes

    FROM pg_stat_replication
),

replication_summary AS (
    SELECT
        COUNT(*) AS replica_count,

        SUM(
            CASE
                WHEN state <> 'streaming'
                THEN 1
                ELSE 0
            END
        ) AS non_streaming_replicas,

        SUM(
            CASE
                WHEN sync_state = 'sync'
                THEN 1
                ELSE 0
            END
        ) AS sync_replicas,

        SUM(
            CASE
                WHEN sync_state = 'async'
                THEN 1
                ELSE 0
            END
        ) AS async_replicas,

        MAX(replay_lag_bytes) AS max_replay_lag_bytes,
        MAX(write_lag_bytes)  AS max_write_lag_bytes,
        MAX(flush_lag_bytes)  AS max_flush_lag_bytes

    FROM replication
),

replication_slots AS (
    SELECT
        slot_name,
        slot_type,
        active,
        wal_status,
        restart_lsn,
        confirmed_flush_lsn,

        pg_wal_lsn_diff(
            pg_current_wal_lsn(),
            restart_lsn
        ) AS retained_wal_bytes

    FROM pg_replication_slots
),

slot_summary AS (
    SELECT
        COUNT(*) AS slot_count,

        SUM(
            CASE
                WHEN NOT active
                THEN 1
                ELSE 0
            END
        ) AS inactive_slots,

        SUM(
            CASE
                WHEN wal_status = 'lost'
                THEN 1
                ELSE 0
            END
        ) AS lost_slots,

        MAX(retained_wal_bytes) AS max_retained_wal_bytes

    FROM replication_slots
),

health_checks AS (

    SELECT
        'replication_lag' AS check_name,

        CASE
            WHEN rs.max_replay_lag_bytes >= c.replication_critical_bytes
                THEN 'CRITICAL'

            WHEN rs.max_replay_lag_bytes >= c.replication_warning_bytes
                THEN 'WARNING'

            ELSE 'OK'
        END AS severity,

        CASE
            WHEN rs.max_replay_lag_bytes >= c.replication_critical_bytes
                THEN 'Replication replay lag is above critical threshold'

            WHEN rs.max_replay_lag_bytes >= c.replication_warning_bytes
                THEN 'Replication replay lag requires attention'

            ELSE 'Replication lag is within acceptable limits'
        END AS message

    FROM replication_summary rs
    CROSS JOIN config c


    UNION ALL


    SELECT
        'slot_retention',

        CASE
            WHEN ss.max_retained_wal_bytes >= c.slot_critical_bytes
                THEN 'CRITICAL'

            WHEN ss.max_retained_wal_bytes >= c.slot_warning_bytes
                THEN 'WARNING'

            ELSE 'OK'
        END,

        CASE
            WHEN ss.max_retained_wal_bytes >= c.slot_critical_bytes
                THEN 'Replication slot retention may cause WAL disk growth'

            WHEN ss.max_retained_wal_bytes >= c.slot_warning_bytes
                THEN 'Replication slot retains significant WAL amount'

            ELSE 'Replication slot retention is within limits'
        END

    FROM slot_summary ss
    CROSS JOIN config c


    UNION ALL


    SELECT
        'archiver',

        CASE
            WHEN wa.archive_success_ratio IS NULL
                THEN 'INFO'

            WHEN wa.archive_success_ratio < c.archive_critical_ratio
                THEN 'CRITICAL'

            WHEN wa.archive_success_ratio < c.archive_warning_ratio
                THEN 'WARNING'

            ELSE 'OK'
        END,

        CASE
            WHEN wa.archive_success_ratio IS NULL
                THEN 'No WAL archive activity detected'

            WHEN wa.archive_success_ratio < c.archive_critical_ratio
                THEN 'WAL archive failures are critical'

            WHEN wa.archive_success_ratio < c.archive_warning_ratio
                THEN 'WAL archive failures detected'

            ELSE 'WAL archiver is healthy'
        END

    FROM wal_archiver wa
    CROSS JOIN config c


    UNION ALL


    SELECT
        'checkpoint_pressure',

        CASE
            WHEN cs.checkpoints_req >
                 cs.checkpoints_timed * 2
                THEN 'WARNING'

            ELSE 'OK'
        END,

        CASE
            WHEN cs.checkpoints_req >
                 cs.checkpoints_timed * 2
                THEN 'Requested checkpoints exceed timed checkpoints'

            ELSE 'Checkpoint activity is normal'
        END

    FROM checkpoint_stats cs
),


health_score AS (

    SELECT
        GREATEST(
            0,
            100 -
            SUM(
                CASE severity
                    WHEN 'CRITICAL'
                        THEN 30

                    WHEN 'WARNING'
                        THEN 10

                    WHEN 'INFO'
                        THEN 5

                    ELSE 0
                END
            )
        ) AS score

    FROM health_checks
),


overall_status AS (

    SELECT
        score,

        CASE
            WHEN score >= 90
                THEN 'HEALTHY'

            WHEN score >= 70
                THEN 'WARNING'

            ELSE 'CRITICAL'

        END AS status

    FROM health_score
),


recommendations AS (

    SELECT
        check_name,

        CASE
            WHEN severity = 'CRITICAL'
                THEN
                    CASE check_name

                        WHEN 'replication_lag'
                        THEN
                            'Check standby availability and network latency. Investigate replay delay.'

                        WHEN 'slot_retention'
                        THEN
                            'Review inactive replication slots. Remove unused slots or restore consumers.'

                        WHEN 'archiver'
                        THEN
                            'Investigate archive_command failures and WAL archive destination.'

                        ELSE
                            'Immediate investigation required.'

                    END


            WHEN severity = 'WARNING'
                THEN
                    CASE check_name

                        WHEN 'replication_lag'
                        THEN
                            'Monitor standby lag and verify replica performance.'

                        WHEN 'slot_retention'
                        THEN
                            'Review WAL retention growth and replication slot usage.'

                        WHEN 'archiver'
                        THEN
                            'Review WAL archive failures.'

                        WHEN 'checkpoint_pressure'
                        THEN
                            'Review max_wal_size and checkpoint configuration.'

                        ELSE
                            'Review system state.'

                    END


            ELSE NULL

        END AS recommendation

    FROM health_checks

    WHERE severity <> 'OK'
),

final_report AS (

    SELECT
        'health'::text AS section,
        'overall_score'::text AS name,
        NULL::text AS ts,
        score::text AS value
    FROM health_score


    UNION ALL


    SELECT
        'health',
        'status',
        NULL,
        status
    FROM overall_status


    UNION ALL


    SELECT
        'check',
        check_name,
        NULL,
        severity || ': ' || message
    FROM health_checks


    UNION ALL


    SELECT
        'recommendation',
        check_name,
        NULL,
        recommendation
    FROM recommendations
    WHERE recommendation IS NOT NULL


    UNION ALL


    SELECT
        'summary',
        'wal_generated_total',
        to_char(w.ts, 'YYYY-MM-DD HH24:MI:SS'),
        pg_size_pretty(w.wal_bytes)
    FROM wal_rate w


    UNION ALL


    SELECT
        'summary',
        'wal_rate',
        NULL,
        pg_size_pretty(w.wal_bytes_per_second::bigint)
    FROM wal_rate w


    UNION ALL


    SELECT
        'summary',
        'archive_success_ratio',
        NULL,
        CASE
            WHEN archive_success_ratio IS NULL
                THEN 'N/A'
            ELSE
                round(
                    archive_success_ratio * 100,
                    2
                )::text || '%'
        END
    FROM wal_archiver


    UNION ALL


    SELECT
        'summary',
        'archived_count',
        NULL,
        archived_count::text
    FROM wal_archiver


    UNION ALL


    SELECT
        'summary',
        'failed_count',
        NULL,
        failed_count::text
    FROM wal_archiver


    UNION ALL


    SELECT
        'summary',
        'replica_count',
        NULL,
        replica_count::text
    FROM replication_summary


    UNION ALL


    SELECT
        'summary',
        'max_replay_lag',
        NULL,
        pg_size_pretty(max_replay_lag_bytes)
    FROM replication_summary


    UNION ALL


    SELECT
        'summary',
        'slot_count',
        NULL,
        slot_count::text
    FROM slot_summary


    UNION ALL


    SELECT
        'summary',
        'inactive_slots',
        NULL,
        inactive_slots::text
    FROM slot_summary


    UNION ALL


    SELECT
        'summary',
        'max_retained_wal',
        NULL,
        pg_size_pretty(max_retained_wal_bytes)
    FROM slot_summary


    UNION ALL


    SELECT
        'replica',
        application_name,
        NULL,
        'state=' || state ||
        ', sync=' || sync_state ||
        ', replay_lag=' ||
        pg_size_pretty(replay_lag_bytes)
    FROM replication


    UNION ALL


    SELECT
        'slot',
        slot_name,
        NULL,
        'active=' || active ||
        ', retained_wal=' ||
        pg_size_pretty(retained_wal_bytes)
    FROM replication_slots

)


SELECT
    section,
    name,
    ts,
    value
FROM final_report

ORDER BY
    CASE section
    WHEN 'health' THEN 1
    WHEN 'check' THEN 2
    WHEN 'recommendation' THEN 3
    WHEN 'summary' THEN 4
    WHEN 'replica' THEN 5
    WHEN 'slot' THEN 6
    ELSE 99
END,
    name;