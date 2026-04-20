/*
  query_analytics.sql
  PostgreSQL 15.5

  Comprehensive query diagnostics based on pg_stat_statements:
    - execution profile
    - latency profile
    - shared/local/temp block activity
    - cache efficiency
    - planning overhead
    - query classification and recommendations
*/

WITH query_base AS (
    SELECT
        now() AS ts,
        s.userid,
        s.dbid,
        s.queryid,
        s.toplevel,
        s.query,
        s.plans,
        s.total_plan_time,
        s.min_plan_time,
        s.max_plan_time,
        s.mean_plan_time,
        s.stddev_plan_time,
        s.calls,
        s.total_exec_time,
        s.min_exec_time,
        s.max_exec_time,
        s.mean_exec_time,
        s.stddev_exec_time,
        s.rows,
        s.shared_blks_hit,
        s.shared_blks_read,
        s.shared_blks_dirtied,
        s.shared_blks_written,
        s.local_blks_hit,
        s.local_blks_read,
        s.local_blks_dirtied,
        s.local_blks_written,
        s.temp_blks_read,
        s.temp_blks_written,
        s.blk_read_time,
        s.blk_write_time,
        r.rolname AS user_name,
        d.datname AS database_name
    FROM pg_stat_statements s
             LEFT JOIN pg_roles r
                       ON r.oid = s.userid
             LEFT JOIN pg_database d
                       ON d.oid = s.dbid
    WHERE d.datname IS NOT NULL
      AND d.datname NOT IN ('template0', 'template1')
      AND s.query NOT ILIKE '%pg_stat_statements%'
    ),

    query_enriched AS (
SELECT
    qb.ts,
    qb.user_name,
    qb.database_name,
    qb.userid,
    qb.dbid,
    qb.queryid,
    qb.toplevel,
    qb.query,
    left(regexp_replace(qb.query, '\s+', ' ', 'g'), 500) AS normalized_query_preview,
    qb.plans,
    qb.total_plan_time,
    qb.min_plan_time,
    qb.max_plan_time,
    qb.mean_plan_time,
    qb.stddev_plan_time,
    qb.calls,
    qb.total_exec_time,
    qb.min_exec_time,
    qb.max_exec_time,
    qb.mean_exec_time,
    qb.stddev_exec_time,
    qb.rows,
    qb.shared_blks_hit,
    qb.shared_blks_read,
    qb.shared_blks_dirtied,
    qb.shared_blks_written,
    qb.local_blks_hit,
    qb.local_blks_read,
    qb.local_blks_dirtied,
    qb.local_blks_written,
    qb.temp_blks_read,
    qb.temp_blks_written,
    qb.blk_read_time,
    qb.blk_write_time,

    (qb.shared_blks_hit + qb.shared_blks_read) AS total_shared_block_access,
    (qb.local_blks_hit + qb.local_blks_read) AS total_local_block_access,
    (qb.temp_blks_read + qb.temp_blks_written) AS total_temp_block_activity,

    CASE
    WHEN (qb.shared_blks_hit + qb.shared_blks_read) > 0
    THEN round(
    (
    100.0 * qb.shared_blks_hit
    / (qb.shared_blks_hit + qb.shared_blks_read)
    )::numeric,
    2
    )
    ELSE NULL
    END AS shared_cache_hit_ratio_percent,

    CASE
    WHEN qb.calls > 0
    THEN round((qb.total_exec_time / qb.calls)::numeric, 4)
    ELSE NULL
    END AS avg_exec_time_ms,

    CASE
    WHEN qb.calls > 0
    THEN round((qb.total_plan_time / qb.calls)::numeric, 4)
    ELSE NULL
    END AS avg_plan_time_ms,

    CASE
    WHEN qb.calls > 0
    THEN round((qb.rows::numeric / qb.calls), 4)
    ELSE NULL
    END AS avg_rows_per_call,

    CASE
    WHEN qb.calls > 0
    THEN round((qb.shared_blks_read::numeric / qb.calls), 4)
    ELSE NULL
    END AS avg_shared_reads_per_call,

    CASE
    WHEN qb.calls > 0
    THEN round((qb.temp_blks_written::numeric / qb.calls), 4)
    ELSE NULL
    END AS avg_temp_written_per_call,

    CASE
    WHEN qb.total_exec_time > 0
    THEN round((100.0 * qb.total_plan_time / qb.total_exec_time)::numeric, 2)
    ELSE NULL
    END AS planning_to_execution_percent,

    CASE
    WHEN qb.total_exec_time > 0
    THEN round((100.0 * qb.blk_read_time / qb.total_exec_time)::numeric, 2)
    ELSE NULL
    END AS read_io_time_percent,

    CASE
    WHEN qb.total_exec_time > 0
    THEN round((100.0 * qb.blk_write_time / qb.total_exec_time)::numeric, 2)
    ELSE NULL
    END AS write_io_time_percent,

    CASE
    WHEN qb.query ~* '^\s*select'
    THEN 'SELECT'
    WHEN qb.query ~* '^\s*insert'
    THEN 'INSERT'
    WHEN qb.query ~* '^\s*update'
    THEN 'UPDATE'
    WHEN qb.query ~* '^\s*delete'
    THEN 'DELETE'
    WHEN qb.query ~* '^\s*vacuum'
    THEN 'VACUUM'
    WHEN qb.query ~* '^\s*analyze'
    THEN 'ANALYZE'
    WHEN qb.query ~* '^\s*create'
    THEN 'CREATE'
    WHEN qb.query ~* '^\s*alter'
    THEN 'ALTER'
    WHEN qb.query ~* '^\s*drop'
    THEN 'DROP'
    ELSE 'OTHER'
    END AS query_type
FROM query_base qb
    ),

    query_ranked AS (
SELECT
    qe.*,
    rank() OVER (ORDER BY qe.total_exec_time DESC NULLS LAST) AS rank_total_exec_time,
    rank() OVER (ORDER BY qe.mean_exec_time DESC NULLS LAST) AS rank_mean_exec_time,
    rank() OVER (ORDER BY qe.calls DESC NULLS LAST) AS rank_calls,
    rank() OVER (ORDER BY qe.shared_blks_read DESC NULLS LAST) AS rank_shared_reads,
    rank() OVER (ORDER BY qe.temp_blks_written DESC NULLS LAST) AS rank_temp_written
FROM query_enriched qe
    ),

    query_diagnostics AS (
SELECT
    qr.*,
    CASE
    WHEN qr.calls >= 1000 AND qr.mean_exec_time >= 50
    THEN 'FREQUENT_AND_SLOW'
    WHEN qr.mean_exec_time >= 500
    THEN 'SLOW_QUERY'
    WHEN qr.shared_blks_read >= 100000
    THEN 'HEAVY_SHARED_READ'
    WHEN qr.temp_blks_written >= 10000
    THEN 'TEMP_SPILL_RISK'
    WHEN qr.shared_cache_hit_ratio_percent IS NOT NULL
    AND qr.shared_cache_hit_ratio_percent < 90
    AND qr.shared_blks_read > 1000
    THEN 'LOW_CACHE_EFFICIENCY'
    WHEN qr.planning_to_execution_percent IS NOT NULL
    AND qr.planning_to_execution_percent > 30
    AND qr.calls > 100
    THEN 'PLANNING_OVERHEAD'
    ELSE 'NORMAL'
    END AS diagnostic_flag,

    CASE
    WHEN qr.calls >= 1000 AND qr.mean_exec_time >= 50
    THEN 'High-frequency and high-latency statement. Review execution plan and indexing strategy.'
    WHEN qr.mean_exec_time >= 500
    THEN 'High average latency. Investigate execution plan, join strategy, filters, and indexes.'
    WHEN qr.shared_blks_read >= 100000
    THEN 'Large shared block read volume. Check table access pattern and index coverage.'
    WHEN qr.temp_blks_written >= 10000
    THEN 'Significant temporary file usage. Review work_mem, sort/hash operations, and execution plan.'
    WHEN qr.shared_cache_hit_ratio_percent IS NOT NULL
    AND qr.shared_cache_hit_ratio_percent < 90
    AND qr.shared_blks_read > 1000
    THEN 'Low cache hit ratio. Investigate access locality, table size, and memory pressure.'
    WHEN qr.planning_to_execution_percent IS NOT NULL
    AND qr.planning_to_execution_percent > 30
    AND qr.calls > 100
    THEN 'Planning time is a significant share of execution time. Review plan reuse and statement normalization.'
    ELSE 'No immediate performance risk detected.'
    END AS recommendation
FROM query_ranked qr
    ),

    query_summary AS (
SELECT
    COUNT(*) AS query_count,
    COUNT(*) FILTER (WHERE diagnostic_flag <> 'NORMAL') AS flagged_query_count,
    SUM(calls) AS total_calls,
    round(SUM(total_exec_time)::numeric, 2) AS total_exec_time_ms,
    round(AVG(mean_exec_time)::numeric, 4) AS avg_mean_exec_time_ms,
    SUM(shared_blks_read) AS total_shared_blks_read,
    SUM(shared_blks_hit) AS total_shared_blks_hit,
    SUM(temp_blks_written) AS total_temp_blks_written,
    MAX(mean_exec_time) AS max_mean_exec_time,
    MAX(total_exec_time) AS max_total_exec_time
FROM query_diagnostics
    ),

    top_queries AS (
SELECT *
FROM query_diagnostics
ORDER BY total_exec_time DESC NULLS LAST, calls DESC NULLS LAST
    LIMIT 100
    )

SELECT
    'summary'::text AS section,
    NULL::text AS database_name,
    NULL::text AS user_name,
    NULL::bigint AS queryid,
    NULL::text AS query_type,
    NULL::text AS diagnostic_flag,
    NULL::text AS normalized_query_preview,
    qs.query_count,
    qs.flagged_query_count,
    qs.total_calls,
    qs.total_exec_time_ms,
    qs.avg_mean_exec_time_ms,
    qs.total_shared_blks_read,
    qs.total_shared_blks_hit,
    qs.total_temp_blks_written,
    qs.max_mean_exec_time,
    qs.max_total_exec_time,
    NULL::bigint AS calls,
    NULL::numeric AS total_exec_time,
    NULL::numeric AS mean_exec_time,
    NULL::numeric AS avg_rows_per_call,
    NULL::bigint AS shared_blks_read,
    NULL::bigint AS shared_blks_hit,
    NULL::numeric AS shared_cache_hit_ratio_percent,
    NULL::bigint AS temp_blks_written,
    NULL::numeric AS planning_to_execution_percent,
    NULL::text AS recommendation
FROM query_summary qs

UNION ALL

SELECT
    'query'::text AS section,
    tq.database_name,
    tq.user_name,
    tq.queryid,
    tq.query_type,
    tq.diagnostic_flag,
    tq.normalized_query_preview,
    NULL::bigint AS query_count,
    NULL::bigint AS flagged_query_count,
    NULL::bigint AS total_calls,
    NULL::numeric AS total_exec_time_ms,
    NULL::numeric AS avg_mean_exec_time_ms,
    NULL::bigint AS total_shared_blks_read,
    NULL::bigint AS total_shared_blks_hit,
    NULL::bigint AS total_temp_blks_written,
    NULL::double precision AS max_mean_exec_time,
    NULL::double precision AS max_total_exec_time,
    tq.calls,
    round(tq.total_exec_time::numeric, 2) AS total_exec_time,
    round(tq.mean_exec_time::numeric, 4) AS mean_exec_time,
    tq.avg_rows_per_call,
    tq.shared_blks_read,
    tq.shared_blks_hit,
    tq.shared_cache_hit_ratio_percent,
    tq.temp_blks_written,
    tq.planning_to_execution_percent,
    tq.recommendation
FROM top_queries tq
ORDER BY
    section,
    total_exec_time DESC NULLS LAST,
    calls DESC NULLS LAST;
