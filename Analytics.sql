WITH table_stats AS (
    SELECT
        n.nspname AS schema_name,
        c.relname AS table_name,
        c.oid AS table_oid,
        pg_total_relation_size(c.oid)::numeric AS total_bytes,
        pg_relation_size(c.oid)::numeric AS table_bytes,
        c.reltuples::numeric AS approx_row_count,
        c.relpages::numeric AS table_pages,
        COALESCE(st.seq_scan, 0)::numeric AS seq_scans,
        COALESCE(st.seq_tup_read, 0)::numeric AS seq_tuples_read,
        COALESCE(st.idx_scan, 0)::numeric AS index_scans,
        COALESCE(st.idx_tup_fetch, 0)::numeric AS index_tuples_fetched,
        COALESCE(st.n_tup_ins, 0)::numeric AS inserts,
        COALESCE(st.n_tup_upd, 0)::numeric AS updates,
        COALESCE(st.n_tup_del, 0)::numeric AS deletes,
        COALESCE(st.n_live_tup, 0)::numeric AS live_tuples,
        COALESCE(st.n_dead_tup, 0)::numeric AS dead_tuples,
        COALESCE(st.n_tup_hot_upd, 0)::numeric AS hot_updates,
        COALESCE(st.n_mod_since_analyze, 0)::numeric AS changes_since_analyze,
        st.last_vacuum,
        st.last_autovacuum,
        st.last_analyze,
        st.last_autoanalyze,
        st.vacuum_count,
        st.autovacuum_count,
        st.analyze_count,
        st.autoanalyze_count,
        COALESCE(sio.heap_blks_read, 0)::numeric AS heap_blocks_read,
        COALESCE(sio.heap_blks_hit, 0)::numeric AS heap_blocks_hit,
        COALESCE(sio.idx_blks_read, 0)::numeric AS idx_blocks_read,
        COALESCE(sio.idx_blks_hit, 0)::numeric AS idx_blocks_hit,
        COALESCE(sio.toast_blks_read, 0)::numeric AS toast_blocks_read,
        COALESCE(sio.toast_blks_hit, 0)::numeric AS toast_blocks_hit
    FROM pg_class c
             JOIN pg_namespace n ON n.oid = c.relnamespace
             LEFT JOIN pg_stat_all_tables st ON st.relid = c.oid
             LEFT JOIN pg_statio_all_tables sio ON sio.relid = c.oid
    WHERE c.relkind = 'r'
      AND n.nspname NOT IN ('pg_catalog', 'information_schema')
      AND n.nspname !~ '^pg_toast'
    AND n.nspname = 'dspc'
    ),

    index_stats AS (
SELECT
    t.schema_name,
    t.table_name,
    t.table_oid,
    i.relname AS index_name,
    i.oid AS index_oid,
    ix.indisunique AS is_unique,
    ix.indisprimary AS is_primary,
    ix.indisvalid AS is_valid,
    ix.indisready AS is_ready,
    pg_relation_size(i.oid)::numeric AS index_bytes,
    COALESCE(si.idx_scan, 0)::numeric AS index_scans,
    COALESCE(si.idx_tup_read, 0)::numeric AS index_tuples_read,
    COALESCE(si.idx_tup_fetch, 0)::numeric AS index_tuples_fetched,
    COALESCE(siio.idx_blks_read, 0)::numeric AS idx_blocks_read,
    COALESCE(siio.idx_blks_hit, 0)::numeric AS idx_blocks_hit,
    CASE WHEN COALESCE(si.idx_scan, 0)::numeric = 0 THEN 'UNUSED' ELSE 'USED' END AS usage_status
FROM table_stats t
    LEFT JOIN pg_index ix ON ix.indrelid = t.table_oid
    LEFT JOIN pg_class i ON i.oid = ix.indexrelid
    LEFT JOIN pg_stat_all_indexes si ON si.indexrelid = i.oid
    LEFT JOIN pg_statio_all_indexes siio ON siio.indexrelid = i.oid
    ),

    bloat_stats AS (
SELECT
    n.nspname AS schema_name,
    c.relname AS table_name,
    c.oid AS table_oid,
    CASE
    WHEN COALESCE(st.n_live_tup, 0)::numeric + COALESCE(st.n_dead_tup, 0)::numeric > 0
    THEN ROUND((COALESCE(st.n_dead_tup, 0)::numeric /
    (COALESCE(st.n_live_tup, 0)::numeric + COALESCE(st.n_dead_tup, 0)::numeric)) * 100, 2)
    ELSE 0
    END AS estimated_bloat_percent,
    CASE
    WHEN COALESCE(st.n_live_tup, 0)::numeric + COALESCE(st.n_dead_tup, 0)::numeric > 0
    THEN ROUND(
    (pg_relation_size(c.oid)::numeric * COALESCE(st.n_dead_tup, 0)::numeric) /
    (COALESCE(st.n_live_tup, 0)::numeric + COALESCE(st.n_dead_tup, 0)::numeric)
    )
    ELSE 0
    END AS estimated_bloat_bytes
FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN pg_stat_all_tables st ON st.relid = c.oid
WHERE c.relkind = 'r'
  AND n.nspname = 'dspc'
    ),

    lock_stats AS (
SELECT
    relation::regclass AS table_name,
    COUNT(*)::numeric AS active_locks,
    array_agg(DISTINCT mode) AS lock_modes
FROM pg_locks
WHERE relation IS NOT NULL
  AND relation IN (SELECT table_oid FROM table_stats)
GROUP BY relation
    )

SELECT
    ts.schema_name,
    ts.table_name,
    pg_size_pretty(ts.total_bytes) AS table_total_size,
    pg_size_pretty(ts.table_bytes) AS table_data_size,
    ts.table_pages AS table_pages_count,
    ts.approx_row_count,
    ts.seq_scans,
    ts.seq_tuples_read,
    ts.index_scans AS table_index_scans,
    ts.index_tuples_fetched AS table_index_fetches,
    ts.inserts,
    ts.updates,
    ts.deletes,
    ts.hot_updates,
    ts.live_tuples,
    ts.dead_tuples,
    CASE
        WHEN ts.live_tuples > 0
            THEN ROUND((ts.dead_tuples / ts.live_tuples) * 100, 2)
        ELSE 0
        END AS dead_tuple_ratio_percent,
    ts.changes_since_analyze,
    ts.last_vacuum,
    ts.last_autovacuum,
    ts.last_analyze,
    ts.last_autoanalyze,
    ts.vacuum_count,
    ts.autovacuum_count,
    ts.analyze_count,
    ts.autoanalyze_count,
    ts.heap_blocks_read,
    ts.heap_blocks_hit,
    CASE
        WHEN (ts.heap_blocks_read + ts.heap_blocks_hit) > 0
            THEN ROUND((ts.heap_blocks_hit / (ts.heap_blocks_read + ts.heap_blocks_hit)) * 100, 2)
        ELSE NULL
        END AS heap_cache_hit_ratio_percent,
    pg_size_pretty(COALESCE(bs.estimated_bloat_bytes, 0)) AS estimated_bloat_size,
    COALESCE(bs.estimated_bloat_percent, 0) AS estimated_bloat_percent,
    COALESCE(ls.active_locks, 0) AS active_locks_count,
    COALESCE(array_to_string(ls.lock_modes, ', '), 'None') AS active_lock_modes,
    CASE
        WHEN ts.seq_scans > 0 AND ts.index_scans > 0
            THEN ROUND((ts.index_scans / ts.seq_scans), 2)
        WHEN ts.seq_scans = 0 AND ts.index_scans > 0
            THEN 999.99
        ELSE 0
        END AS index_to_seq_ratio,
    CASE
        WHEN ts.updates > 0
            THEN ROUND((ts.hot_updates / ts.updates) * 100, 2)
        ELSE NULL
        END AS hot_update_ratio_percent,
    ist.index_name,
    CASE
        WHEN ist.is_primary THEN 'PRIMARY'
        WHEN ist.is_unique THEN 'UNIQUE'
        ELSE 'REGULAR'
        END AS index_type,
    CASE
        WHEN ist.is_valid = false THEN 'INVALID'
        WHEN ist.is_ready = false THEN 'NOT_READY'
        ELSE 'READY'
        END AS index_status,
    pg_size_pretty(COALESCE(ist.index_bytes, 0)) AS index_size,
    ist.index_scans AS idx_scans,
    ist.index_tuples_read AS idx_tuples_read,
    ist.index_tuples_fetched AS idx_tuples_fetched,
    ist.usage_status AS index_usage_status,
    ist.idx_blocks_read,
    ist.idx_blocks_hit,
    CASE
        WHEN (ist.idx_blocks_read + ist.idx_blocks_hit) > 0
            THEN ROUND((ist.idx_blocks_hit / (ist.idx_blocks_read + ist.idx_blocks_hit)) * 100, 2)
        ELSE NULL
        END AS index_cache_hit_ratio_percent,
    CASE
        WHEN ist.index_tuples_fetched > 0 AND ist.index_tuples_read > 0
            THEN ROUND((ist.index_tuples_fetched / ist.index_tuples_read), 4)
        ELSE NULL
        END AS index_selectivity_ratio,
    CASE
        WHEN ts.total_bytes > 0
            THEN ROUND((COALESCE(ist.index_bytes, 0) / ts.total_bytes) * 100, 2)
        ELSE 0
        END AS index_size_percent,
    CASE
        WHEN ts.seq_scans > ts.index_scans * 10 AND ts.seq_scans > 1000
            THEN 'CONSIDER_INDEX'
        WHEN ist.index_scans = 0 AND ist.index_bytes > 1048576
            THEN 'UNUSED_INDEX'
        WHEN COALESCE(bs.estimated_bloat_percent, 0) > 20
            THEN 'NEEDS_VACUUM'
        WHEN ts.changes_since_analyze > ts.live_tuples * 0.1 AND ts.live_tuples > 1000
            THEN 'NEEDS_ANALYZE'
        ELSE 'OK'
        END AS recommendation
FROM table_stats ts
         LEFT JOIN index_stats ist ON ist.schema_name = ts.schema_name AND ist.table_name = ts.table_name
         LEFT JOIN bloat_stats bs ON bs.schema_name = ts.schema_name AND bs.table_name = ts.table_name
         LEFT JOIN lock_stats ls ON ls.table_name = (ts.schema_name || '.' || ts.table_name)::regclass
ORDER BY
    ts.total_bytes DESC,
    ts.table_name,
    ist.index_bytes DESC NULLS LAST;