![PostgreSQL](https://img.shields.io/badge/PostgreSQL-336791?style=flat&logo=postgresql&logoColor=white)
![SQL](https://img.shields.io/badge/Language-SQL-blue)
![Scope](https://img.shields.io/badge/Scope-schema-success)
![Focus](https://img.shields.io/badge/Focus-tables%20%26%20indexes-orange)
![Diagnostics](https://img.shields.io/badge/Diagnostics-bloat%20%7C%20locks%20%7C%20stats-critical)

![Repo Size](https://img.shields.io/github/repo-size/VioletSoul/postgres-sql-analytics)
![Code Size](https://img.shields.io/github/languages/code-size/VioletSoul/postgres-sql-analytics)
[![Stars](https://img.shields.io/github/stars/VioletSoul/postgres-sql-analytics.svg?style=social)](https://github.com/VioletSoul/dspc-diagnostics)
[![Last Commit](https://img.shields.io/github/last-commit/VioletSoul/postgres-sql-analytics.svg)](https://github.com/VioletSoul/dspc-diagnostics/commits/main)

## Обзор

Запрос предназначен для получения расширенной статистики и диагностики по таблицам и индексам схемы `dspc` в PostgreSQL.  
Он построен как серия CTE‑подзапросов, которые агрегируют статистические, технические и эксплуатационные метрики для дальнейшего анализа, оптимизации и контроля состояния базы данных.

---

## Структура запроса

Запрос основан на четырёх согласованных общих табличных выражениях (CTE):

- `table_stats` – статистика по таблицам
- `index_stats` – статистика по индексам
- `bloat_stats` – оценка «раздувания» таблиц (bloat)
- `lock_stats` – текущее состояние блокировок

Финальный `SELECT` объединяет эти наборы данных в единый результат.

---

## `table_stats`

Данные берутся из системных каталогов и статистических представлений PostgreSQL:

- `pg_class`, `pg_namespace` – метаданные таблиц и схем
- `pg_stat_all_tables`, `pg_statio_all_tables` – статистика использования таблиц и I/O

Фильтрация ограничена «живыми» реляционными таблицами (тип `r`) в схеме `dspc` (за исключением системных/служебных схем).

Собираются следующие метрики:

- `schema_name` – имя схемы
- `table_name` – имя таблицы
- `table_oid` – OID объекта
- `total_bytes` – общий размер таблицы, включая индексы и TOAST
- `table_bytes` – размер только heap‑структуры таблицы
- `approx_row_count` – примерное количество строк (оценка PostgreSQL)
- `table_pages` – количество страниц таблицы
- `seq_scans` – количество последовательных (полных) сканирований
- `seq_tuples_read` – количество кортежей, прочитанных при последовательных сканах
- `index_scans` – количество сканов по индексам
- `index_tuples_fetched` – количество кортежей, выбранных через индексы
- `n_tup_ins`, `n_tup_upd`, `n_tup_del` – количество вставок, обновлений, удалений
- `n_live_tup`, `n_dead_tup` – количество «живых» и «мёртвых» кортежей
- `n_tup_hot_upd` – количество HOT‑обновлений
- `n_mod_since_analyze` – число изменений с момента последнего `ANALYZE`
- `last_vacuum`, `last_autovacuum`, `last_analyze`, `last_autoanalyze` – время последнего обслуживания
- `vacuum_count`, `autovacuum_count`, `analyze_count`, `autoanalyze_count` – счётчики сервисных операций
- `heap_blocks_read` / `heap_blocks_hit` – I/O активности heap (прочитано vs попадания в кеш)
- `idx_blocks_read` / `idx_blocks_hit` – I/O активности индексов
- `toast_blocks_read` / `toast_blocks_hit` – I/O активности TOAST

---

## `index_stats`

Формируется на основе `table_stats`, `pg_index`, `pg_class`, `pg_stat_all_indexes`, `pg_statio_all_indexes`.

Анализируемые поля:

- `index_name` – имя индекса
- `index_oid` – OID индекса
- `is_unique` / `is_primary` – уникальность и признак PRIMARY KEY
- `is_valid` / `is_ready` – валидность и готовность индекса
- `index_bytes` – размер индекса
- `index_scans` – общее количество сканов по индексу
- `index_tuples_read` / `index_tuples_fetched` – кортежи, прочитанные/выбранные по индексу
- `idx_blocks_read` / `idx_blocks_hit` – I/O‑статистика индекса
- `usage_status` – статус использования индекса (`USED`, если были сканы, иначе `UNUSED`)

---

## `bloat_stats`

Оценивает «раздувание» таблиц по числу мёртвых кортежей:

- `estimated_bloat_percent` – доля мёртвых кортежей от общего числа
- `estimated_bloat_bytes` – соответствующий объём в байтах

Эти метрики служат эвристическим индикатором необходимости `VACUUM`.

---

## `lock_stats`

Агрегирует информацию о блокировках из `pg_locks` для таблиц целевого набора:

- `table_name` – имя затронутой таблицы
- `active_locks` – количество активных блокировок
- `lock_modes` – список типов блокировок (`RowExclusiveLock` и др.)

---

## Финальный `SELECT`

Финальный запрос объединяет все метрики и формирует по одной строке на каждую пару «таблица–индекс».

Для каждой таблицы отображаются:

- `schema_name`, `table_name` – идентификатор объекта
- Размеры в человекочитаемом формате и количество страниц
- `table_trow_count` – оценка количества строк
- `seq_scans`, `seq_tuples_read` – статистика полных сканов
- `table_index_scans`, `table_index_fetches` – статистика сканов по индексам
- `inserts`, `updates`, `deletes`, `hot_updates` – активность DML
- `live_tuples`, `dead_tuples`
- `dead_tuple_ratio_percent` – отношение мёртвых к живым кортежам
- `changes_since_analyze` – количество изменений с момента последнего `ANALYZE`
- `last_vacuum` / `last_autovacuum` / `last_analyze` / `last_autoanalyze`
- `vacuum_count`, `autovacuum_count`, `analyze_count`, `autoanalyze_count`
- `heap_blocks_read` / `heap_blocks_hit` – I/O‑активность таблицы
- `heap_cache_hit_ratio_percent` – процент попаданий в кеш для таблицы
- `estimated_bloat_size`, `estimated_bloat_percent` – оценка bloat
- `active_locks_count`, `active_lock_modes` – состояние блокировок

Для каждого индекса отображаются:

- `index_name`
- `index_type` (`PRIMARY` / `UNIQUE` / `REGULAR`)
- `index_status` (`READY` / `INVALID`)
- `index_size`
- `idx_scans`, `idx_tuples_read`, `idx_tuples_fetched`, `index_usage_status`
- `idx_blocks_read` / `idx_blocks_hit`, `index_cache_hit_ratio_percent`
- `index_selectivity_ratio` – селективность индекса (fetched/read)
- `index_size_percent` – доля размера индекса в общем размере таблицы

Дополнительно включён:

- `recommendation` – автоматизированная рекомендация для DBA на основе выявленных паттернов (создать индекс, удалить неиспользуемый, выполнить `VACUUM`/`ANALYZE` и т.п.).

---

## Логика диагностики

Столбец рекомендаций формируется по набору простых правил:

- Если последовательных сканов значительно больше, чем индексных → `CONSIDER_INDEX`
- Если индекс крупный, но не используется → `UNUSED_INDEX`
- Если bloat превышает 20% → `NEEDS_VACUUM`
- Если число изменений превышает 10% от живых строк → `NEEDS_ANALYZE`
- Если проблем не обнаружено → `OK`

---

## Сортировка

Результат сортируется по:

1. Общему размеру таблицы (по убыванию)
2. Имени таблицы
3. Размеру индекса (по убыванию)

---

## Назначение

Этот запрос даёт комплексный аудит состояния таблиц, индексов и блокировок для схемы `dspc`.  
Его можно использовать как технический отчёт для систематического анализа:

- качества обслуживания (`VACUUM` / `ANALYZE`),
- характеристик производительности (паттерны сканирования, hit‑ratio кеша),
- структуры хранения и раздувания таблиц,
- полезности и селективности индексов.
