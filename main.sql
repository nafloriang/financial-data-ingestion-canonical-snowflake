/*
Financial Data Ingestion & Canonical Modeling - Modular SQL App (Snowsight + SnowSQL)

This file is an executable runbook for Snowsight and a launcher guide for SnowSQL.
It intentionally keeps the modular architecture (one concern per module).

SNOWSIGHT USAGE:
- Open each module in order and run it.
- This file itself is safe to run in Snowsight (no SnowSQL-only commands are executed).

SNOWSQL USAGE:
- Uncomment the !source statements at the end of this file.
*/

-- Required execution order (modular pipeline):
-- 1) sql/00_bootstrap.sql
-- 2) sql/01_raw_ingestion.sql
-- 3) sql/02_canonical_ddl.sql
-- 4) sql/03_transform_headers.sql
-- 5) sql/04_transform_lines.sql
-- 6) sql/05_merge_canonical.sql
-- 7) sql/06_anomaly_detection.sql
-- 8) sql/07_ops_views.sql
-- 9) sql/08_smoke_tests.sql

SELECT 'Run modules in the documented order. main.sql is intentionally orchestration-only.' AS status;

-- SnowSQL mode (optional):
-- !source sql/00_bootstrap.sql
-- !source sql/01_raw_ingestion.sql
-- !source sql/02_canonical_ddl.sql
-- !source sql/03_transform_headers.sql
-- !source sql/04_transform_lines.sql
-- !source sql/05_merge_canonical.sql
-- !source sql/06_anomaly_detection.sql
-- !source sql/07_ops_views.sql
-- !source sql/08_smoke_tests.sql
