/*
Financial Data Ingestion & Canonical Modeling - Modular SQL App

Entry-point orchestration script.
Choose one execution mode:
1) SnowSQL CLI: uncomment !source lines and run this file.
2) Snowflake Worksheet: execute scripts in the listed order manually.
*/

-- Execution order
-- 00_bootstrap.sql
-- 01_raw_ingestion.sql
-- 02_canonical_ddl.sql
-- 03_transform_headers.sql
-- 04_transform_lines.sql
-- 05_merge_canonical.sql
-- 06_anomaly_detection.sql
-- 07_ops_views.sql
-- 08_smoke_tests.sql

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
