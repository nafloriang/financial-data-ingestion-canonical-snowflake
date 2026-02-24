/* 07_ops_views.sql
   Operational and observability views.
*/
USE DATABASE FIN_INGEST;

CREATE OR REPLACE VIEW FIN_INGEST.OPS.VW_LOAD_AUDIT_SUMMARY AS
SELECT file_type, load_status,
    COUNT(*) AS batch_count,
    SUM(rows_parsed) AS total_rows_parsed,
    SUM(rows_loaded) AS total_rows_loaded,
    SUM(errors_seen) AS total_errors_seen,
    MAX(load_ts) AS latest_load_ts
FROM FIN_INGEST.RAW.RAW_LOAD_AUDIT
GROUP BY file_type, load_status;

CREATE OR REPLACE VIEW FIN_INGEST.OPS.VW_CANON_COUNTS AS
SELECT client_id, source_system,
    COUNT(*) AS txn_count,
    SUM(IFF(is_valid, 1, 0)) AS valid_txn_count,
    SUM(IFF(NOT is_valid, 1, 0)) AS invalid_txn_count
FROM FIN_INGEST.CANON.CAN_TXN
GROUP BY client_id, source_system;

CREATE OR REPLACE VIEW FIN_INGEST.OPS.VW_ANOMALY_COUNTS AS
SELECT a.client_id, a.source_system, a.anomaly_code, COUNT(*) AS anomaly_count
FROM FIN_INGEST.CANON.CAN_TXN_ANOMALY a
GROUP BY a.client_id, a.source_system, a.anomaly_code;
