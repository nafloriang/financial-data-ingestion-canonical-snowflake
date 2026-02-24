/* 06_anomaly_detection.sql
   Detect and persist anomaly records for both headers and lines.
*/
USE DATABASE FIN_INGEST;

CREATE OR REPLACE TEMP TABLE STG_ANOMALY AS
WITH hdr_flags AS (
    SELECT h.canonical_txn_id, h.client_id, h.source_system,
           f.value::STRING AS anomaly_code,
           CAST(NULL AS NUMBER) AS line_number,
           h.src_file,
           'Header-level anomaly from canonical header validation' AS anomaly_detail
    FROM STG_CAN_TXN_HEADER h,
         LATERAL FLATTEN(INPUT => h.anomaly_codes) f
    WHERE h.rn = 1
),
line_flags AS (
    SELECT l.canonical_txn_id, t.client_id, t.source_system,
           IFF(l.quantity < 0, 'NEGATIVE_QTY', 'NEGATIVE_AMOUNT_LINE') AS anomaly_code,
           l.line_number, l.src_file,
           'Line-level negative value detected' AS anomaly_detail
    FROM STG_CAN_TXN_LINE l
    JOIN FIN_INGEST.CANON.CAN_TXN t ON t.canonical_txn_id = l.canonical_txn_id
    WHERE l.quantity < 0 OR l.line_amount < 0
)
SELECT * FROM hdr_flags
UNION ALL
SELECT * FROM line_flags;

MERGE INTO FIN_INGEST.CANON.CAN_TXN_ANOMALY a
USING (
    SELECT DISTINCT canonical_txn_id, client_id, source_system, anomaly_code, anomaly_detail, line_number, src_file
    FROM STG_ANOMALY
) s
ON a.canonical_txn_id = s.canonical_txn_id
AND a.anomaly_code = s.anomaly_code
AND COALESCE(a.line_number, -1) = COALESCE(s.line_number, -1)
AND a.anomaly_detail = s.anomaly_detail
WHEN MATCHED THEN UPDATE SET
    a.client_id = s.client_id,
    a.source_system = s.source_system,
    a.src_file = s.src_file,
    a.detected_ts = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    canonical_txn_id, client_id, source_system, anomaly_code,
    anomaly_detail, line_number, src_file, detected_ts
) VALUES (
    s.canonical_txn_id, s.client_id, s.source_system, s.anomaly_code,
    s.anomaly_detail, s.line_number, s.src_file, CURRENT_TIMESTAMP()
);
