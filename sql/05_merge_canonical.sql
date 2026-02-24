/* 05_merge_canonical.sql
   Upsert normalized header and line staging tables into canonical core tables.
*/
USE DATABASE FIN_INGEST;

MERGE INTO FIN_INGEST.CANON.CAN_TXN t
USING (
    SELECT canonical_txn_id, client_id, source_system, source_txn_id, txn_timestamp, currency, total_amount,
           customer_id, account_id, merchant, src_file, ingest_ts,
           IFF(ARRAY_SIZE(anomaly_codes) = 0, TRUE, FALSE) AS is_valid,
           anomaly_codes, attributes
    FROM STG_CAN_TXN_HEADER
    WHERE rn = 1
) s
ON t.canonical_txn_id = s.canonical_txn_id
WHEN MATCHED THEN UPDATE SET
    t.client_id = s.client_id, t.source_system = s.source_system, t.source_txn_id = s.source_txn_id,
    t.txn_timestamp = s.txn_timestamp, t.currency = s.currency, t.total_amount = s.total_amount,
    t.customer_id = s.customer_id, t.account_id = s.account_id, t.merchant = s.merchant,
    t.src_file = s.src_file, t.ingest_ts = s.ingest_ts, t.is_valid = s.is_valid,
    t.anomaly_codes = s.anomaly_codes, t.attributes = s.attributes, t.updated_ts = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    canonical_txn_id, client_id, source_system, source_txn_id, txn_timestamp,
    currency, total_amount, customer_id, account_id, merchant,
    src_file, ingest_ts, is_valid, anomaly_codes, attributes, created_ts, updated_ts
) VALUES (
    s.canonical_txn_id, s.client_id, s.source_system, s.source_txn_id, s.txn_timestamp,
    s.currency, s.total_amount, s.customer_id, s.account_id, s.merchant,
    s.src_file, s.ingest_ts, s.is_valid, s.anomaly_codes, s.attributes, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

MERGE INTO FIN_INGEST.CANON.CAN_TXN_LINE t
USING (
    SELECT canonical_txn_id, line_number, line_txn_id, item_id, description, quantity,
           unit_price, line_amount, currency, src_file, ingest_ts, attributes
    FROM STG_CAN_TXN_LINE
) s
ON t.canonical_txn_id = s.canonical_txn_id
AND t.line_number = s.line_number
WHEN MATCHED THEN UPDATE SET
    t.line_txn_id = s.line_txn_id, t.item_id = s.item_id, t.description = s.description,
    t.quantity = s.quantity, t.unit_price = s.unit_price, t.line_amount = s.line_amount,
    t.currency = s.currency, t.src_file = s.src_file, t.ingest_ts = s.ingest_ts,
    t.attributes = s.attributes, t.updated_ts = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    canonical_txn_id, line_number, line_txn_id, item_id, description,
    quantity, unit_price, line_amount, currency,
    src_file, ingest_ts, attributes, created_ts, updated_ts
) VALUES (
    s.canonical_txn_id, s.line_number, s.line_txn_id, s.item_id, s.description,
    s.quantity, s.unit_price, s.line_amount, s.currency,
    s.src_file, s.ingest_ts, s.attributes, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);
