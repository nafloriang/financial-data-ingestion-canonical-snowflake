/* 03_transform_headers.sql
   Normalize raw JSON/XML/CSV into a unified transaction header staging table.

   Dedupe rule requirement:
   ROW_NUMBER() OVER (PARTITION BY client_id, source_txn_id ORDER BY ingest_ts DESC)
*/
USE DATABASE FIN_INGEST;

CREATE OR REPLACE TEMP TABLE STG_CAN_TXN_HEADER AS
WITH
json_hdr AS (
    SELECT r.client_id, 'JSON' AS source_system,
        COALESCE(r.payload:transaction_id::STRING, r.payload:txn_id::STRING, r.payload:id::STRING) AS source_txn_id,
        TRY_TO_TIMESTAMP_NTZ(COALESCE(r.payload:transaction_ts::STRING, r.payload:transaction_time::STRING, r.payload:timestamp::STRING, r.payload:txn_timestamp::STRING)) AS txn_timestamp,
        UPPER(COALESCE(r.payload:currency::STRING, r.payload:ccy::STRING)) AS currency,
        TRY_TO_NUMBER(COALESCE(r.payload:total_amount::STRING, r.payload:amount::STRING, r.payload:total::STRING)) AS total_amount,
        COALESCE(r.payload:customer_id::STRING, r.payload:customer:id::STRING, r.payload:customerId::STRING) AS customer_id,
        COALESCE(r.payload:account_id::STRING, r.payload:account:id::STRING, r.payload:accountId::STRING) AS account_id,
        COALESCE(r.payload:merchant::STRING, r.payload:merchant:name::STRING, r.payload:payee::STRING) AS merchant,
        r.src_file,
        r.ingest_ts,
        OBJECT_CONSTRUCT('raw_payload', r.payload, 'source_format', 'JSON') AS attributes,
        SHA2(TO_JSON(r.payload), 256) AS payload_hash
    FROM FIN_INGEST.RAW.RAW_TXN_JSON r
),
xml_hdr AS (
    SELECT r.client_id, 'XML' AS source_system,
        COALESCE(r.payload:"@transaction_id"::STRING, r.payload:"transaction_id"::STRING, r.payload:"txn_id"::STRING, r.payload:"id"::STRING) AS source_txn_id,
        TRY_TO_TIMESTAMP_NTZ(COALESCE(r.payload:"transaction_ts"::STRING, r.payload:"transaction_time"::STRING, r.payload:"timestamp"::STRING, r.payload:"txn_timestamp"::STRING)) AS txn_timestamp,
        UPPER(COALESCE(r.payload:"currency"::STRING, r.payload:"ccy"::STRING)) AS currency,
        TRY_TO_NUMBER(COALESCE(r.payload:"total_amount"::STRING, r.payload:"amount"::STRING, r.payload:"total"::STRING)) AS total_amount,
        COALESCE(r.payload:"customer_id"::STRING, r.payload:"customer":"id"::STRING) AS customer_id,
        COALESCE(r.payload:"account_id"::STRING, r.payload:"account":"id"::STRING) AS account_id,
        COALESCE(r.payload:"merchant"::STRING, r.payload:"merchant":"name"::STRING, r.payload:"payee"::STRING) AS merchant,
        r.src_file,
        r.ingest_ts,
        OBJECT_CONSTRUCT('raw_payload', r.payload, 'source_format', 'XML') AS attributes,
        SHA2(TO_JSON(r.payload), 256) AS payload_hash
    FROM FIN_INGEST.RAW.RAW_TXN_XML r
),
csv_hdr AS (
    SELECT r.client_id, 'CSV' AS source_system,
        r.payload[0]::STRING AS source_txn_id,
        TRY_TO_TIMESTAMP_NTZ(r.payload[1]::STRING) AS txn_timestamp,
        UPPER(r.payload[2]::STRING) AS currency,
        TRY_TO_NUMBER(r.payload[3]::STRING) AS total_amount,
        r.payload[4]::STRING AS customer_id,
        r.payload[5]::STRING AS account_id,
        r.payload[6]::STRING AS merchant,
        r.src_file,
        r.ingest_ts,
        OBJECT_CONSTRUCT('csv_payload', r.payload, 'source_format', 'CSV') AS attributes,
        SHA2(TO_JSON(r.payload), 256) AS payload_hash
    FROM FIN_INGEST.RAW.RAW_CSV_GENERIC r
),
all_hdr AS (
    SELECT * FROM json_hdr
    UNION ALL
    SELECT * FROM xml_hdr
    UNION ALL
    SELECT * FROM csv_hdr
),
enriched AS (
    SELECT
        *,
        COALESCE(source_txn_id, payload_hash) AS effective_source_txn_id,
        COALESCE(
            IFF(source_txn_id IS NOT NULL,
                SHA2(client_id || '|' || source_txn_id, 256),
                SHA2(client_id || '|' || payload_hash, 256)
            ),
            SHA2(client_id || '|' || src_file || '|' || NVL(TO_VARCHAR(txn_timestamp), ''), 256)
        ) AS canonical_txn_id
    FROM all_hdr
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY client_id, source_txn_id ORDER BY ingest_ts DESC) AS rn,
        COUNT(*) OVER (PARTITION BY client_id, source_txn_id) AS dup_cnt
    FROM enriched
)
SELECT
    canonical_txn_id,
    client_id,
    source_system,
    effective_source_txn_id AS source_txn_id,
    txn_timestamp,
    currency,
    total_amount,
    customer_id,
    account_id,
    merchant,
    src_file,
    ingest_ts,
    rn,
    dup_cnt,
    ARRAY_CONSTRUCT_COMPACT(
        IFF(dup_cnt > 1, 'DUPLICATE_TXN', NULL),
        IFF(txn_timestamp IS NULL OR total_amount IS NULL, 'MISSING_REQUIRED', NULL),
        IFF(total_amount < 0, 'NEGATIVE_AMOUNT', NULL)
    ) AS anomaly_codes,
    attributes
FROM ranked;
