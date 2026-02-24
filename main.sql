/* ============================================================================
   Financial Data Ingestion & Canonical Modeling (Snowflake SQL only)
   Target DB: FIN_INGEST
   Idempotent: YES (CREATE OR REPLACE + MERGE-based canonical loads)
   ============================================================================ */

/* === 01 | SESSION / CONTEXT ================================================= */
USE DATABASE FIN_INGEST;
USE SCHEMA RAW;

CREATE SCHEMA IF NOT EXISTS FIN_INGEST.RAW;
CREATE SCHEMA IF NOT EXISTS FIN_INGEST.CANON;
CREATE SCHEMA IF NOT EXISTS FIN_INGEST.OPS;

/* === 02 | OPTIONAL RAW INGESTION PATTERNS (STAGE + COPY + AUDIT) ===========
   NOTE:
   - RAW tables are already populated in this environment.
   - Keep these statements as runnable reference blocks for re-ingestion.
   - IMPORTANT: insert into RAW_LOAD_AUDIT immediately after each COPY using
     RESULT_SCAN(LAST_QUERY_ID()) in the same session.
-------------------------------------------------------------------------------
-- JSON CLIENT LOAD PATTERN
COPY INTO FIN_INGEST.RAW.RAW_TXN_JSON (client_id, src_file, src_row_number, payload)
FROM (
    SELECT
        'CLIENT_C'::STRING                                                AS client_id,
        METADATA$FILENAME::STRING                                         AS src_file,
        METADATA$FILE_ROW_NUMBER::NUMBER                                  AS src_row_number,
        $1                                                                 AS payload
    FROM @FIN_INGEST.RAW.STG_FIN_GCS/client_c/json/
)
FILE_FORMAT = (FORMAT_NAME = FIN_INGEST.RAW.FF_JSON)
ON_ERROR = 'CONTINUE';

INSERT INTO FIN_INGEST.RAW.RAW_LOAD_AUDIT
SELECT
    file,
    'JSON' AS file_type,
    status AS load_status,
    rows_parsed,
    rows_loaded,
    errors_seen,
    first_error,
    CURRENT_TIMESTAMP()
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- XML CLIENT LOAD PATTERN
COPY INTO FIN_INGEST.RAW.RAW_TXN_XML (client_id, src_file, src_row_number, payload)
FROM (
    SELECT
        'CLIENT_A'::STRING                                                AS client_id,
        METADATA$FILENAME::STRING                                         AS src_file,
        METADATA$FILE_ROW_NUMBER::NUMBER                                  AS src_row_number,
        $1                                                                 AS payload
    FROM @FIN_INGEST.RAW.STG_FIN_GCS/client_a/xml/
)
FILE_FORMAT = (FORMAT_NAME = FIN_INGEST.RAW.FF_XML)
ON_ERROR = 'CONTINUE';

INSERT INTO FIN_INGEST.RAW.RAW_LOAD_AUDIT
SELECT
    file,
    'XML' AS file_type,
    status AS load_status,
    rows_parsed,
    rows_loaded,
    errors_seen,
    first_error,
    CURRENT_TIMESTAMP()
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- CSV LOAD PATTERN (for both client_a/csv and client_c/csv)
COPY INTO FIN_INGEST.RAW.RAW_CSV_GENERIC (client_id, src_file, src_row_number, payload)
FROM (
    SELECT
        IFF(METADATA$FILENAME ILIKE '%client_a/%', 'CLIENT_A', 'CLIENT_C') AS client_id,
        METADATA$FILENAME::STRING                                           AS src_file,
        METADATA$FILE_ROW_NUMBER::NUMBER                                    AS src_row_number,
        ARRAY_CONSTRUCT(*)                                                   AS payload
    FROM @FIN_INGEST.RAW.STG_FIN_GCS/
)
FILES = ('client_a/csv/transactions.csv','client_c/csv/transactions.csv')
FILE_FORMAT = (FORMAT_NAME = FIN_INGEST.RAW.FF_CSV)
ON_ERROR = 'CONTINUE';

INSERT INTO FIN_INGEST.RAW.RAW_LOAD_AUDIT
SELECT
    file,
    'CSV' AS file_type,
    status AS load_status,
    rows_parsed,
    rows_loaded,
    errors_seen,
    first_error,
    CURRENT_TIMESTAMP()
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
------------------------------------------------------------------------------- */

/* === 03 | CANONICAL TABLE DDL ============================================== */
CREATE OR REPLACE TABLE FIN_INGEST.CANON.CAN_TXN (
    canonical_txn_id STRING,
    client_id STRING,
    source_system STRING,
    source_txn_id STRING,
    txn_timestamp TIMESTAMP_NTZ,
    currency STRING,
    total_amount NUMBER(38, 9),
    customer_id STRING,
    account_id STRING,
    merchant STRING,
    src_file STRING,
    ingest_ts TIMESTAMP_NTZ,
    is_valid BOOLEAN,
    anomaly_codes ARRAY,
    attributes VARIANT,
    created_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_CAN_TXN PRIMARY KEY (canonical_txn_id)
);

CREATE OR REPLACE TABLE FIN_INGEST.CANON.CAN_TXN_LINE (
    canonical_txn_id STRING,
    line_number NUMBER,
    line_txn_id STRING,
    item_id STRING,
    description STRING,
    quantity NUMBER(38, 9),
    unit_price NUMBER(38, 9),
    line_amount NUMBER(38, 9),
    currency STRING,
    src_file STRING,
    ingest_ts TIMESTAMP_NTZ,
    attributes VARIANT,
    created_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_CAN_TXN_LINE PRIMARY KEY (canonical_txn_id, line_number)
);

CREATE OR REPLACE TABLE FIN_INGEST.CANON.CAN_TXN_ANOMALY (
    canonical_txn_id STRING,
    client_id STRING,
    source_system STRING,
    anomaly_code STRING,
    anomaly_detail STRING,
    line_number NUMBER,
    src_file STRING,
    detected_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_CAN_TXN_ANOMALY PRIMARY KEY (canonical_txn_id, anomaly_code, line_number, anomaly_detail)
);

/* === 04 | NORMALIZE RAW -> STAGING HEADER ===================================
   Assumptions:
   - JSON and XML may use different keys; COALESCE across common alternatives.
   - CSV payload is ARRAY; indices are treated as best-effort mapping and all
     unknown columns are retained in attributes.
   - TRY_TO_* used everywhere to tolerate schema drift and malformed values.
*/
CREATE OR REPLACE TEMP TABLE STG_CAN_TXN_HEADER AS
WITH
json_hdr AS (
    SELECT
        r.client_id,
        'JSON' AS source_system,
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
    SELECT
        r.client_id,
        'XML' AS source_system,
        COALESCE(
            r.payload:"@transaction_id"::STRING,
            r.payload:"transaction_id"::STRING,
            r.payload:"txn_id"::STRING,
            r.payload:"id"::STRING
        ) AS source_txn_id,
        TRY_TO_TIMESTAMP_NTZ(COALESCE(
            r.payload:"transaction_ts"::STRING,
            r.payload:"transaction_time"::STRING,
            r.payload:"timestamp"::STRING,
            r.payload:"txn_timestamp"::STRING
        )) AS txn_timestamp,
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
    SELECT
        r.client_id,
        'CSV' AS source_system,
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
with_keys AS (
    SELECT
        *,
        COALESCE(source_txn_id, payload_hash) AS dedupe_business_key,
        COALESCE(
            IFF(source_txn_id IS NOT NULL,
                SHA2(client_id || '|' || source_system || '|' || source_txn_id, 256),
                SHA2(client_id || '|' || source_system || '|' || payload_hash, 256)
            ),
            SHA2(client_id || '|' || source_system || '|' || src_file || '|' || NVL(TO_VARCHAR(txn_timestamp), ''), 256)
        ) AS canonical_txn_id
    FROM all_hdr
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY client_id, source_system, dedupe_business_key
            ORDER BY ingest_ts DESC, src_file DESC
        ) AS rn,
        COUNT(*) OVER (
            PARTITION BY client_id, source_system, dedupe_business_key
        ) AS dup_cnt
    FROM with_keys
)
SELECT
    canonical_txn_id,
    client_id,
    source_system,
    source_txn_id,
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

/* === 05 | NORMALIZE RAW -> STAGING LINE ITEMS ===============================
   Inconsistent nesting is handled with wide COALESCE pathing + OUTER FLATTEN.
   If a source has no line items, no line rows are required.
*/
CREATE OR REPLACE TEMP TABLE STG_CAN_TXN_LINE AS
WITH
json_line AS (
    SELECT
        h.canonical_txn_id,
        COALESCE(TRY_TO_NUMBER(li.value:line_number::STRING), li.index + 1) AS line_number,
        COALESCE(li.value:line_id::STRING, li.value:id::STRING) AS line_txn_id,
        COALESCE(li.value:item_id::STRING, li.value:sku::STRING, li.value:product_id::STRING) AS item_id,
        COALESCE(li.value:description::STRING, li.value:item_name::STRING, li.value:name::STRING) AS description,
        TRY_TO_NUMBER(COALESCE(li.value:quantity::STRING, li.value:qty::STRING)) AS quantity,
        TRY_TO_NUMBER(COALESCE(li.value:unit_price::STRING, li.value:price::STRING)) AS unit_price,
        TRY_TO_NUMBER(COALESCE(li.value:line_amount::STRING, li.value:amount::STRING, li.value:total::STRING)) AS line_amount,
        UPPER(COALESCE(li.value:currency::STRING, r.payload:currency::STRING, r.payload:ccy::STRING)) AS currency,
        r.src_file,
        r.ingest_ts,
        OBJECT_CONSTRUCT('raw_line', li.value, 'source_format', 'JSON') AS attributes
    FROM FIN_INGEST.RAW.RAW_TXN_JSON r
    JOIN STG_CAN_TXN_HEADER h
      ON h.client_id = r.client_id
     AND h.source_system = 'JSON'
     AND h.src_file = r.src_file
     AND h.rn = 1
    , LATERAL FLATTEN(
        INPUT => COALESCE(r.payload:line_items, r.payload:items, r.payload:lines),
        OUTER => TRUE
      ) li
    WHERE li.value IS NOT NULL
),
xml_line AS (
    SELECT
        h.canonical_txn_id,
        COALESCE(TRY_TO_NUMBER(li.value:"line_number"::STRING), li.index + 1) AS line_number,
        COALESCE(li.value:"line_id"::STRING, li.value:"id"::STRING) AS line_txn_id,
        COALESCE(li.value:"item_id"::STRING, li.value:"sku"::STRING, li.value:"product_id"::STRING) AS item_id,
        COALESCE(li.value:"description"::STRING, li.value:"item_name"::STRING, li.value:"name"::STRING) AS description,
        TRY_TO_NUMBER(COALESCE(li.value:"quantity"::STRING, li.value:"qty"::STRING)) AS quantity,
        TRY_TO_NUMBER(COALESCE(li.value:"unit_price"::STRING, li.value:"price"::STRING)) AS unit_price,
        TRY_TO_NUMBER(COALESCE(li.value:"line_amount"::STRING, li.value:"amount"::STRING, li.value:"total"::STRING)) AS line_amount,
        UPPER(COALESCE(li.value:"currency"::STRING, r.payload:"currency"::STRING, r.payload:"ccy"::STRING)) AS currency,
        r.src_file,
        r.ingest_ts,
        OBJECT_CONSTRUCT('raw_line', li.value, 'source_format', 'XML') AS attributes
    FROM FIN_INGEST.RAW.RAW_TXN_XML r
    JOIN STG_CAN_TXN_HEADER h
      ON h.client_id = r.client_id
     AND h.source_system = 'XML'
     AND h.src_file = r.src_file
     AND h.rn = 1
    , LATERAL FLATTEN(
        INPUT => COALESCE(
            r.payload:"line_items":"line",
            r.payload:"items":"item",
            r.payload:"lines":"line"
        ),
        OUTER => TRUE
      ) li
    WHERE li.value IS NOT NULL
),
csv_line AS (
    SELECT
        h.canonical_txn_id,
        1 AS line_number,
        NULL AS line_txn_id,
        r.payload[7]::STRING AS item_id,
        r.payload[8]::STRING AS description,
        TRY_TO_NUMBER(r.payload[9]::STRING) AS quantity,
        TRY_TO_NUMBER(r.payload[10]::STRING) AS unit_price,
        TRY_TO_NUMBER(r.payload[11]::STRING) AS line_amount,
        UPPER(COALESCE(r.payload[2]::STRING, r.payload[12]::STRING)) AS currency,
        r.src_file,
        r.ingest_ts,
        OBJECT_CONSTRUCT('csv_payload', r.payload, 'source_format', 'CSV', 'mapping_assumption', '0..6=header, 7..11=line') AS attributes
    FROM FIN_INGEST.RAW.RAW_CSV_GENERIC r
    JOIN STG_CAN_TXN_HEADER h
      ON h.client_id = r.client_id
     AND h.source_system = 'CSV'
     AND h.src_file = r.src_file
     AND h.rn = 1
    WHERE r.payload[7] IS NOT NULL OR r.payload[8] IS NOT NULL OR r.payload[11] IS NOT NULL
)
SELECT * FROM json_line
UNION ALL
SELECT * FROM xml_line
UNION ALL
SELECT * FROM csv_line;

/* === 06 | MERGE INTO CANONICAL TABLES ======================================= */
MERGE INTO FIN_INGEST.CANON.CAN_TXN t
USING (
    SELECT
        canonical_txn_id,
        client_id,
        source_system,
        source_txn_id,
        txn_timestamp,
        currency,
        total_amount,
        customer_id,
        account_id,
        merchant,
        src_file,
        ingest_ts,
        IFF(ARRAY_SIZE(anomaly_codes) = 0, TRUE, FALSE) AS is_valid,
        anomaly_codes,
        attributes
    FROM STG_CAN_TXN_HEADER
    WHERE rn = 1
) s
ON t.canonical_txn_id = s.canonical_txn_id
WHEN MATCHED THEN UPDATE SET
    t.client_id = s.client_id,
    t.source_system = s.source_system,
    t.source_txn_id = s.source_txn_id,
    t.txn_timestamp = s.txn_timestamp,
    t.currency = s.currency,
    t.total_amount = s.total_amount,
    t.customer_id = s.customer_id,
    t.account_id = s.account_id,
    t.merchant = s.merchant,
    t.src_file = s.src_file,
    t.ingest_ts = s.ingest_ts,
    t.is_valid = s.is_valid,
    t.anomaly_codes = s.anomaly_codes,
    t.attributes = s.attributes,
    t.updated_ts = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    canonical_txn_id, client_id, source_system, source_txn_id, txn_timestamp,
    currency, total_amount, customer_id, account_id, merchant,
    src_file, ingest_ts, is_valid, anomaly_codes, attributes,
    created_ts, updated_ts
) VALUES (
    s.canonical_txn_id, s.client_id, s.source_system, s.source_txn_id, s.txn_timestamp,
    s.currency, s.total_amount, s.customer_id, s.account_id, s.merchant,
    s.src_file, s.ingest_ts, s.is_valid, s.anomaly_codes, s.attributes,
    CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

MERGE INTO FIN_INGEST.CANON.CAN_TXN_LINE t
USING (
    SELECT
        canonical_txn_id,
        line_number,
        line_txn_id,
        item_id,
        description,
        quantity,
        unit_price,
        line_amount,
        currency,
        src_file,
        ingest_ts,
        attributes
    FROM STG_CAN_TXN_LINE
) s
ON t.canonical_txn_id = s.canonical_txn_id
AND t.line_number = s.line_number
WHEN MATCHED THEN UPDATE SET
    t.line_txn_id = s.line_txn_id,
    t.item_id = s.item_id,
    t.description = s.description,
    t.quantity = s.quantity,
    t.unit_price = s.unit_price,
    t.line_amount = s.line_amount,
    t.currency = s.currency,
    t.src_file = s.src_file,
    t.ingest_ts = s.ingest_ts,
    t.attributes = s.attributes,
    t.updated_ts = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    canonical_txn_id, line_number, line_txn_id, item_id, description,
    quantity, unit_price, line_amount, currency,
    src_file, ingest_ts, attributes, created_ts, updated_ts
) VALUES (
    s.canonical_txn_id, s.line_number, s.line_txn_id, s.item_id, s.description,
    s.quantity, s.unit_price, s.line_amount, s.currency,
    s.src_file, s.ingest_ts, s.attributes, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

/* === 07 | ANOMALY CAPTURE ====================================================
   Rules implemented:
   - DUPLICATE_TXN (all records that share business key)
   - MISSING_REQUIRED (txn_timestamp OR total_amount missing)
   - NEGATIVE_AMOUNT (header total < 0)
   - NEGATIVE_QTY (line quantity < 0)
   - NEGATIVE_AMOUNT_LINE (line amount < 0)
*/
CREATE OR REPLACE TEMP TABLE STG_ANOMALY AS
WITH hdr_flags AS (
    SELECT
        h.canonical_txn_id,
        h.client_id,
        h.source_system,
        f.value::STRING AS anomaly_code,
        CAST(NULL AS NUMBER) AS line_number,
        h.src_file,
        'Header-level anomaly from canonical header validation' AS anomaly_detail
    FROM STG_CAN_TXN_HEADER h,
         LATERAL FLATTEN(INPUT => h.anomaly_codes) f
    WHERE h.rn = 1
),
line_flags AS (
    SELECT
        l.canonical_txn_id,
        t.client_id,
        t.source_system,
        IFF(l.quantity < 0, 'NEGATIVE_QTY', 'NEGATIVE_AMOUNT_LINE') AS anomaly_code,
        l.line_number,
        l.src_file,
        'Line-level negative value detected' AS anomaly_detail
    FROM STG_CAN_TXN_LINE l
    JOIN FIN_INGEST.CANON.CAN_TXN t
      ON t.canonical_txn_id = l.canonical_txn_id
    WHERE l.quantity < 0 OR l.line_amount < 0
)
SELECT * FROM hdr_flags
UNION ALL
SELECT * FROM line_flags;

MERGE INTO FIN_INGEST.CANON.CAN_TXN_ANOMALY a
USING (
    SELECT DISTINCT
        canonical_txn_id,
        client_id,
        source_system,
        anomaly_code,
        anomaly_detail,
        line_number,
        src_file
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

/* === 08 | OPS / OBSERVABILITY VIEWS ========================================= */
CREATE OR REPLACE VIEW FIN_INGEST.OPS.VW_LOAD_AUDIT_SUMMARY AS
SELECT
    file_type,
    load_status,
    COUNT(*) AS batch_count,
    SUM(rows_parsed) AS total_rows_parsed,
    SUM(rows_loaded) AS total_rows_loaded,
    SUM(errors_seen) AS total_errors_seen,
    MAX(load_ts) AS latest_load_ts
FROM FIN_INGEST.RAW.RAW_LOAD_AUDIT
GROUP BY file_type, load_status;

CREATE OR REPLACE VIEW FIN_INGEST.OPS.VW_CANON_COUNTS AS
SELECT
    client_id,
    source_system,
    COUNT(*) AS txn_count,
    SUM(IFF(is_valid, 1, 0)) AS valid_txn_count,
    SUM(IFF(NOT is_valid, 1, 0)) AS invalid_txn_count
FROM FIN_INGEST.CANON.CAN_TXN
GROUP BY client_id, source_system;

CREATE OR REPLACE VIEW FIN_INGEST.OPS.VW_ANOMALY_COUNTS AS
SELECT
    a.client_id,
    a.source_system,
    a.anomaly_code,
    COUNT(*) AS anomaly_count
FROM FIN_INGEST.CANON.CAN_TXN_ANOMALY a
GROUP BY a.client_id, a.source_system, a.anomaly_code;

/* === 09 | SMOKE TESTS ======================================================== */
SELECT 'CAN_TXN' AS object_name, COUNT(*) AS row_count FROM FIN_INGEST.CANON.CAN_TXN
UNION ALL
SELECT 'CAN_TXN_LINE' AS object_name, COUNT(*) AS row_count FROM FIN_INGEST.CANON.CAN_TXN_LINE
UNION ALL
SELECT 'CAN_TXN_ANOMALY' AS object_name, COUNT(*) AS row_count FROM FIN_INGEST.CANON.CAN_TXN_ANOMALY;

SELECT * FROM FIN_INGEST.OPS.VW_CANON_COUNTS ORDER BY client_id, source_system;
SELECT * FROM FIN_INGEST.OPS.VW_ANOMALY_COUNTS ORDER BY anomaly_count DESC, client_id, source_system;
SELECT * FROM FIN_INGEST.OPS.VW_LOAD_AUDIT_SUMMARY ORDER BY latest_load_ts DESC;

/* End of script */
