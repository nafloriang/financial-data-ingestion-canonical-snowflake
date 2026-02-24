/* 04_transform_lines.sql
   Normalize nested line-items and inconsistent structures into canonical line staging.
*/
USE DATABASE FIN_INGEST;

CREATE OR REPLACE TEMP TABLE STG_CAN_TXN_LINE AS
WITH
json_line AS (
    SELECT h.canonical_txn_id,
        COALESCE(TRY_TO_NUMBER(li.value:line_number::STRING), li.index + 1) AS line_number,
        COALESCE(li.value:line_id::STRING, li.value:id::STRING) AS line_txn_id,
        COALESCE(li.value:item_id::STRING, li.value:sku::STRING, li.value:product_id::STRING) AS item_id,
        COALESCE(li.value:description::STRING, li.value:item_name::STRING, li.value:name::STRING) AS description,
        TRY_TO_NUMBER(COALESCE(li.value:quantity::STRING, li.value:qty::STRING)) AS quantity,
        TRY_TO_NUMBER(COALESCE(li.value:unit_price::STRING, li.value:price::STRING)) AS unit_price,
        TRY_TO_NUMBER(COALESCE(li.value:line_amount::STRING, li.value:amount::STRING, li.value:total::STRING)) AS line_amount,
        UPPER(COALESCE(li.value:currency::STRING, r.payload:currency::STRING, r.payload:ccy::STRING)) AS currency,
        r.src_file, r.ingest_ts,
        OBJECT_CONSTRUCT('raw_line', li.value, 'source_format', 'JSON') AS attributes
    FROM FIN_INGEST.RAW.RAW_TXN_JSON r
    JOIN STG_CAN_TXN_HEADER h ON h.client_id = r.client_id AND h.source_system = 'JSON' AND h.src_file = r.src_file AND h.rn = 1,
         LATERAL FLATTEN(INPUT => COALESCE(r.payload:line_items, r.payload:items, r.payload:lines), OUTER => TRUE) li
    WHERE li.value IS NOT NULL
),
xml_line AS (
    SELECT h.canonical_txn_id,
        COALESCE(TRY_TO_NUMBER(li.value:"line_number"::STRING), li.index + 1) AS line_number,
        COALESCE(li.value:"line_id"::STRING, li.value:"id"::STRING) AS line_txn_id,
        COALESCE(li.value:"item_id"::STRING, li.value:"sku"::STRING, li.value:"product_id"::STRING) AS item_id,
        COALESCE(li.value:"description"::STRING, li.value:"item_name"::STRING, li.value:"name"::STRING) AS description,
        TRY_TO_NUMBER(COALESCE(li.value:"quantity"::STRING, li.value:"qty"::STRING)) AS quantity,
        TRY_TO_NUMBER(COALESCE(li.value:"unit_price"::STRING, li.value:"price"::STRING)) AS unit_price,
        TRY_TO_NUMBER(COALESCE(li.value:"line_amount"::STRING, li.value:"amount"::STRING, li.value:"total"::STRING)) AS line_amount,
        UPPER(COALESCE(li.value:"currency"::STRING, r.payload:"currency"::STRING, r.payload:"ccy"::STRING)) AS currency,
        r.src_file, r.ingest_ts,
        OBJECT_CONSTRUCT('raw_line', li.value, 'source_format', 'XML') AS attributes
    FROM FIN_INGEST.RAW.RAW_TXN_XML r
    JOIN STG_CAN_TXN_HEADER h ON h.client_id = r.client_id AND h.source_system = 'XML' AND h.src_file = r.src_file AND h.rn = 1,
         LATERAL FLATTEN(INPUT => COALESCE(r.payload:"line_items":"line", r.payload:"items":"item", r.payload:"lines":"line"), OUTER => TRUE) li
    WHERE li.value IS NOT NULL
),
csv_line AS (
    SELECT h.canonical_txn_id,
        1 AS line_number,
        NULL AS line_txn_id,
        r.payload[7]::STRING AS item_id,
        r.payload[8]::STRING AS description,
        TRY_TO_NUMBER(r.payload[9]::STRING) AS quantity,
        TRY_TO_NUMBER(r.payload[10]::STRING) AS unit_price,
        TRY_TO_NUMBER(r.payload[11]::STRING) AS line_amount,
        UPPER(COALESCE(r.payload[2]::STRING, r.payload[12]::STRING)) AS currency,
        r.src_file, r.ingest_ts,
        OBJECT_CONSTRUCT('csv_payload', r.payload, 'source_format', 'CSV', 'mapping_assumption', '0..6=header, 7..11=line') AS attributes
    FROM FIN_INGEST.RAW.RAW_CSV_GENERIC r
    JOIN STG_CAN_TXN_HEADER h ON h.client_id = r.client_id AND h.source_system = 'CSV' AND h.src_file = r.src_file AND h.rn = 1
    WHERE r.payload[7] IS NOT NULL OR r.payload[8] IS NOT NULL OR r.payload[11] IS NOT NULL
)
SELECT * FROM json_line
UNION ALL
SELECT * FROM xml_line
UNION ALL
SELECT * FROM csv_line;
