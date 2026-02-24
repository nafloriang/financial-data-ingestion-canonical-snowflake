/* 02_canonical_ddl.sql
   Canonical model DDL.
*/
USE DATABASE FIN_INGEST;

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
