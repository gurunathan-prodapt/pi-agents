-- BigQuery DDL for PoolBasisprodukt table
-- Replaces usage of the legacy 'PoolBasisprodukt' table from Oracle.
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_instance.ksh
-- This table is assumed to be the final target for processed basis product instances.

CREATE TABLE IF NOT EXISTS my_gcp_project.my_bq_dataset.PoolBasisprodukt
(
    CNTRCT_ID       INT64       NOT NULL,
    BPR_ID          INT64       NOT NULL,
    BPR_INSTANCE_ID INT64       NOT NULL,
    ICCID           STRING,
    IMSI_MCC        INT64,
    IMSI_MNC        INT64,
    IMSI_HLR        INT64,
    IMSI_SI         INT64,
    CNTRCT_ID_REF   INT64,
    -- Add any other relevant columns from the original PoolBasisprodukt table if known
    processing_date DATE        NOT NULL -- Added for partitioning/clustering and data context
)
PARTITION BY processing_date;