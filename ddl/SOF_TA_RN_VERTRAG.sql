-- Migrated DDL for legacy source table SOF$TA_RN_VERTRAG (implied by SQL script)
-- Legacy job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_msisdn.ksh

CREATE TABLE IF NOT EXISTS `YOUR_BIGQUERY_PROJECT.YOUR_BIGQUERY_DATASET.SOF_TA_RN_VERTRAG` (
    cntrct_id INT64,
    tn_tel_msisdn STRING,
    -- Adding common fields for contract tables
    valid_from DATE,
    valid_to DATE,
    status STRING
);