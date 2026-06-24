-- Migrated DDL for legacy source table SOF$TA_BPR_BCP
-- Legacy job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_msisdn.ksh

CREATE TABLE IF NOT EXISTS `YOUR_BIGQUERY_PROJECT.YOUR_BIGQUERY_DATASET.SOF_TA_BPR_BCP` (
    cntrct_id INT64,
    bpr_id INT64,
    cntrct_id_ref INT64,
    -- Adding common fields for tables
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);