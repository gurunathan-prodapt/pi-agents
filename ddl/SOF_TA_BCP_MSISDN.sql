-- Migrated DDL for legacy target table SOF$TA_BCP_MSISDN
-- Legacy job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_msisdn.ksh

CREATE TABLE IF NOT EXISTS `YOUR_BIGQUERY_PROJECT.YOUR_BIGQUERY_DATASET.SOF_TA_BCP_MSISDN` (
    CNTRCT_ID INT64,
    BPR_ID INT64,
    CNTRCT_ID_REF INT64,
    TN_TEL_MSISDN STRING,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);