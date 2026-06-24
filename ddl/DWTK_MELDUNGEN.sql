-- Migrated DDL for legacy source table DWTK_MELDUNGEN
-- Legacy job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_msisdn.ksh

CREATE TABLE IF NOT EXISTS `YOUR_BIGQUERY_PROJECT.YOUR_BIGQUERY_DATASET.DWTK_MELDUNGEN` (
    timecreated TIMESTAMP,
    job_kennung STRING,
    -- Assuming common log table columns based on usage in the SQL script
    message STRING,
    severity STRING
);