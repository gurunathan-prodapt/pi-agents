-- BigQuery DDL for the staging table dwtk_meldungen
-- Replica of Oracle isbert_schema.dwtk_meldungen used by job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_acc_ref.ksh
CREATE TABLE IF NOT EXISTS `isbert_rpt_staging.dwtk_meldungen`
(
    timecreated TIMESTAMP,
    job_kennung STRING
    -- Add other columns from the source Oracle table as needed for a complete replica
);