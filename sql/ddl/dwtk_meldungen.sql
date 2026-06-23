-- DDL for BigQuery table dwtk_meldungen
-- Replaces data from legacy Oracle source.
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh

CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.dwtk_meldungen`
(
    timecreated TIMESTAMP,
    job_kennung STRING,
    -- Add other columns as per source schema.
    -- Example:
    -- some_other_field STRING,
    -- some_numeric_field INT64
);