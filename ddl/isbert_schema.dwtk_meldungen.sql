-- BigQuery DDL for isbert_schema.dwtk_meldungen
-- Replaces Oracle table used by vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_apn_carmen.ksh
CREATE TABLE IF NOT EXISTS `your_project.your_dataset.dwtk_meldungen` (
    job_kennung STRING,
    timecreated DATETIME
);