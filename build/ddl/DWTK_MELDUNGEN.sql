-- BigQuery DDL for legacy source table DWTK_MELDUNGEN
-- Original job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_barrier_zusgf.ksh
CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.DWTK_MELDUNGEN` (
    timecreated TIMESTAMP,
    job_kennung STRING
);