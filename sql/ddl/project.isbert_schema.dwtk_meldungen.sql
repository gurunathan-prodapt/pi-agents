-- BigQuery DDL for project.isbert_schema.dwtk_meldungen
-- Replaces Oracle table DWTK_MELDUNGEN
-- Legacy Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs.ksh

CREATE TABLE IF NOT EXISTS `project.isbert_schema.dwtk_meldungen`
(
    timecreated TIMESTAMP,
    job_kennung STRING
)
OPTIONS(
    description="Migrated DWTK_MELDUNGEN table from Oracle."
);