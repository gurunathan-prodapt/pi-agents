-- BigQuery DDL for raw_isbert.dwtk_meldungen
-- Legacy source: Oracle table inferred from d_ausd_v_ta_discount_rr.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_discount_rr.ksh

CREATE TABLE IF NOT EXISTS `your_project.raw_isbert.dwtk_meldungen` (
    timecreated TIMESTAMP,
    job_kennung STRING
);