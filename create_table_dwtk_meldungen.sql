--
-- BigQuery DDL for control table dwtk_meldungen
-- Replaces Oracle table dwtk_meldungen from job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount.ksh
--
CREATE TABLE IF NOT EXISTS `project.dataset.dwtk_meldungen`
(
    job_kennung         STRING,
    timecreated         TIMESTAMP
);