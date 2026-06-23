-- BigQuery DDL for DWTK_MELDUNGEN
-- Replaces usage in legacy job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vertrag_tmp.ksh
-- Based on usage in d_ausd_v_ta_vertrag_tmp.sql

CREATE TABLE IF NOT EXISTS `project.dataset.dwtk_meldungen`
(
    timecreated DATETIME,
    job_kennung STRING,
    -- Add other columns if they exist in the source table
    loaded_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);