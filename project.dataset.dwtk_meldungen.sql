--
-- Target BigQuery DDL for table dwtk_meldungen
-- Replaces usage in vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_notice.ksh
--
-- NOTE: This is a placeholder schema based on the query usage.
--       The actual schema should be derived from the source Oracle `isbert_schema.dwtk_meldungen` table.
--
CREATE OR REPLACE TABLE `project.dataset.dwtk_meldungen`
(
    job_kennung STRING,
    timecreated TIMESTAMP
);