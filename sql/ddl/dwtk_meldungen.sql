--
-- BigQuery DDL for dwtk_meldungen (source table)
-- Replaces: Oracle table isbert_schema.dwtk_meldungen used in d_ausd_v_ta_disc_zusgf.sql
--
-- Schema inferred from SELECT statement for v_datum in d_ausd_v_ta_disc_zusgf.sql
CREATE TABLE IF NOT EXISTS `my_project.my_dataset.dwtk_meldungen` (
  job_kennung STRING,
  timecreated TIMESTAMP
);