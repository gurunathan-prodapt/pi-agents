--
-- BigQuery DDL for ta_disc_zusgf
-- Replaces: Oracle table sof$ta_disc_zusgf and usage in d_ausd_v_ta_disc_zusgf.sql
--
-- Schema inferred from INSERT statement in d_ausd_v_ta_disc_zusgf.sql
CREATE TABLE IF NOT EXISTS `my_project.my_dataset.ta_disc_zusgf` (
  cntrct_id INT64,
  cntrct_obj_version INT64,
  disc_vector_ty STRING,
  rabatt_alle STRING
);