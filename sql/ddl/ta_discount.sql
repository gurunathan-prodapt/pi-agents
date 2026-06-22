--
-- BigQuery DDL for ta_discount (source table)
-- Replaces: Oracle table sof$ta_discount used in d_ausd_v_ta_disc_zusgf.sql
--
-- Schema inferred from SELECT statement in d_ausd_v_ta_disc_zusgf.sql
CREATE TABLE IF NOT EXISTS `my_project.my_dataset.ta_discount` (
  cntrct_id INT64,
  cntrct_obj_version INT64,
  disc_vector_ty STRING,
  rabatt STRING,
  rabatthoehe INT64
);