-- File: stored_procedures/sp_execute_core_sql.sql
-- Reusable core execution wrapper
-- Replace the placeholder query with translated logic from d_ausd_bp_ta_msisdn.sql

CREATE OR REPLACE PROCEDURE `gcp-project-placeholder.dw_isbert_dataset.sp_execute_core_sql`(
  IN p_job_name STRING,
  IN p_tab_name STRING,
  IN p_stichtag_date DATE,
  IN p_today DATE,
  IN p_yesterday DATE,
  IN p_restart_value STRING,
  OUT o_records INT64
)
BEGIN
  DECLARE v_sql STRING;

  -- Reusable staging table for intermediate results
  CREATE TEMP TABLE tmp_core_result AS
  SELECT
    p_tab_name AS tab_name,
    p_stichtag_date AS stichtag_date,
    p_today AS today_date,
    p_yesterday AS yesterday_date,
    p_restart_value AS restart_value;

  -- TODO: Replace with actual translated SQL from d_ausd_bp_ta_msisdn.sql
  -- Example pattern:
  -- INSERT INTO `gcp-project-placeholder.dw_isbert_dataset.target_table`
  -- SELECT ...
  -- FROM source_table
  -- WHERE business_date = p_stichtag_date;

  SET o_records = (SELECT COUNT(*) FROM tmp_core_result);
END;