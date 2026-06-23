-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_disc_zusgf.ksh
-- Purpose: Utility SP to log reference date information for a job.

CREATE OR REPLACE PROCEDURE `project_id.dataset_id.DWMSG_SetzeStichtagInfo_sp`(
  p_job_nr INT64,
  p_reference_date STRING,
  p_date_format STRING
)
BEGIN
  INSERT INTO `project_id.dataset_id.job_log_table`(job_nr, created_at, message)
  VALUES (p_job_nr, CURRENT_TIMESTAMP(), CONCAT('Reference Date Set: ', p_reference_date, ' (Format: ', p_date_format, ')'));
END;