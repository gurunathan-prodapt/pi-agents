-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_disc_zusgf.ksh
-- Purpose: Utility SP to construct a log file name based on job details.

CREATE OR REPLACE PROCEDURE `project_id.dataset_id.DWMSG_Logdateiname_sp`(
  OUT p_log_file STRING,
  p_job_kennung STRING,
  p_job_nr INT64
)
BEGIN
  -- Simulate a log file name. In BigQuery, logs are typically table-based.
  -- This name could be used as an identifier in the log table.
  SET p_log_file = FORMAT('log_%s_%s_%s.log', p_job_kennung, CAST(p_job_nr AS STRING), FORMAT_DATE('%Y%m%d', CURRENT_DATE()));
END;