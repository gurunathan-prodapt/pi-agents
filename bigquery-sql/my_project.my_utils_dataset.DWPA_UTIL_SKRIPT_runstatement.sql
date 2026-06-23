-- BigQuery Stored Procedure for DWPA_UTIL_SKRIPT_runstatement
-- Utility for logging final job status, replacing a direct call from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vertrag_tmp.ksh
CREATE OR REPLACE PROCEDURE `my_project.my_utils_dataset.DWPA_UTIL_SKRIPT_runstatement`(
  IN p_exit_code INT64,
  IN p_message STRING
)
BEGIN
  -- This procedure is designed for general status messages, often at the end of a script.
  -- For detailed lifecycle logging, `DWMSG_ErzeugeEintrag` with full context is preferred.
  INSERT INTO `my_project.my_utils_dataset.job_log` (
    job_name,
    start_time,
    end_time,
    status,
    message,
    exit_code
  )
  VALUES (
    'FINAL_STATUS_LOG', -- Placeholder for job_name as it's not passed to this specific utility call.
    NULL,               -- Start time is not captured by this specific utility call.
    CURRENT_TIMESTAMP(),
    CASE WHEN p_exit_code = 0 THEN 'SUCCESS' ELSE 'FAILED' END,
    p_message,
    p_exit_code
  );
END;