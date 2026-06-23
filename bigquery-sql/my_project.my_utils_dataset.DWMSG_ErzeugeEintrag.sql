-- BigQuery Stored Procedure for DWMSG_ErzeugeEintrag
-- Utility for creating log entries, replacing logic from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vertrag_tmp.ksh
CREATE OR REPLACE PROCEDURE `my_project.my_utils_dataset.DWMSG_ErzeugeEintrag`(
  IN p_job_name STRING,
  IN p_status STRING,
  IN p_message STRING,
  IN p_start_time TIMESTAMP,
  IN p_end_time TIMESTAMP,
  IN p_exit_code INT64
)
BEGIN
  INSERT INTO `my_project.my_utils_dataset.job_log` (
    job_name,
    start_time,
    end_time,
    status,
    message,
    exit_code
  )
  VALUES (
    p_job_name,
    p_start_time,
    p_end_time,
    p_status,
    p_message,
    p_exit_code
  );
END;