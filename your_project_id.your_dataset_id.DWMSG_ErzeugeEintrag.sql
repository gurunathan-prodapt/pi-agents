-- Target for: Helper Stored Procedures
-- Legacy Source: N/A (logging utility)
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_assign.ksh

CREATE OR REPLACE PROCEDURE `your_project_id.your_dataset_id.DWMSG_ErzeugeEintrag`(
  p_job_id STRING,
  p_entry_nr INT64,
  p_log_level STRING,
  p_message STRING
)
BEGIN
  INSERT INTO `your_project_id.your_dataset_id.job_log` (
    job_id,
    entry_nr,
    log_level,
    message,
    timestamp
  )
  VALUES (
    p_job_id,
    p_entry_nr,
    p_log_level,
    p_message,
    CURRENT_TIMESTAMP()
  );
END;