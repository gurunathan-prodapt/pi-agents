-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount_rr.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount_rr.ksh

CREATE OR REPLACE PROCEDURE `my_gcp_project.my_bq_dataset.DWMSG_ErzeugeEintrag`(
  IN p_dw_eintrags_nr STRING,
  IN p_job_kennung STRING,
  IN p_programm_name STRING,
  IN p_log_datei STRING
)
BEGIN
  -- This procedure logs the start of a job or a significant event.
  INSERT INTO `my_gcp_project.my_bq_dataset.job_log`
    (job_id, job_name, severity, message, created_at)
  VALUES
    (p_dw_eintrags_nr, p_job_kennung, 'I', CONCAT('Job started: ', p_programm_name, ' (Conceptual log ID: ', p_log_datei, ')'), CURRENT_TIMESTAMP());
END;