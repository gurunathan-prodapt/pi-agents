-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount_rr.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount_rr.ksh

CREATE OR REPLACE PROCEDURE `my_gcp_project.my_bq_dataset.DWMSG_Fehlerbehandlung`(
  IN p_dw_eintrags_nr STRING
)
BEGIN
  -- This procedure handles error logging within the BigQuery context.
  -- It retrieves current error information using ERROR functions and logs it.
  INSERT INTO `my_gcp_project.my_bq_dataset.job_log`
    (job_id, job_name, severity, error_code, message, created_at)
  VALUES
    (p_dw_eintrags_nr, 'N/A', 'E', ERROR_CODE(), CONCAT('Job failed with error: ', ERROR_MESSAGE()), CURRENT_TIMESTAMP());
END;