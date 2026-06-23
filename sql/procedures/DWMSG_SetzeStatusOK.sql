-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount_rr.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount_rr.ksh

CREATE OR REPLACE PROCEDURE `my_gcp_project.my_bq_dataset.DWMSG_SetzeStatusOK`(
  IN p_dw_eintrags_nr STRING
)
BEGIN
  -- This procedure logs a successful completion status for the job.
  INSERT INTO `my_gcp_project.my_bq_dataset.job_log`
    (job_id, job_name, severity, message, created_at)
  VALUES
    (p_dw_eintrags_nr, 'N/A', 'I', 'Job completed successfully.', CURRENT_TIMESTAMP());
END;