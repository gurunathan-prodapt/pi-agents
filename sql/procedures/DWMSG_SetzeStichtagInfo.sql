-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount_rr.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount_rr.ksh

CREATE OR REPLACE PROCEDURE `my_gcp_project.my_bq_dataset.DWMSG_SetzeStichtagInfo`(
  IN p_dw_eintrags_nr STRING,
  IN p_stichtag_info STRING,
  IN p_format STRING
)
BEGIN
  -- This procedure logs critical date information for the job.
  INSERT INTO `my_gcp_project.my_bq_dataset.job_log`
    (job_id, job_name, severity, message, created_at)
  VALUES
    (p_dw_eintrags_nr, 'N/A', 'I', CONCAT('Stichtag (Reference Date) set: ', p_stichtag_info, ' (Format: ', p_format, ')'), CURRENT_TIMESTAMP());
END;