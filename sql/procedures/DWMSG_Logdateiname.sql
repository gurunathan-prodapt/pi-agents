-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount_rr.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount_rr.ksh

CREATE OR REPLACE PROCEDURE `my_gcp_project.my_bq_dataset.DWMSG_Logdateiname`(
  OUT p_log_datei STRING,
  IN p_job_kennung STRING,
  IN p_dw_eintrags_nr STRING
)
BEGIN
  -- In the BigQuery context, there is no physical log file.
  -- This procedure will set p_log_datei to a conceptual log identifier
  -- which could be used to retrieve logs from the job_log table.
  SET p_log_datei = CONCAT('bq_log_', p_job_kennung, '_', p_dw_eintrags_nr);
END;