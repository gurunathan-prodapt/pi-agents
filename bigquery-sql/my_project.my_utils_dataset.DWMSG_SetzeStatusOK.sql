-- BigQuery Stored Procedure for DWMSG_SetzeStatusOK
-- Utility for setting job status to OK, replacing logic from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vertrag_tmp.ksh
CREATE OR REPLACE PROCEDURE `my_project.my_utils_dataset.DWMSG_SetzeStatusOK`(
  IN p_job_name STRING,
  IN p_start_time TIMESTAMP
)
BEGIN
  CALL `my_project.my_utils_dataset.DWMSG_ErzeugeEintrag`(
    p_job_name,
    'SUCCESS',
    'Job completed successfully.',
    p_start_time,
    CURRENT_TIMESTAMP(),
    0
  );
END;