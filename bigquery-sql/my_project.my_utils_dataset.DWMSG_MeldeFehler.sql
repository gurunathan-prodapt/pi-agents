-- BigQuery Stored Procedure for DWMSG_MeldeFehler
-- Utility for reporting errors, replacing logic from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vertrag_tmp.ksh
CREATE OR REPLACE PROCEDURE `my_project.my_utils_dataset.DWMSG_MeldeFehler`(
  IN p_job_name STRING,
  IN p_error_message STRING
)
BEGIN
  CALL `my_project.my_utils_dataset.DWMSG_ErzeugeEintrag`(
    p_job_name,
    'FAILED',
    p_error_message,
    NULL,
    CURRENT_TIMESTAMP(),
    1
  );
  RAISE USING MESSAGE p_error_message;
END;