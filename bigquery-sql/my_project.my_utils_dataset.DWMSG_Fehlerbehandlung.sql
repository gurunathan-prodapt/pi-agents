-- BigQuery Stored Procedure for DWMSG_Fehlerbehandlung
-- Utility for general error handling, replacing logic from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vertrag_tmp.ksh
CREATE OR REPLACE PROCEDURE `my_project.my_utils_dataset.DWMSG_Fehlerbehandlung`(
  IN p_job_name STRING,
  IN p_context_message STRING,
  IN p_sql_error_message STRING
)
BEGIN
  DECLARE full_error_message STRING;
  SET full_error_message = p_context_message || ' SQL Error: ' || p_sql_error_message;
  CALL `my_project.my_utils_dataset.DWMSG_MeldeFehler`(p_job_name, full_error_message);
END;