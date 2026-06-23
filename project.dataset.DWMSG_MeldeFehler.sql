-- Target BigQuery stored procedure for error logging.
-- Replaces the external shell function DWMSG_MeldeFehler in the legacy job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.ksh
CREATE OR REPLACE PROCEDURE `project.dataset.DWMSG_MeldeFehler`(
  p_Eintragsnr STRING,
  p_Severity STRING,
  p_ErrorCode INT64,
  p_Message STRING
)
BEGIN
  INSERT INTO `project.dataset.error_log` (entry_number, severity, error_code, message, timestamp)
  VALUES (p_Eintragsnr, p_Severity, p_ErrorCode, p_Message, CURRENT_TIMESTAMP());
END;