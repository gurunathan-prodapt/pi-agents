-- Target BigQuery table for error logging.
-- Replaces the external shell function DWMSG_MeldeFehler in the legacy job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.ksh
CREATE TABLE `project.dataset.error_log` (
  entry_number STRING,
  severity STRING,
  error_code INT64,
  message STRING,
  timestamp TIMESTAMP
);