-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_acc_ref.ksh
-- Description: DDL for the error log table.
CREATE TABLE `project.dataset.error_log` (
  error_ts TIMESTAMP NOT NULL,
  error_source STRING,
  error_nr INT64,
  error_arg STRING,
  message_text STRING
);