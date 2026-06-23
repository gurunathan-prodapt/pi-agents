-- BigQuery schema definition for error_log
-- Replaces DWMSG_MeldeFehler calls in vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_inv_acc.ksh
CREATE TABLE IF NOT EXISTS `my_project.my_dataset.error_log` (
    error_ts TIMESTAMP,
    error_nr INT64,
    error_arg STRING,
    procedure_name STRING,
    message STRING
);