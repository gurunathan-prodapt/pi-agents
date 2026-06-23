-- BigQuery DDL for error_log
-- New table for logging errors, replacing legacy f_alis_msgerr.ksh and DWMSG_MeldeFehler.
-- Part of the migration for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vertrag_tmp.ksh

CREATE TABLE IF NOT EXISTS `project.dataset.error_log`
(
    log_time       TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    job_kennung    STRING,
    eintrags_nr    STRING,
    error_code     STRING,
    error_message  STRING,
    severity       STRING, -- e.g., 'ERROR', 'WARNING', 'INFO'
    source_script  STRING,
    -- Add other relevant error details
    stack_trace    STRING
);