-- BigQuery DDL for project.dataset.error_log
-- Replaces error handling logic (DWMSG_MeldeFehler) in vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh

CREATE TABLE IF NOT EXISTS project.dataset.error_log (
    log_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    job_name STRING NOT NULL,
    error_code STRING,
    error_message STRING NOT NULL,
    severity STRING,
    details JSON
);