--
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_da_vda_tk.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_da_vda_tk.ksh
--
-- Purpose: DDL for the BigQuery table to centralize logging for job execution, errors, and informational messages.
--
CREATE TABLE IF NOT EXISTS `project.dataset.job_log` (
    job_name STRING NOT NULL,
    job_version STRING,
    job_kennung STRING NOT NULL,
    log_level STRING NOT NULL,   -- e.g., 'INFO', 'WARNING', 'ERROR'
    log_message STRING,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
OPTIONS (
    description = 'Centralized logging table for ETL job execution details.'
);