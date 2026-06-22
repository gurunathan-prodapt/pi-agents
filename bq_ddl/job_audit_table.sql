--
-- BigQuery DDL for the job_audit table.
-- Replaces logging mechanisms in vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_evn.ksh
--
CREATE TABLE IF NOT EXISTS `project.dataset.job_audit` (
    job_id STRING NOT NULL,
    job_name STRING NOT NULL,
    status STRING NOT NULL,
    stichtag_param STRING,
    restart_value_param INT64,
    stichtag_processed DATE,
    restart_value_processed INT64,
    error_code INT64,
    error_arg STRING,
    message STRING,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
);