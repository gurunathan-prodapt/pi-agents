-- BigQuery DDL for job control/metadata table
-- Replaces legacy job tracking mechanisms for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_discount.ksh

CREATE TABLE IF NOT EXISTS `project_id.dataset_id.job_control` (
    job_run_id STRING NOT NULL, -- Unique ID for each job execution
    job_name STRING NOT NULL,
    job_kennung STRING, -- From p_JobKennung parameter
    eintrags_nr STRING, -- From p_EintragsNr parameter
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status STRING NOT NULL, -- e.g., 'RUNNING', 'SUCCEEDED', 'FAILED', 'DEACTIVATED'
    message STRING,
    processed_records INT64,
    is_active BOOLEAN, -- Flag to indicate if job is currently active
    PRIMARY KEY(job_run_id) NOT ENFORCED
)
OPTIONS(
    description = "Table for managing job control and status for data pipelines."
);