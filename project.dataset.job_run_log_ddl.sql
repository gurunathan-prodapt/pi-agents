-- DDL for job_run_log
-- Legacy job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_inv_def.ksh

CREATE TABLE IF NOT EXISTS `project_id.dataset_id.job_run_log` (
    run_id STRING OPTIONS(description="Unique identifier for each job run"), -- Added run_id for better logging
    start_timestamp TIMESTAMP NOT NULL,
    end_timestamp TIMESTAMP NOT NULL,
    job_kennung STRING NOT NULL,
    eintrags_nr STRING NOT NULL,
    records_processed INT64,
    status STRING, -- e.g., 'SUCCESS', 'FAILED', 'VALIDATION_FAILED'
    PRIMARY KEY (run_id) NOT ENFORCED
);