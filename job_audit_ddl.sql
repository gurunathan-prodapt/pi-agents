-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_msisdn_his.ksh
-- This DDL creates the job_audit table for tracking the execution of the migrated job.

CREATE TABLE IF NOT EXISTS `project.dataset.job_audit` (
    job_id STRING NOT NULL,
    job_name STRING,
    start_timestamp TIMESTAMP NOT NULL,
    end_timestamp TIMESTAMP,
    status STRING NOT NULL, -- e.g., 'RUNNING', 'SUCCESS', 'FAILED'
    parameters JSON,
    message STRING
);