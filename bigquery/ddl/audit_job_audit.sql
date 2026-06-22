-- DDL for job_audit table
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_basis_his.ksh

CREATE OR REPLACE TABLE `project.audit.job_audit`
(
    audit_timestamp TIMESTAMP,
    job_name STRING,
    job_id STRING,
    status STRING, -- e.g., 'STARTED', 'COMPLETED', 'FAILED'
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_seconds INT64,
    processed_records INT64,
    parameters JSON,
    audited_by STRING
);