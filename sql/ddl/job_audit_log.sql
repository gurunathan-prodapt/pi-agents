--
-- BigQuery DDL for job_audit_log table
-- Replaces filesystem-based logging from legacy job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_acc_ref.ksh
--
CREATE TABLE IF NOT EXISTS `project.dataset.job_audit_log` (
    job_name STRING NOT NULL OPTIONS(description="Name of the job executed"),
    job_entry_number STRING NOT NULL OPTIONS(description="Unique identifier for each job execution instance"),
    start_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the job started"),
    end_timestamp TIMESTAMP OPTIONS(description="Timestamp when the job ended"),
    status STRING NOT NULL OPTIONS(description="Overall status of the job (e.g., 'STARTED', 'COMPLETED', 'FAILED')"),
    message STRING OPTIONS(description="General message or description of the job step"),
    parameters JSON OPTIONS(description="JSON object of input parameters used for the job run"),
    insert_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Timestamp when the log entry was inserted")
);