-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_beschr.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_beschr.ksh
-- Description: Table for detailed logging of job execution steps.
CREATE TABLE IF NOT EXISTS project.dataset.job_log (
    job_id STRING NOT NULL OPTIONS(description="Unique identifier for the job run"),
    log_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp of the log entry"),
    log_level STRING OPTIONS(description="Level of the log entry (e.g., 'INFO', 'WARN', 'ERROR')"),
    message STRING NOT NULL OPTIONS(description="Detailed log message"),
    PRIMARY KEY(job_id, log_timestamp) NOT ENFORCED
);