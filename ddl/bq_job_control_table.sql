-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_valid.ksh
-- Purpose: Table for managing job execution status, replacing implicit job table logic.

CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset.job_control` (
    job_run_id STRING NOT NULL OPTIONS(description="Unique identifier for each job execution instance"),
    job_name STRING NOT NULL OPTIONS(description="Name of the job, e.g., 'k_ausd_v_ta_cntrct_valid'"),
    job_kennung STRING OPTIONS(description="Job identifier from input parameter -j"),
    eintrags_nr STRING OPTIONS(description="Entry number from input parameter -f"),
    start_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the job execution started"),
    end_timestamp TIMESTAMP OPTIONS(description="Timestamp when the job execution ended"),
    status STRING NOT NULL OPTIONS(description="Current status of the job (e.g., 'ACTIVE', 'COMPLETED', 'FAILED', 'IGNORED')"),
    message STRING OPTIONS(description="Additional messages or notes about the job status"),
    records_processed INT64 OPTIONS(description="Number of records processed by the job"),
    process_id INT64 OPTIONS(description="Emulation of shell PID (for active job identification)")
)
PARTITION BY DATE(start_timestamp)
CLUSTER BY job_name, status;