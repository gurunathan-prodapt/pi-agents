-- Legacy Source: r_ausd_bp_ta_bpr_basis.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_basis.ksh

CREATE TABLE IF NOT EXISTS project.dataset.job_log (
    job_id STRING NOT NULL OPTIONS(description="Unique identifier for the job run, often a UUID."),
    job_name STRING NOT NULL OPTIONS(description="Name of the job being executed (e.g., wrapper or kernel procedure)."),
    entry_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp when this log entry was recorded."),
    log_level STRING NOT NULL OPTIONS(description="Severity of the log entry (e.g., 'INFO', 'WARNING', 'ERROR')."),
    message STRING OPTIONS(description="Detailed log message."),
    status STRING OPTIONS(description="Overall status of the job at this entry point (e.g., 'STARTED', 'RUNNING', 'COMPLETED', 'FAILED', 'FAILED_PARAM_VALIDATION')."),
    processing_date DATE OPTIONS(description="The 'Stichtag' (processing date) for which the job was run."),
    restart_value INT64 OPTIONS(description="The 'Wiederanlaufwert' (restart value) used for the job."),
    error_details STRING OPTIONS(description="Contains error message and stack trace if the job failed."),
    kernel_job_entry_nr INT64 OPTIONS(description="The DW_EintragsNr passed to the kernel script for tracking.")
);