-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_instance.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_instance.ksh

CREATE TABLE IF NOT EXISTS `project.dataset.job_log` (
    entry_nr INT64 OPTIONS(description="Unique entry number for the log"),
    job_name STRING OPTIONS(description="Name of the job being executed"),
    script_name STRING OPTIONS(description="Name of the script (e.g., stored procedure)"),
    log_name STRING OPTIONS(description="Specific log identifier, if any"),
    stichtag DATE OPTIONS(description="Processing date (Stichtag) for the job"),
    status STRING OPTIONS(description="Status of the job entry (e.g., 'START', 'SUCCESS', 'FAILURE')"),
    created_at TIMESTAMP OPTIONS(description="Timestamp when the log entry was created"),
    error_message STRING OPTIONS(description="Detailed error message if status is 'FAILURE'")
);