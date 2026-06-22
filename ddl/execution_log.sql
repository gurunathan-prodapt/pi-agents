-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs3.ksh
-- This DDL creates the execution logging table for BigQuery stored procedures.
-- Please replace `your_project_id` and `your_dataset_id` with your actual BigQuery project and dataset.
CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.execution_log` (
    log_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Timestamp of the execution log entry."),
    procedure_name STRING NOT NULL OPTIONS(description="Name of the stored procedure being executed."),
    job_name STRING NOT NULL OPTIONS(description="Job identifier for this execution."),
    entry_nr STRING NOT NULL OPTIONS(description="Entry number or instance identifier for this execution."),
    tab_name STRING NOT NULL OPTIONS(description="Name of the primary table operated on by this execution."),
    records_processed INT64 OPTIONS(description="Number of records processed by the job."),
    status STRING NOT NULL OPTIONS(description="Status of the execution: 'SUCCESS', 'FAILED', 'SKIPPED_ALREADY_ACTIVE'.")
)
OPTIONS(
    description="Table to log details of BigQuery stored procedure executions, including record counts."
);