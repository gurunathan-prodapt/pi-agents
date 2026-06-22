-- Legacy Source: New table for logging
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_geschaeftspartner.ksh

CREATE TABLE IF NOT EXISTS your_project.your_dataset_logging.job_log (
    timestamp TIMESTAMP OPTIONS(description="Timestamp of the log entry"),
    job_name STRING OPTIONS(description="Name of the job that generated the log"),
    message STRING OPTIONS(description="Log message content"),
    level STRING OPTIONS(description="Log level (e.g., INFO, DEBUG, ERROR)"),
    records_processed INT64 OPTIONS(description="Number of records processed, if applicable")
)
OPTIONS(
    description="Log table for migrated ETL jobs"
);