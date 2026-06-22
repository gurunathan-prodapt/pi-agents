-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs3.ksh
-- This DDL creates the job control table for managing ETL job states.
-- Please replace `your_project_id` and `your_dataset_id` with your actual BigQuery project and dataset.
CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.job_table` (
    job_name STRING NOT NULL OPTIONS(description="Unique identifier for the job."),
    entry_nr STRING NOT NULL OPTIONS(description="Entry number or instance identifier for the job execution."),
    tab_name STRING NOT NULL OPTIONS(description="Name of the primary table this job operates on."),
    active_flag BOOL NOT NULL DEFAULT FALSE OPTIONS(description="TRUE if the job is currently active, FALSE otherwise."),
    created_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Timestamp when the job record was first created."),
    updated_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Timestamp of the last update to this job record."),
    completed_ts TIMESTAMP OPTIONS(description="Timestamp when the job last completed successfully or failed.")
)
OPTIONS(
    description="Table to manage the state and history of ETL jobs, replacing legacy shell script job management."
);