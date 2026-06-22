-- DDL for job_table
-- Replaces job tracking functionality from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_aurd_rechstan.ksh
CREATE TABLE IF NOT EXISTS `my_project.my_dataset.job_table`
(
    job_id STRING NOT NULL OPTIONS(description="Unique identifier for the job run"),
    entry_number STRING NOT NULL OPTIONS(description="Entry number from the original p_EintragsNr"),
    start_time TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the job started"),
    end_time TIMESTAMP OPTIONS(description="Timestamp when the job ended"),
    reference_date DATE NOT NULL OPTIONS(description="Reference date for the job run (p_Stichtag)"),
    status STRING NOT NULL OPTIONS(description="Status of the job (e.g., 'RUNNING', 'SUCCESS', 'FAILED')"),
    processed_records INT64 OPTIONS(description="Number of records processed by the job"),
    target_table STRING OPTIONS(description="Name of the main target table"),
    error_message STRING OPTIONS(description="Detailed error message if the job failed"),
    restart_value INT64 OPTIONS(description="Restart value from p_wiederanlaufWert")
)
PARTITION BY
    reference_date
OPTIONS(
    description="Table to store metadata and status for ETL jobs, replacing legacy job tracking."
);