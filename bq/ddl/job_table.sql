-- BigQuery DDL for job_table
-- Replaces: Implied job control functionality in vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_geschaeftspartner.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_geschaeftspartner.ksh

CREATE TABLE IF NOT EXISTS `project.dataset.job_table`
(
  tab_name STRING NOT NULL OPTIONS(description="Name of the table or process managed"),
  active_flag STRING OPTIONS(description="Indicates if the job is active (e.g., 'A' for Active, 'N' for Not Active)"),
  process_flag STRING OPTIONS(description="Flag indicating processing status (e.g., 'I' for Initial, 'F' for Finished)"),
  from_date DATE OPTIONS(description="Start date of the processing window"),
  to_date DATE OPTIONS(description="End date of the processing window"),
  job_type STRING OPTIONS(description="Type of job (e.g., 'J' for Job)"),
  restart_flag STRING OPTIONS(description="Indicates if a restart is needed (e.g., 'Y', 'N')"),
  record_count INT64 OPTIONS(description="Number of records associated with the job's last run"),
  description STRING OPTIONS(description="Description of the job or process"),
  last_updated_ts TIMESTAMP OPTIONS(description="Timestamp of the last update to this record")
)
OPTIONS(
  description="Manages job status and metadata for various processes."
);