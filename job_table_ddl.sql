-- job_table_ddl.sql
--
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_adressen.ksh (implied job tracking)
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_adressen.ksh
--
-- DDL for the job tracking table used by k_ausd_adressen_control.bq.sql.
-- This table helps in monitoring job execution status and logs.
--
-- Note: Replace `project.` with your actual Google Cloud Project ID.

CREATE TABLE IF NOT EXISTS `project.isbert_schema.job_table` (
  tab_name STRING OPTIONS(description="Logical name of the table or job processed"),
  job_status STRING OPTIONS(description="Current status of the job (e.g., 'RUNNING', 'COMPLETED', 'FAILED')"),
  job_type STRING OPTIONS(description="Type of job (e.g., 'I' for Initialbefuellung)"),
  stichtag STRING OPTIONS(description="Reference date for the job, in original DDMMYYYY format"),
  process_date STRING OPTIONS(description="The actual date of processing (derived from stichtag)"),
  record_state STRING OPTIONS(description="Placeholder for a record state, e.g., 'J'"),
  restart_flag STRING OPTIONS(description="Indicates if the job was a restart ('Y'/'N')"),
  record_count INT64 OPTIONS(description="Number of records processed or affected by the job"),
  description STRING OPTIONS(description="Detailed description or message about the job's status"),
  job_kennung STRING OPTIONS(description="Job identifier from the input parameters"),
  eintrags_nr STRING OPTIONS(description="Entry number from the input parameters"),
  created_at TIMESTAMP OPTIONS(description="Timestamp when the job entry was created"),
  updated_at TIMESTAMP OPTIONS(description="Last timestamp when the job entry was updated")
);