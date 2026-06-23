-- BigQuery DDL for job_log table
-- Replaces logging functionality from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_action_assoc.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_action_assoc.ksh

CREATE TABLE IF NOT EXISTS `your_project.your_dataset.job_log` (
  entry_no INT64 NOT NULL OPTIONS(description="Unique entry number for the job run"),
  job_kennung STRING NOT NULL OPTIONS(description="Identifier for the job, e.g., BERT_V_TA_ACTION_ASSOC"),
  program_name STRING OPTIONS(description="Name of the program or script"),
  program_version STRING OPTIONS(description="Version of the program or script"),
  log_name STRING OPTIONS(description="Logical log file name (e.g., job_kennung_entry_no.log)"),
  status STRING NOT NULL OPTIONS(description="Status of the job run (e.g., STARTED, OK, ERROR)"),
  stichtag STRING OPTIONS(description="Reference date for the job run in DDMMYYYY format"),
  created_ts TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the log entry was created")
);