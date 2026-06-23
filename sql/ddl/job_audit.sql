-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_vertrag.ksh
-- Description: DDL for the job audit table in BigQuery.
CREATE TABLE IF NOT EXISTS `project.dataset.job_audit` (
  job_entry_nr INT64 OPTIONS(description="Unique entry number for the job execution, mimicking DW_EintragsNr"),
  job_kennung STRING OPTIONS(description="Identifier for the job, e.g., 'RN_VERTRAG'"),
  status STRING OPTIONS(description="Current status of the job (e.g., 'STARTED', 'OK', 'ERROR')"),
  error_nr INT64 OPTIONS(description="Error number if the job failed (e.g., 192, 193)"),
  error_arg STRING OPTIONS(description="Additional argument for the error, if any"),
  log_ts TIMESTAMP OPTIONS(description="Timestamp of the log entry"),
  message STRING OPTIONS(description="Detailed log message"),
  stichtag STRING OPTIONS(description="Processing date (DDMMYYYY) for the job"),
  sysdate_value STRING OPTIONS(description="System date (DDMMYYYY) when the job was run"),
  restart_value INT64 OPTIONS(description="Restart value (Wiederanlaufwert) used for the job"),
  log_file_name STRING OPTIONS(description="Simulated log file name for audit trail")
)
OPTIONS(
  description="Audit table for tracking BigQuery job executions, replacing legacy file-based logging."
);