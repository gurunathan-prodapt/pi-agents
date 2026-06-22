-- Target for: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_msisdn_his.ksh
-- Description: BigQuery DDL for the job audit logging table.

CREATE TABLE IF NOT EXISTS `project.dataset.dwmsg_job_audit` (
  job_id INT64 NOT NULL OPTIONS(description="Unique identifier for the job run, derived from max(job_id) + 1 per job_name."),
  job_name STRING NOT NULL OPTIONS(description="Identifier for the type of job (e.g., 'AUSD_BP_TA_MSISDN_HIS')."),
  script_name STRING OPTIONS(description="Name of the script or stored procedure executing the job."),
  log_file STRING OPTIONS(description="Virtual log file name for historical reference."),
  stichtag STRING OPTIONS(description="Cutoff date for the job in DDMMYYYY format."),
  status STRING NOT NULL OPTIONS(description="Current status of the job (e.g., 'STARTED', 'COMPLETED', 'FAILED')."),
  error_message STRING OPTIONS(description="Detailed error message if the job failed."),
  created_at TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the audit entry was created.")
)
PARTITION BY
  DATE(created_at)
CLUSTER BY
  job_name, job_id;