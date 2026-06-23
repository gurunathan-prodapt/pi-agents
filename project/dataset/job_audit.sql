--
-- BigQuery table for logging job execution status
-- Replaces logging functionality from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_def.ksh
--
CREATE TABLE IF NOT EXISTS `project.dataset.job_audit` (
    job_id STRING NOT NULL OPTIONS(description="Unique identifier for the job run, typically a combination of job key and entry number."),
    job_key STRING NOT NULL OPTIONS(description="Key identifier for the job, e.g., BERT_V_TA_INV_DEF."),
    job_name STRING OPTIONS(description="Descriptive name of the job."),
    job_version STRING OPTIONS(description="Version of the job."),
    entry_number STRING NOT NULL OPTIONS(description="Unique entry number for a specific job run."),
    start_time TIMESTAMP OPTIONS(description="Timestamp when the job started."),
    end_time TIMESTAMP OPTIONS(description="Timestamp when the job ended."),
    status STRING OPTIONS(description="Status of the job (e.g., 'RUNNING', 'SUCCESS', 'FAILED')."),
    message STRING OPTIONS(description="General message or description of the job status."),
    stichtag_info DATE OPTIONS(description="Key date information for the job run."),
    parameters JSON OPTIONS(description="JSON object containing input parameters for the job run.")
)
PARTITION BY DATE(start_time)
CLUSTER BY job_key, status
OPTIONS(
    description="Audit table for tracking the execution status and metadata of BigQuery jobs."
);