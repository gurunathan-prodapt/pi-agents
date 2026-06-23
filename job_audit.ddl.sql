-- Target: BigQuery DDL
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_basis_his.ksh
-- Description: DDL for the BigQuery job audit table, replacing shell script's FOSJobErzeugeEintrag functionality.

CREATE TABLE IF NOT EXISTS `project.dataset.job_audit` (
    job_id STRING NOT NULL OPTIONS(description="Identifier for the job."),
    run_id STRING OPTIONS(description="Unique identifier for the specific execution run of the job."),
    start_timestamp TIMESTAMP NOT NULL OPTIONS(description="When the job execution started."),
    end_timestamp TIMESTAMP OPTIONS(description="When the job execution ended."),
    status STRING NOT NULL OPTIONS(description="Final status of the job (e.g., 'SUCCESS', 'FAILED', 'RUNNING')."),
    input_params JSON OPTIONS(description="JSON object containing input parameters for the job run."),
    records_processed INT64 OPTIONS(description="Number of records processed or affected by the job."),
    notes STRING OPTIONS(description="Additional notes or messages regarding the job execution.")
)
PARTITION BY DATE(start_timestamp)
CLUSTER BY job_id
OPTIONS(
    description="Table to store audit information for BigQuery job executions."
);