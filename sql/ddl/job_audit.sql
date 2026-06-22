-- Legacy Source: N/A (new BigQuery audit table)
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_period.ksh

CREATE TABLE IF NOT EXISTS `project.dataset.job_audit` (
    job_id STRING NOT NULL OPTIONS(description="Identifier for the ETL job"),
    run_id STRING NOT NULL OPTIONS(description="Unique identifier for each job execution"),
    start_time TIMESTAMP OPTIONS(description="Timestamp when the job started"),
    end_time TIMESTAMP OPTIONS(description="Timestamp when the job ended"),
    status STRING OPTIONS(description="Overall status of the job (e.g., 'SUCCESS', 'FAILED')"),
    job_kennung_param STRING OPTIONS(description="JobKennung parameter passed to the script"),
    eintragsnr_param STRING OPTIONS(description="EintragsNr parameter passed to the script"),
    processed_records INT64 OPTIONS(description="Number of records processed by the transformation"),
    error_message STRING OPTIONS(description="Error message if the job failed"),
    log_details JSON OPTIONS(description="Additional details in JSON format")
)
PARTITION BY DATE(start_time)
CLUSTER BY job_id, status
OPTIONS(
    description="Audit table for tracking ETL job executions."
);