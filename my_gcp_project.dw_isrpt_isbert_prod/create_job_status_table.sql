-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vertrag_tmp.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vertrag_tmp.ksh

-- Create table to track the overall status (OK/ERR) of job runs.
CREATE TABLE IF NOT EXISTS `my_gcp_project.dw_isrpt_isbert_prod.job_status`
(
    job_run_id STRING NOT NULL OPTIONS(description="Foreign key to job_registry.job_run_id"),
    job_kennung STRING NOT NULL OPTIONS(description="Identifier for the job type (JobKennung)"),
    status_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the status was last updated"),
    current_status STRING NOT NULL OPTIONS(description="Current status of the job (RUNNING, OK, ERR)"),
    last_update_message STRING OPTIONS(description="Last message associated with the status update"),
    error_code INT OPTIONS(description="Last error code encountered"),
    error_message STRING OPTIONS(description="Last error message encountered")
)
PARTITION BY
    DATE_TRUNC(status_timestamp, DAY)
CLUSTER BY
    job_run_id
OPTIONS(
    description = "Table tracking the latest status of each job execution."
);