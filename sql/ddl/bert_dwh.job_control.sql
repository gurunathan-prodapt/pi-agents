-- Header: BigQuery DDL for job_control table
-- Legacy Source: N/A (new control table)
-- Job: BERT_V_TA_DISC_ZUSGF

CREATE TABLE IF NOT EXISTS `bert_dwh.job_control` (
    job_kennung STRING NOT NULL,
    eintrags_nr INT64 NOT NULL OPTIONS(description="Sequential ID for job execution"),
    job_status STRING NOT NULL OPTIONS(description="e.g., RUNNING, SUCCEEDED, FAILED"),
    start_time TIMESTAMP OPTIONS(description="Job start timestamp"),
    end_time TIMESTAMP OPTIONS(description="Job end timestamp"),
    records_processed INT64 OPTIONS(description="Number of records processed by the job"),
    last_update_time TIMESTAMP OPTIONS(description="Last update timestamp for job status"),
    error_message STRING OPTIONS(description="Error message if job failed"),
    PRIMARY KEY (job_kennung, eintrags_nr) NOT ENFORCED
);