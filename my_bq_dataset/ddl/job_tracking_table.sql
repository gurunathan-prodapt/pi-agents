-- DDL for job_tracking_table
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_cntrct_dist.ksh
CREATE TABLE IF NOT EXISTS `my_gcp_project.my_bq_dataset.job_tracking_table` (
    job_id STRING NOT NULL OPTIONS(description="Corresponds to Jobkennung"),
    entry_number STRING NOT NULL OPTIONS(description="Corresponds to EintragsNr"),
    key_date DATE NOT NULL OPTIONS(description="Corresponds to Stichtag"),
    record_count INT64 OPTIONS(description="Number of records processed by the core SQL script"),
    status STRING OPTIONS(description="Status of the job execution (e.g., SUCCESS, FAILED)"),
    start_timestamp TIMESTAMP OPTIONS(description="Timestamp when the job started"),
    end_timestamp TIMESTAMP OPTIONS(description="Timestamp when the job ended"),
    error_message STRING OPTIONS(description="Details of any error encountered")
);