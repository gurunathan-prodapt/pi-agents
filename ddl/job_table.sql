-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs2.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs2.ksh
-- Description: DDL for the job control table in BigQuery.

CREATE TABLE IF NOT EXISTS project.dataset.job_table (
    job_id STRING NOT NULL OPTIONS(description="Identifier for the job, e.g., 'BERT_DROP_TEMP_TABLE'."),
    entry_number STRING OPTIONS(description="Entry number for specific job runs, e.g., 'f' parameter from ksh."),
    start_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the job started."),
    end_timestamp TIMESTAMP OPTIONS(description="Timestamp when the job ended."),
    status STRING NOT NULL OPTIONS(description="Current status of the job, e.g., 'RUNNING', 'SUCCESS', 'FAILED', 'DEACTIVATED'."),
    record_count INT64 OPTIONS(description="Number of records processed by the job."),
    table_name STRING OPTIONS(description="Name of the main table processed by the job, e.g., 'ta_cntrct_crs2'."),
    error_message STRING OPTIONS(description="Details of any error encountered."),
    last_update_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Timestamp of the last update to this record.")
);