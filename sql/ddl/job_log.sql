-- DDL for BigQuery audit log table
-- Replaces FOSJobErzeugeEintrag functionality.
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh

CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.job_log`
(
    job_name STRING NOT NULL,
    entry_number STRING,
    key_date DATE,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status STRING, -- 'SUCCESS', 'FAILED', 'RUNNING'
    record_count INT64,
    message STRING,
    run_id STRING DEFAULT GENERATE_UUID()
);