-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_vertrag.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_vertrag.ksh
-- Description: DDL for BigQuery job run logging table.
CREATE TABLE IF NOT EXISTS `my_dataset.job_run_log` (
    run_id STRING DEFAULT GENERATE_UUID(),
    job_id STRING NOT NULL,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status STRING, -- e.g., 'SUCCESS', 'FAILURE', 'RUNNING'
    records_processed INT64,
    message STRING,
    log_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);