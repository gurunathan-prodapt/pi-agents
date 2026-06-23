-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_vertrag.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_vertrag.ksh
-- Description: DDL for BigQuery job error logging table.
CREATE TABLE IF NOT EXISTS `my_dataset.job_error_log` (
    log_id STRING DEFAULT GENERATE_UUID(),
    job_id STRING NOT NULL,
    run_id STRING,
    error_message STRING,
    error_detail STRING,
    log_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);