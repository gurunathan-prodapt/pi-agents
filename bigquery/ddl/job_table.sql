-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_vertrag.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_vertrag.ksh
-- Description: DDL for BigQuery job management table.
CREATE TABLE IF NOT EXISTS `my_dataset.job_table` (
    job_id STRING NOT NULL,
    job_name STRING,
    status STRING, -- e.g., 'ACTIVE', 'INACTIVE', 'RUNNING', 'COMPLETED', 'FAILED'
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    last_update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);