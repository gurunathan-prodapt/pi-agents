-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vvl_upgrade.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vvl_upgrade.ksh
-- Description: BigQuery table to store job execution logs.
CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.job_log` (
    log_time TIMESTAMP NOT NULL,
    job_id STRING,
    entry_nr STRING,
    log_level STRING, -- e.g., 'INFO', 'WARN', 'ERROR'
    message STRING,
    error_details STRING
);