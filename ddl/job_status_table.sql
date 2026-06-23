-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vvl_upgrade.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vvl_upgrade.ksh
-- Description: BigQuery table to track job execution status.
CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.job_status_table` (
    job_id STRING NOT NULL,
    entry_nr STRING NOT NULL,
    status STRING NOT NULL, -- e.g., 'ACTIVE', 'COMPLETED', 'FAILED', 'INACTIVE'
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    records_processed INT64,
    error_message STRING
);