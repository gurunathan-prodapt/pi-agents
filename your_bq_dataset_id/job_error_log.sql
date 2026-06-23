-- Legacy Source: Error logging from k_ausd_v_ta_vvl_dwh.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vvl_dwh.ksh
CREATE TABLE IF NOT EXISTS `your_gcp_project_id.your_bq_dataset_id.job_error_log` (
    job_kennung STRING NOT NULL,
    eintrags_nr STRING NOT NULL,
    error_timestamp TIMESTAMP NOT NULL,
    error_message STRING,
    error_code STRING,
    severity STRING
);