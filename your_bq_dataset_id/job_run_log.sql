-- Legacy Source: Job tracking from k_ausd_v_ta_vvl_dwh.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vvl_dwh.ksh
CREATE TABLE IF NOT EXISTS `your_gcp_project_id.your_bq_dataset_id.job_run_log` (
    job_kennung STRING NOT NULL,
    eintrags_nr STRING NOT NULL,
    start_timestamp TIMESTAMP NOT NULL,
    end_timestamp TIMESTAMP,
    status STRING,
    processed_records INT64,
    log_details JSON
);