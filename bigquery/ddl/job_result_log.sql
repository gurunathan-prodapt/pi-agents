-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_valid.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_valid.ksh

CREATE TABLE IF NOT EXISTS `project.dataset.job_result_log` (
    job_name STRING NOT NULL,
    job_id STRING NOT NULL,
    entry_number STRING NOT NULL,
    run_timestamp TIMESTAMP NOT NULL,
    records_processed INT64,
    result_details STRING
);