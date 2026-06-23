-- DDL for BigQuery table job_control_table
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_bp_ref.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_bp_ref.ksh

CREATE TABLE IF NOT EXISTS `my_project.my_dataset.job_control_table` (
    job_kennung STRING NOT NULL,
    entry_nr INT64 NOT NULL,
    status STRING, -- e.g., 'RUNNING', 'SUCCESS', 'FAILED', 'DEACTIVATED'
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    records_processed INT64,
    error_message STRING
);