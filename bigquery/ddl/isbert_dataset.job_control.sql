-- DDL for a job control/logging table
-- Original job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_basis_his.ksh

CREATE TABLE IF NOT EXISTS `your-gcp-project.isbert_dataset.job_control` (
    job_name STRING NOT NULL,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status STRING,
    process_date DATE,
    rows_processed INT64,
    error_message STRING,
    dag_run_id STRING,
    task_id STRING,
    execution_date TIMESTAMP
);