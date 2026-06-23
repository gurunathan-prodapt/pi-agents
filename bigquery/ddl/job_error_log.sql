-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_vertrag.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_vertrag.ksh
-- Target: BigQuery DDL for job_error_log table

CREATE TABLE IF NOT EXISTS `gcp-project-id.bq_dataset_name.job_error_log` (
    jobkennung STRING NOT NULL,
    eintragsnr INT64,
    error_message STRING NOT NULL,
    error_stack STRING,
    error_statement STRING,
    created_at TIMESTAMP NOT NULL
);