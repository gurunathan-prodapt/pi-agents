-- DDL for job_audit_log table
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_msisdn.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_msisdn.ksh

CREATE TABLE IF NOT EXISTS `your_gcp_project.your_bq_dataset.job_audit_log`
(
    job_kennung STRING,
    eintrags_nr STRING,
    stichtag DATE,
    records_processed INT64,
    start_timestamp TIMESTAMP,
    end_timestamp TIMESTAMP,
    status STRING
)
OPTIONS(
    description="Logs audit information for job execution, including record counts"
);