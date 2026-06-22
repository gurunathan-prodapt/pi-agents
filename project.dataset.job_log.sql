-- BigQuery DDL for job_log table
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_apn_ve.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_apn_ve.ksh

CREATE TABLE IF NOT EXISTS `project.dataset.job_log`
(
    job_kennung STRING NOT NULL,
    eintrags_nr STRING NOT NULL,
    tab_name STRING,
    status STRING NOT NULL,
    created_at TIMESTAMP NOT NULL
)
OPTIONS(
    description="Logs status and metadata for BigQuery stored procedure executions."
);