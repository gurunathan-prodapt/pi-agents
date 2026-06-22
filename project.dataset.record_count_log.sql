-- BigQuery DDL for record_count_log table
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_apn_ve.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_apn_ve.ksh

CREATE TABLE IF NOT EXISTS `project.dataset.record_count_log`
(
    job_kennung STRING NOT NULL,
    eintrags_nr STRING NOT NULL,
    tab_name STRING,
    record_count INT64 NOT NULL,
    created_at TIMESTAMP NOT NULL
)
OPTIONS(
    description="Logs the number of records processed by BigQuery stored procedures."
);