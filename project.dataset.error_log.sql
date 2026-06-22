-- BigQuery DDL for error_log table
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_apn_ve.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_apn_ve.ksh

CREATE TABLE IF NOT EXISTS `project.dataset.error_log`
(
    error_source STRING NOT NULL,
    error_type STRING NOT NULL,
    error_number INT64 NOT NULL,
    error_argument STRING,
    created_at TIMESTAMP NOT NULL
)
OPTIONS(
    description="Logs error messages from BigQuery stored procedures."
);