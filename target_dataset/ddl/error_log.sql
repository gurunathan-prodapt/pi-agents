--
-- BigQuery DDL for target_dataset.error_log
-- Purpose: Logging errors for job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vertrag_tmp.ksh
--
CREATE SCHEMA IF NOT EXISTS `target_dataset`;

CREATE TABLE IF NOT EXISTS `target_dataset.error_log`
(
    `log_time` TIMESTAMP NOT NULL,
    `job_name` STRING NOT NULL,
    `error_code` STRING,
    `error_message` STRING,
    `severity` STRING
)
OPTIONS(
    description="Table for logging job errors"
);