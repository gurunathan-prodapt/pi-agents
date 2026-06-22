--
-- BigQuery DDL for target_dataset.job_run_log
-- Purpose: Logging job run details for job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vertrag_tmp.ksh
--
CREATE SCHEMA IF NOT EXISTS `target_dataset`;

CREATE TABLE IF NOT EXISTS `target_dataset.job_run_log`
(
    `log_time` TIMESTAMP NOT NULL,
    `job_kennung` STRING NOT NULL,
    `eintrags_nr` STRING NOT NULL,
    `status` STRING NOT NULL,
    `records_processed` INT64,
    `start_time` TIMESTAMP,
    `end_time` TIMESTAMP
)
OPTIONS(
    description="Table for logging job run details and metrics"
);