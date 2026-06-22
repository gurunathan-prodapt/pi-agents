--
-- BigQuery DDL for target_dataset.job_table
-- Purpose: Job control and status for job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vertrag_tmp.ksh
--
CREATE SCHEMA IF NOT EXISTS `target_dataset`;

CREATE TABLE IF NOT EXISTS `target_dataset.job_table`
(
    `job_kennung` STRING NOT NULL,
    `eintrags_nr` STRING NOT NULL,
    `active_flag` BOOL NOT NULL,
    `last_run_start_time` TIMESTAMP,
    `last_run_end_time` TIMESTAMP,
    `status` STRING
)
OPTIONS(
    description="Table for managing job active status and run times"
);