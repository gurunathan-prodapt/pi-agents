--
-- Target BigQuery DDL for table job_log
-- Replaces file-based logging in vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_notice.ksh
--
CREATE OR REPLACE TABLE `project.dataset.job_log`
(
    job_kennung STRING,
    eintrags_nr INT64,
    script_name STRING,
    log_level STRING,
    message STRING,
    created_at TIMESTAMP
);