-- BigQuery DDL for job_result_log
-- New table for logging processed record counts, replacing temporary files in legacy ksh.
-- Part of the migration for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vertrag_tmp.ksh

CREATE TABLE IF NOT EXISTS `project.dataset.job_result_log`
(
    log_time        TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    job_kennung     STRING,
    eintrags_nr     STRING,
    target_table    STRING,
    record_count    INT64,
    -- Add other relevant result metrics
    batch_id        STRING
);