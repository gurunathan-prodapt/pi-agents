--
-- BigQuery DDL for error logging table job_error_log
-- Used by migrated job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount.ksh
--
CREATE TABLE IF NOT EXISTS `project.dataset.job_error_log`
(
    error_id            STRING,
    job_name            STRING,
    error_time          TIMESTAMP,
    error_message       STRING,
    stack_trace         STRING,
    severity            STRING,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);