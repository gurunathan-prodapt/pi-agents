--
-- BigQuery DDL for logging table job_log
-- Used by migrated job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount.ksh
--
CREATE TABLE IF NOT EXISTS `project.dataset.job_log`
(
    log_id              STRING,
    job_name            STRING,
    start_time          TIMESTAMP,
    end_time            TIMESTAMP,
    status              STRING,
    message             STRING,
    records_processed   INT64,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);