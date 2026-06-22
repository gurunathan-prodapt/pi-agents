-- Legacy Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_disc_zusgf.ksh

CREATE TABLE IF NOT EXISTS `isbert_ds.error_log`
(
    job_kennung     STRING,
    eintrags_nr     INT64,
    log_time        TIMESTAMP,
    error_message   STRING,
    stack_trace     STRING
);