-- Legacy Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_disc_zusgf.ksh

CREATE TABLE IF NOT EXISTS `isbert_ds.job_message_log`
(
    job_kennung     STRING,
    eintrags_nr     INT64,
    log_time        TIMESTAMP,
    message         STRING,
    log_level       STRING -- e.g., 'INFO', 'WARN', 'DEBUG'
);