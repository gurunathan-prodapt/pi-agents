-- Legacy Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_disc_zusgf.ksh

CREATE TABLE IF NOT EXISTS `isbert_ds.job_control`
(
    job_kennung     STRING,
    eintrags_nr     INT64,
    start_time      TIMESTAMP,
    end_time        TIMESTAMP,
    status          STRING, -- e.g., 'RUNNING', 'SUCCESS', 'FAILED', 'DEACTIVATED'
    message         STRING,
    PRIMARY KEY (job_kennung, eintrags_nr) NOT ENFORCED
);