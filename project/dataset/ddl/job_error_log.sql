-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_apn_ve.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_apn_ve.ksh
CREATE TABLE IF NOT EXISTS `project.dataset.job_error_log` (
    job_kennung STRING,
    eintrags_nr STRING,
    error_nr INT64,
    error_arg STRING,
    error_ts TIMESTAMP
);