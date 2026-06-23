-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_bcp.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_bcp.ksh

CREATE TABLE `project.dataset.job_log` (
    job_run_id INT64,
    job_name STRING,
    job_kennung STRING,
    log_timestamp TIMESTAMP,
    status STRING,
    error_nr INT64,
    error_arg STRING,
    stichtag STRING,
    wiederanlaufwert INT64,
    message STRING
);