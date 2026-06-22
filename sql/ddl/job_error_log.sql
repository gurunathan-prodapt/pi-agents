-- DDL for project.dataset.job_error_log
-- Legacy Source: Logging for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_iccid_einzeln.ksh
CREATE OR REPLACE TABLE `project.dataset.job_error_log` (
    job_kennung STRING,
    eintragsnr STRING,
    stichtag STRING,
    err_nr INT64,
    err_arg STRING,
    created_at TIMESTAMP
);