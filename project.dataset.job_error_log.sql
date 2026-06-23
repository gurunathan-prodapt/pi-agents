-- DDL for job_error_log table
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vvl_dwh.ksh
CREATE TABLE IF NOT EXISTS project.dataset.job_error_log (
    job_kennung STRING NOT NULL,
    error_nr INT64,
    error_arg STRING,
    created_at TIMESTAMP
);