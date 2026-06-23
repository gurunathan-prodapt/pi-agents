-- DDL for job_control table
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vvl_dwh.ksh
CREATE TABLE IF NOT EXISTS project.dataset.job_control (
    entry_nr INT64 NOT NULL,
    job_kennung STRING NOT NULL,
    script_name STRING,
    log_name STRING,
    stichtag STRING,
    status STRING,
    created_at TIMESTAMP,
    finished_at TIMESTAMP
);