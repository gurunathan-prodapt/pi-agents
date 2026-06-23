-- DDL for job_audit table
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vvl_dwh.ksh
CREATE TABLE IF NOT EXISTS project.dataset.job_audit (
    entry_nr INT64 NOT NULL,
    job_kennung STRING NOT NULL,
    status STRING,
    log_name STRING,
    created_at TIMESTAMP
);