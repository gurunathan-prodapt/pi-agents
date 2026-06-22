-- DDL for project.dataset.job_audit
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_da_vda_tk.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_da_vda_tk.ksh

CREATE TABLE IF NOT EXISTS `project.dataset.job_audit` (
    job_nr BIGINT,
    job_kennung STRING,
    progname STRING,
    progversion STRING,
    stichtag STRING,
    restart_value INT64,
    log_file STRING,
    status STRING,
    created_at TIMESTAMP,
    message STRING
);