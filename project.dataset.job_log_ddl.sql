-- DDL for project.dataset.job_log
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_da_vda_tk.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_da_vda_tk.ksh

CREATE TABLE IF NOT EXISTS `project.dataset.job_log` (
    job_kennung STRING,
    progname STRING,
    progversion STRING,
    log_level STRING,
    message STRING,
    created_at TIMESTAMP
);