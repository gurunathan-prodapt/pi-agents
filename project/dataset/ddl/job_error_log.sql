-- DDL for job_error_log
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vvl_upgrade.ksh
-- This table logs errors encountered during job execution.

CREATE TABLE IF NOT EXISTS project.dataset.job_error_log (
    job_kennung STRING NOT NULL,
    eintrags_nr STRING NOT NULL,
    error_code STRING,
    error_message STRING,
    error_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
);