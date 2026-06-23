--
-- DDL for job_error_log table
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_barrier_zusgf.ksh
--
CREATE TABLE IF NOT EXISTS `project.dataset.job_error_log` (
    job_kennung STRING NOT NULL,
    entry_nr STRING NOT NULL,
    error_code STRING,
    error_message STRING,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);