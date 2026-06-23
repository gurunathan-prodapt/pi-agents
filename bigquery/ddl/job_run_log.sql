--
-- DDL for job_run_log table
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_barrier_zusgf.ksh
--
CREATE TABLE IF NOT EXISTS `project.dataset.job_run_log` (
    job_kennung STRING NOT NULL,
    entry_nr STRING NOT NULL,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    record_count INT64,
    status STRING, -- 'SUCCESS', 'FAILED'
    PRIMARY KEY (job_kennung, entry_nr, start_time) NOT ENFORCED
);