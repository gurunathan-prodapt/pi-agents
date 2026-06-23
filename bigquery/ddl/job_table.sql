--
-- DDL for job_table
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_barrier_zusgf.ksh
--
CREATE TABLE IF NOT EXISTS `project.dataset.job_table` (
    job_kennung STRING NOT NULL,
    entry_nr STRING NOT NULL,
    status STRING NOT NULL, -- e.g., 'ACTIVE', 'INACTIVE', 'COMPLETED', 'FAILED'
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (job_kennung, entry_nr) NOT ENFORCED
);