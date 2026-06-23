-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_valid.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_valid.ksh

CREATE TABLE IF NOT EXISTS `project.dataset.job_table` (
    job_name STRING NOT NULL,
    job_id STRING NOT NULL,
    entry_number STRING NOT NULL,
    start_timestamp TIMESTAMP,
    end_timestamp TIMESTAMP,
    status STRING, -- e.g., 'RUNNING', 'SUCCESS', 'FAILED', 'ACTIVE', 'INACTIVE'
    message STRING,
    last_update_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (job_id, entry_number) NOT ENFORCED
);