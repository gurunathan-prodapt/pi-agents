-- BigQuery table for managing job states.
-- Replaces shell script's internal job control mechanism for job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_apn_ve.ksh
CREATE TABLE IF NOT EXISTS project.dataset.job_table (
    job_kennung STRING NOT NULL,
    eintrags_nr STRING NOT NULL,
    status STRING, -- e.g., 'ACTIVE', 'INACTIVE', 'RUNNING', 'COMPLETED', 'FAILED'
    start_timestamp TIMESTAMP,
    end_timestamp TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);