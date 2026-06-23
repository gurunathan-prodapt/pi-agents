-- BigQuery DDL for project.dataset.job_tracking
-- Replaces job tracking logic (FOSJobErzeugeEintrag) in vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh

CREATE TABLE IF NOT EXISTS project.dataset.job_tracking (
    track_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    job_name STRING NOT NULL,
    status STRING NOT NULL, -- e.g., 'SUCCESS', 'FAILED', 'STARTED'
    record_count INT64,
    stichtag DATE,
    eintragsnr STRING,
    details JSON
);