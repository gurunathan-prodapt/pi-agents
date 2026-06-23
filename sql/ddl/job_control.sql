--
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_da_vda_tk.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_da_vda_tk.ksh
--
-- Purpose: DDL for the BigQuery table to store control parameters and audit information for job runs.
--
CREATE TABLE IF NOT EXISTS `project.dataset.job_control` (
    job_kennung STRING NOT NULL,        -- Identifier for the job type
    stichtag STRING NOT NULL,           -- The 'Stichtag' (cutoff date) used for the job run in DDMMYYYY format
    sysdate_ddmmyyyy STRING NOT NULL,   -- The system date when the job ran in DDMMYYYY format
    restart_value INT64,                -- The 'Wiederanlaufwert' (restart value) used for the job run
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
OPTIONS (
    description = 'Table to store control parameters and audit information for ETL job runs.'
);