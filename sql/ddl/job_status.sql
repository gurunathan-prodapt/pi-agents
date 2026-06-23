--
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_da_vda_tk.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_da_vda_tk.ksh
--
-- Purpose: DDL for the BigQuery table to track the current status of each job run.
--
CREATE TABLE IF NOT EXISTS `project.dataset.job_status` (
    job_kennung STRING NOT NULL,    -- Identifier for the job type
    job_entry_nr INT64 NOT NULL,    -- Unique identifier for a specific job run
    status STRING NOT NULL,         -- e.g., 'RUNNING', 'SUCCESS', 'FAILED'
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
OPTIONS (
    description = 'Table to track the current status of each job run.'
);