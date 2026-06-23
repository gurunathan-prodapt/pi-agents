-- BigQuery DDL for the job_control table
-- Used for managing job states, replacing shell-based job control for job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_acc_ref.ksh
CREATE TABLE IF NOT EXISTS `isbert_rpt_staging.job_control`
(
    job_kennung         STRING NOT NULL,
    eintragsnr          STRING NOT NULL,
    status              STRING NOT NULL, -- e.g., 'ACTIVE', 'INACTIVE', 'FAILED'
    last_run_timestamp  TIMESTAMP,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (job_kennung, eintragsnr) NOT ENFORCED
);