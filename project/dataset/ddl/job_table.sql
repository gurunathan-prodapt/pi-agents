-- DDL for project.dataset.job_table
-- Purpose: Job management and status tracking for BigQuery jobs
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_acc_ref.ksh

CREATE TABLE IF NOT EXISTS `project.dataset.job_table`
(
    job_kennung  STRING NOT NULL,
    eintrags_nr  STRING NOT NULL,
    tab_name     STRING NOT NULL,
    status       STRING NOT NULL, -- e.g., 'ACTIVE', 'DEACTIVATED', 'COMPLETED', 'FAILED'
    created_at   TIMESTAMP,
    updated_at   TIMESTAMP,
    PRIMARY KEY (job_kennung, eintrags_nr) NOT ENFORCED
);