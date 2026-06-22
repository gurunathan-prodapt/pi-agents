-- DDL for job_table
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vvl_upgrade.ksh
-- This table manages the status of active jobs, replacing the logic in h_alis_sqlplus.ksh's starteSQLSkript.

CREATE TABLE IF NOT EXISTS project.dataset.job_table (
    job_kennung STRING NOT NULL,
    eintrags_nr STRING NOT NULL,
    job_status STRING NOT NULL, -- e.g., 'ACTIVE', 'INACTIVE'
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (job_kennung, eintrags_nr) NOT ENFORCED
);