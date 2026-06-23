-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_iccid_einzeln.ksh
-- Description: DDL for the job logging table.

CREATE TABLE your_project_id.your_dataset_id.job_log (
    job_kennung STRING NOT NULL,
    eintrags_nr INT64 NOT NULL,
    tab_name STRING,
    stichtag DATE,
    records INT64,
    status STRING NOT NULL,
    message STRING,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);