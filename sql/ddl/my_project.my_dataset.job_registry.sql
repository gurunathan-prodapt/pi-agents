-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_msisdn_his.ksh
-- BigQuery DDL for job_registry table.
CREATE TABLE my_project.my_dataset.job_registry (
    job_entry_nr INT64 NOT NULL,
    job_name STRING NOT NULL,
    script_name STRING NOT NULL,
    stichtag DATE NOT NULL,
    created_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP,
    status STRING NOT NULL
);