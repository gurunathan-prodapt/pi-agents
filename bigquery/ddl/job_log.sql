-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_vertrag.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_vertrag.ksh
-- Target: BigQuery DDL for job_log table

CREATE TABLE IF NOT EXISTS `gcp-project-id.bq_dataset_name.job_log` (
    eintragsnr INT64 NOT NULL,
    jobkennung STRING NOT NULL,
    stichtag DATE,
    wiederanlaufwert STRING,
    status STRING,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP
);