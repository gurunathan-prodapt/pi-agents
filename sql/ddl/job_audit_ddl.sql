-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_iccid_vertrag.ksh
-- Description: DDL for the job_audit table to log execution of BigQuery stored procedures.

CREATE TABLE IF NOT EXISTS `project.dataset.job_audit` (
    job_nr INT64 NOT NULL,
    job_kennung STRING NOT NULL,
    script_name STRING NOT NULL,
    log_timestamp TIMESTAMP NOT NULL,
    stichtag STRING NOT NULL, -- Stored as string to match DDMMYYYY format expected by original script
    status STRING NOT NULL,
    message STRING
);