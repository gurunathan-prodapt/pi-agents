-- DDL for project.dataset.job_run_result
-- Legacy Source: Logging for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_iccid_einzeln.ksh
CREATE OR REPLACE TABLE `project.dataset.job_run_result` (
    job_kennung STRING,
    eintragsnr STRING,
    stichtag STRING,
    tab_name STRING,
    datum_heute DATE,
    datum_gestern DATE,
    restart_value INT64,
    record_count STRING,
    created_at TIMESTAMP
);