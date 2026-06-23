-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_discount.ksh
-- Description: DDL for the job execution logging table, replacing temporary files and echo commands.
CREATE TABLE IF NOT EXISTS dataset.job_run_log (
    job_kennung STRING NOT NULL,
    eintragsnr STRING NOT NULL,
    tab_name STRING,
    records_count INT64,
    processed_at TIMESTAMP,
    error_message STRING
);