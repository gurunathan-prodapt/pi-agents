-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_discount.ksh
-- Description: DDL for the job control table, replacing implicit job management in the legacy system.
CREATE TABLE IF NOT EXISTS dataset.job_table (
    job_kennung STRING NOT NULL,
    eintragsnr STRING NOT NULL,
    active_flag BOOL NOT NULL,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);