-- DDL for job_messages table
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vvl_dwh.ksh
CREATE TABLE IF NOT EXISTS project.dataset.job_messages (
    entry_nr INT64 NOT NULL,
    job_kennung STRING NOT NULL,
    message_text STRING,
    message_type STRING,
    created_at TIMESTAMP
);