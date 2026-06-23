-- BigQuery DDL for project.dataset.DWTK_MELDUNGEN
-- Replaces legacy table referenced in vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_apn_vertrag.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh

CREATE TABLE IF NOT EXISTS project.dataset.DWTK_MELDUNGEN (
    job_kennung STRING NOT NULL,
    timecreated TIMESTAMP NOT NULL,
    -- Add other columns as per actual legacy schema
    meldung_id STRING,
    meldung_text STRING
);