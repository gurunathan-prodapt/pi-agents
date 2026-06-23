-- DDL for table DWTK_MELDUNGEN
-- Legacy Source: isbert_schema.dwtk_meldungen (Oracle)
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_vertrag.ksh

CREATE TABLE IF NOT EXISTS my_project.my_dataset.dwtk_meldungen (
    job_kennung STRING NOT NULL,
    timecreated TIMESTAMP NOT NULL,
    -- Add other columns if known from Oracle schema
    -- For this migration, only job_kennung and timecreated are explicitly used
    _metadata JSON -- Placeholder for other potential columns, or for metadata
);