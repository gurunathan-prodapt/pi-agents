-- BigQuery DDL for the table dwtk_meldungen
-- Replaces Oracle table isbert_schema.dwtk_meldungen
-- Legacy Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_da_vda_tk.ksh

CREATE TABLE IF NOT EXISTS my_project.my_dataset.dwtk_meldungen
(
    job_kennung STRING,
    timecreated TIMESTAMP,
    message STRING,
    -- Add other columns from the original Oracle table as needed
    -- For example:
    -- some_other_id INT64,
    -- some_other_data STRING
);