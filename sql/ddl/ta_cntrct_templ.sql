-- BigQuery DDL for the ta_cntrct_templ table
-- Replaces usage of sof$ta_cntrct_templ in legacy Oracle SQL.
-- Legacy Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_templ.ksh

CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.ta_cntrct_templ` (
    cntrct_template_id INT66,
    cds_description_id INT66,
    cds_description STRING
);