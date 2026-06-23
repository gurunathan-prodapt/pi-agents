-- BigQuery DDL for sof_ta_cntrct_templ
-- Referenced in d_ausd_v_ta_vertrag_tmp.sql

CREATE TABLE IF NOT EXISTS `project.dataset.sof_ta_cntrct_templ`
(
    cntrct_template_id INT64,
    cds_description STRING,
    -- Add other relevant columns
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);