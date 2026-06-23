-- BigQuery DDL for sof_ta_cntrct_valid
-- Referenced in d_ausd_v_ta_vertrag_tmp.sql

CREATE TABLE IF NOT EXISTS `project.dataset.sof_ta_cntrct_valid`
(
    cntrct_validity_id INT64,
    first_period_id INT64,
    -- Add other relevant columns
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);