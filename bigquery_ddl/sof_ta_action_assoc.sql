-- BigQuery DDL for sof_ta_action_assoc
-- Referenced in d_ausd_v_ta_vertrag_tmp.sql

CREATE TABLE IF NOT EXISTS `project.dataset.sof_ta_action_assoc`
(
    cntrct_id STRING,
    rv_action_id INT64,
    -- Add other relevant columns
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);