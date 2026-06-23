-- BigQuery DDL for sof_ta_apn_ve
-- Referenced in d_ausd_v_ta_vertrag_tmp.sql

CREATE TABLE IF NOT EXISTS `project.dataset.sof_ta_apn_ve`
(
    cntrct_id STRING,
    access_point_name STRING,
    -- Add other relevant columns
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);