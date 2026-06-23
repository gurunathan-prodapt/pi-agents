-- BigQuery DDL for sof_ta_period
-- Referenced in d_ausd_v_ta_vertrag_tmp.sql

CREATE TABLE IF NOT EXISTS `project.dataset.sof_ta_period`
(
    period_id INT64,
    number_time_measurement NUMERIC,
    einheit STRING,
    -- Add other relevant columns
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);