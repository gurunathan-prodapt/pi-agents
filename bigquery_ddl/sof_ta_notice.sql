-- BigQuery DDL for sof_ta_notice
-- Referenced in d_ausd_v_ta_vertrag_tmp.sql

CREATE TABLE IF NOT EXISTS `project.dataset.sof_ta_notice`
(
    cntrct_id STRING,
    valid_from DATE,
    entry_date_of_notice DATE,
    -- Add other relevant columns
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);