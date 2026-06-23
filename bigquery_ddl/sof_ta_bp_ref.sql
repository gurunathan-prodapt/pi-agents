-- BigQuery DDL for sof_ta_bp_ref
-- Referenced in d_ausd_v_ta_vertrag_tmp.sql

CREATE TABLE IF NOT EXISTS `project.dataset.sof_ta_bp_ref`
(
    cntrct_cp2_id STRING,
    bp_id STRING,
    -- Add other relevant columns
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);