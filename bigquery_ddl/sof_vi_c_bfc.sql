-- BigQuery DDL for sof_vi_c_bfc
-- Referenced in d_ausd_v_ta_vertrag_tmp.sql. This was a view, but for DDL we treat it as a table.

CREATE TABLE IF NOT EXISTS `project.dataset.sof_vi_c_bfc`
(
    cntrct_id STRING,
    bindefrist STRING,
    -- Add other relevant columns
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);