-- BigQuery DDL for dwh_vi_s_rd_segment
-- Referenced in d_ausd_v_ta_vertrag_tmp.sql. This was a view, but for DDL we treat it as a table.

CREATE TABLE IF NOT EXISTS `project.dataset.dwh_vi_s_rd_segment`
(
    rechdef_id_carmen STRING,
    segment_id INT64,
    -- Add other relevant columns
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);