-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_beschr.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_beschr.ksh
--
-- Placeholder DDL for the target table where the core transformation logic
-- (from d_ausd_bp_ta_bpr_beschr_core.sql) writes its primary output.
-- The actual schema must be defined based on the complete analysis and migration
-- of the original 'd_ausd_bp_ta_bpr_beschr.sql'.

CREATE TABLE IF NOT EXISTS `<project_id>.<dataset>.target_result_table` (
    -- TODO: Define actual columns here based on the output of the original d_ausd_bp_ta_bpr_beschr.sql.
    -- Example placeholder columns:
    id STRING OPTIONS(description="Unique identifier for the record."),
    value STRING OPTIONS(description="Example data value."),
    _DATA_DATE DATE NOT NULL OPTIONS(description="Partitioning column, typically matches Stichtag from orchestration procedure. Used for efficient querying and record counting.")
)
PARTITION BY _DATA_DATE -- Recommended for date-based batch processing
CLUSTER BY id           -- Example: Cluster by common query access pattern
OPTIONS(
    description="Target table for the output of d_ausd_bp_ta_bpr_beschr_core, used for record counting."
);