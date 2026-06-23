-- Target table DDL for sof_ta_cntrct_dist, replacing Oracle table.
-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_cntrct_dist.ksh

CREATE OR REPLACE TABLE `project.dataset.sof_ta_cntrct_dist`
(
    CNTRCT_ID STRING NOT NULL OPTIONS(description="Contract Identifier")
    -- Add other columns if they are part of the original Oracle table schema
    -- For example, if there was a date column for partitioning or clustering:
    -- , BUSINESS_DATE DATE OPTIONS(description="Business date of the record")
)
-- Recommended partitioning/clustering based on expected queries
-- PARTITION BY BUSINESS_DATE
-- CLUSTER BY CNTRCT_ID
;