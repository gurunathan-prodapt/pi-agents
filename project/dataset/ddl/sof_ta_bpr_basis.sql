-- Source table DDL for sof_ta_bpr_basis, replacing Oracle table.
-- This DDL is a placeholder. The actual schema should reflect the source Oracle table.
-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_cntrct_dist.ksh

CREATE OR REPLACE TABLE `project.dataset.sof_ta_bpr_basis`
(
    CNTRCT_ID STRING NOT NULL OPTIONS(description="Contract Identifier")
    -- Add other columns from the source Oracle table schema as needed
    -- For example:
    -- , PRODUCT_ID STRING
    -- , EFFECTIVE_DATE DATE
    -- , END_DATE DATE
)
-- Recommended partitioning/clustering based on expected queries and source data volume
-- PARTITION BY EFFECTIVE_DATE
-- CLUSTER BY CNTRCT_ID
;