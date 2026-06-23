-- BigQuery SQL for core data transformation logic
-- Replaces d_ausd_bp_ta_cntrct_dist.sql
-- Legacy Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_cntrct_dist.ksh

-- This script is designed to be executed within a BigQuery Stored Procedure.
-- It expects a TRUNCATE to be handled by the calling procedure if needed.

-- Insert distinct contract IDs from the basis table into the contract distribution table.
INSERT INTO `project.dataset.sof_ta_cntrct_dist`
(cntrct_id)
SELECT
    DISTINCT cntrct_id
FROM
    `project.dataset.sof_ta_bpr_basis`;