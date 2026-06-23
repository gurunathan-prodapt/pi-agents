-- BigQuery DDL for SOF_TA_CNTRCT_DIST
-- Replaces Oracle table SOF$TA_CNTRCT_DIST
-- Legacy Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_cntrct_dist.ksh

CREATE TABLE IF NOT EXISTS `project.dataset.sof_ta_cntrct_dist`
(
    cntrct_id INT64 OPTIONS(description="Contract ID, derived from SOF_TA_BPR_BASIS")
)
OPTIONS(
    description="Stores distinct contract IDs for distribution analysis."
);