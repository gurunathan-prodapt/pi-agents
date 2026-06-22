-- DDL for project.dataset.target_bp_ta_bpr_opt_text
-- Target table for core logic from legacy job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_opt_text.ksh
CREATE TABLE IF NOT EXISTS `project.dataset.target_bp_ta_bpr_opt_text` (
    `CNTRCT_ID` INT64 NOT NULL,
    `BPR_ID` INT64 NOT NULL,
    `PDS_DESCRIPTION` STRING
);