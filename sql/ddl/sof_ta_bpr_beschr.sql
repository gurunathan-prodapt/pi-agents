-- DDL for project.dataset.sof_ta_bpr_beschr
-- Source table for core logic from legacy job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_opt_text.ksh
CREATE TABLE IF NOT EXISTS `project.dataset.sof_ta_bpr_beschr` (
    `BPR_ID` INT64 NOT NULL,
    `PDS_DESCRIPTION` STRING
);