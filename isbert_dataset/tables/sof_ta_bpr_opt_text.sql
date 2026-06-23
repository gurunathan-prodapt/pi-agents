--
-- Target table DDL for BigQuery, replacing Oracle table sof$ta_bpr_opt_text.
-- Generated from legacy source: d_ausd_bp_ta_bpr_opt_text.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_opt_text.ksh
--

CREATE TABLE IF NOT EXISTS `isbert_dataset.sof_ta_bpr_opt_text`
(
    CNTRCT_ID       INT64,
    BPR_ID          INT64,
    PDS_DESCRIPTION STRING
);