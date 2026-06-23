-- DDL for target table sof_ta_bpr_opt_text
-- Replaces output from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_opt_text.ksh
-- and vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_bpr_opt_text.sql
--
-- This table stores the processed Basisprodukt (Base Product) options and descriptions.

CREATE TABLE IF NOT EXISTS `project.dataset.sof_ta_bpr_opt_text` (
    cntrct_id INT64 OPTIONS(description="Contract ID"),
    bpr_id INT64 OPTIONS(description="Basisprodukt ID"),
    pds_description STRING OPTIONS(description="Basisprodukt Description")
)
OPTIONS(
    description="Table to store Basisprodukt options and descriptions, derived from sof_ta_bpr_optionen and sof_ta_bpr_beschr."
);