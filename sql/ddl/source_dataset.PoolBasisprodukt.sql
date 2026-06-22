-- DDL for source_dataset.PoolBasisprodukt
-- Replaces data source for legacy job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_opt_text.ksh
CREATE TABLE IF NOT EXISTS `project.source_dataset.PoolBasisprodukt` (
    `ID` STRING,
    `NAME` STRING,
    `DESCRIPTION` STRING,
    `CREATE_DATE` DATE,
    `UPDATE_DATE` DATE
);