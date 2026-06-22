--
-- Target BigQuery DDL for Oracle table DWTK_MELDUNGEN
-- Replaces usage in vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs2.ksh
--
-- NOTE: The exact schema for DWTK_MELDUNGEN was not provided in the design document.
-- This is a placeholder schema; please update with the actual source table definition.
--
CREATE TABLE IF NOT EXISTS `bq_dataset.dwtk_meldungen`
(
    `id`         STRING      NOT NULL OPTIONS(description="Placeholder ID column"),
    `data_value` STRING              OPTIONS(description="Placeholder data column"),
    `created_at` TIMESTAMP           OPTIONS(description="Placeholder creation timestamp")
)
OPTIONS(
    description="Migrated DWTK_MELDUNGEN table from Oracle"
);