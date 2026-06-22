--
-- Target BigQuery DDL for Oracle table SOF$TA_CNTRCT_CRS2
-- Replaces usage in vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs2.ksh
--
-- NOTE: The exact schema for SOF$TA_CNTRCT_CRS2 was not provided in the design document.
-- This is a placeholder schema; please update with the actual target table definition.
--
CREATE TABLE IF NOT EXISTS `bq_dataset.sof_ta_cntrct_crs2`
(
    `cntrct_id`      STRING       NOT NULL OPTIONS(description="Placeholder contract ID"),
    `crs_code_new`   STRING               OPTIONS(description="Placeholder new CRS code"),
    `status`         STRING               OPTIONS(description="Placeholder status"),
    `processed_date` TIMESTAMP            OPTIONS(description="Placeholder processed timestamp")
)
OPTIONS(
    description="Migrated SOF$TA_CNTRCT_CRS2 table from Oracle"
);