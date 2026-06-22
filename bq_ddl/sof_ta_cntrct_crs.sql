--
-- Target BigQuery DDL for Oracle table SOF$TA_CNTRCT_CRS
-- Replaces usage in vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs2.ksh
--
-- NOTE: The exact schema for SOF$TA_CNTRCT_CRS was not provided in the design document.
-- This is a placeholder schema; please update with the actual source table definition.
--
CREATE TABLE IF NOT EXISTS `bq_dataset.sof_ta_cntrct_crs`
(
    `cntrct_id`   STRING      NOT NULL OPTIONS(description="Placeholder contract ID"),
    `crs_code`    STRING              OPTIONS(description="Placeholder CRS code"),
    `start_date`  DATE                OPTIONS(description="Placeholder start date"),
    `end_date`    DATE                OPTIONS(description="Placeholder end date"),
    `load_date`   TIMESTAMP           OPTIONS(description="Placeholder load timestamp")
)
OPTIONS(
    description="Migrated SOF$TA_CNTRCT_CRS table from Oracle"
);