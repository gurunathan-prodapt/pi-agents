--
-- Target BigQuery DDL for Oracle table VIA
-- Replaces usage in vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs2.ksh
--
-- NOTE: The exact schema for VIA was not provided in the design document.
-- This is a placeholder schema; please update with the actual target table definition.
--
CREATE TABLE IF NOT EXISTS `bq_dataset.via`
(
    `entry_id`    STRING      NOT NULL OPTIONS(description="Placeholder entry ID"),
    `message`     STRING              OPTIONS(description="Placeholder message content"),
    `log_time`    TIMESTAMP           OPTIONS(description="Placeholder log timestamp")
)
OPTIONS(
    description="Migrated VIA table from Oracle"
);