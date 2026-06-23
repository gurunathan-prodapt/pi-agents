-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_discount_rr.ksh
-- BigQuery DDL for configuration table.
CREATE TABLE IF NOT EXISTS `your_project.your_dataset.configuration_table` (
    config_key STRING NOT NULL PRIMARY KEY,
    config_value STRING
);