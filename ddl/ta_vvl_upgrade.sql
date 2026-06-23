-- Legacy Source: N/A (Target table updated by d_ausd_v_ta_vvl_upgrade.sql)
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vvl_upgrade.ksh
--
-- DDL for the target data table ta_vvl_upgrade in BigQuery.
-- This table will store the results of the migrated SQL logic.
--
-- Note: The exact schema might need refinement based on the full migration
--       of d_ausd_v_ta_vvl_upgrade.sql, but these columns are inferred
--       from the INSERT statement in the original SQL.

CREATE TABLE IF NOT EXISTS `project.dataset.ta_vvl_upgrade` (
    vertrags_id STRING,
    upgradegrund STRING,
    upgradedatum DATE
);