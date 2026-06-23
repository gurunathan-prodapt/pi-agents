--
-- Target code for legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_vvl_upgrade.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vvl_upgrade.ksh
--
-- DDL for the target table `sof_ta_vvl_upgrade`.
-- This table is created based on the structure implied by the INSERT statement in d_ausd_v_ta_vvl_upgrade_sp.
--
CREATE OR REPLACE TABLE `your_gcp_project_id.your_bq_dataset_id.sof_ta_vvl_upgrade` (
    vertrags_id STRING,
    upgradegrund STRING,
    upgradedatum TIMESTAMP
);