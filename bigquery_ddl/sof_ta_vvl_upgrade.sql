-- BigQuery DDL for sof_ta_vvl_upgrade
-- Referenced in d_ausd_v_ta_vertrag_tmp.sql

CREATE TABLE IF NOT EXISTS `project.dataset.sof_ta_vvl_upgrade`
(
    vertrags_id STRING,
    upgradegatum DATE,
    upgradegrund STRING,
    -- Add other relevant columns
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);