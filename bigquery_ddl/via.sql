-- BigQuery DDL for VIA
-- Replaces usage in legacy job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vertrag_tmp.ksh
-- This table is listed as a target, but its schema is not explicitly defined in the provided SQL.
-- A placeholder DDL is created. Schema verification and completion are required.

CREATE TABLE IF NOT EXISTS `project.dataset.via`
(
    id     STRING,
    data   STRING,
    -- TODO: Add actual columns for VIA table based on its usage in other SQL files.
    -- This is a placeholder; schema needs to be confirmed.
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);