-- BigQuery DDL for sof_ta_barrier_zusgf
-- Referenced in d_ausd_v_ta_vertrag_tmp.sql

CREATE TABLE IF NOT EXISTS `project.dataset.sof_ta_barrier_zusgf`
(
    cntrct_id STRING,
    sperrart_alle STRING,
    sperrgrund_alle STRING,
    stilllegungszeitraum_alle STRING,
    sperrgrund_zusgf INT64,
    -- Add other relevant columns
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);