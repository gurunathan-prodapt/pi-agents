--
-- BigQuery DDL for target table sof_ta_barrier_zusgf
-- Replaces: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_barrier_zusgf.ksh
--
-- This script creates the target table for the transformed barrier data.
-- Placeholders for project_id and target_dataset need to be replaced.
--

CREATE TABLE IF NOT EXISTS `{{ project_id }}.{{ target_dataset }}.sof_ta_barrier_zusgf`
(
    cntrct_id                  INT64,
    sperrart_alle              STRING,
    sperrgrund_alle            STRING,
    stilllegungszeitraum_alle  STRING,
    sperrgrund_zusgf           INT64
);