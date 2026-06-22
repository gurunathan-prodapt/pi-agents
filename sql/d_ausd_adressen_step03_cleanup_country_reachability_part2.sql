-- Migration of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_adressen.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_adressen.ksh
-- BigQuery Standard SQL: Step 03 - Part 3 - Cleanup intermediate country and reachability tables.
-- Replaces Oracle Step 03j section from original SQL*Plus script.

TRUNCATE TABLE `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_reachability`;
TRUNCATE TABLE `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_country`;
TRUNCATE TABLE `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_country_desc`;
TRUNCATE TABLE `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_laender_kng`;