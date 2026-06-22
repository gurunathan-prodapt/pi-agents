-- Migration of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_adressen.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_adressen.ksh
-- BigQuery Standard SQL: Step 04 - Part 2 - Cleanup intermediate tables for contract partners.
-- Replaces Oracle Step 04c section from original SQL*Plus script.

TRUNCATE TABLE `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_gp_nodp`;
TRUNCATE TABLE `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_gp`;