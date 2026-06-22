-- Migration of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_adressen.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_adressen.ksh
-- BigQuery Standard SQL: Step 04 - Part 8 - Cleanup intermediate tables for service users and main business partner.
-- Replaces Oracle Step 04i section from original SQL*Plus script.

TRUNCATE TABLE `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_business_pt`;
TRUNCATE TABLE `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_dn_nodp`;
TRUNCATE TABLE `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_dn`;