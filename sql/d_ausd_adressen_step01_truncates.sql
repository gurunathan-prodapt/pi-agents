-- Migration of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_adressen.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_adressen.ksh
-- BigQuery Standard SQL: Step 01 - Truncate all temporary and target tables.
-- This ensures a clean slate for each run, mimicking Oracle's TRUNCATE behavior.
-- Replaces part of Oracle Step 01 (PL/SQL DWPA_UTIL_SKRIPT.runstatement calls).

TRUNCATE TABLE `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_gp`;
TRUNCATE TABLE `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_re`;
TRUNCATE TABLE `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_ev`;
TRUNCATE TABLE `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_dn`;
TRUNCATE TABLE `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_gp_nodp`;
TRUNCATE TABLE `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_re_nodp`;
TRUNCATE TABLE `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_ev_nodp`;
TRUNCATE TABLE `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_dn_nodp`;
TRUNCATE TABLE `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_reachability`;
TRUNCATE TABLE `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_business_pt`;
TRUNCATE TABLE `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_country`;
TRUNCATE TABLE `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_country_desc`;
TRUNCATE TABLE `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_laender_kng`;
TRUNCATE TABLE `{{ params.project_id }}.{{ params.target_dataset }}.sof_ta_e_reach_gp`;
TRUNCATE TABLE `{{ params.project_id }}.{{ params.target_dataset }}.sof_ta_e_reach_re`;
TRUNCATE TABLE `{{ params.project_id }}.{{ params.target_dataset }}.sof_ta_e_reach_dn`;
TRUNCATE TABLE `{{ params.project_id }}.{{ params.target_dataset }}.sof_ta_e_reach_ev`;
TRUNCATE TABLE `{{ params.project_id }}.{{ params.target_dataset }}.sof_ta_e_business_gp`;
TRUNCATE TABLE `{{ params.project_id }}.{{ params.target_dataset }}.sof_ta_e_business_re`;
TRUNCATE TABLE `{{ params.project_id }}.{{ params.target_dataset }}.sof_ta_e_business_ev`;
TRUNCATE TABLE `{{ params.project_id }}.{{ params.target_dataset }}.sof_ta_e_business_dn`;
TRUNCATE TABLE `{{ params.project_id }}.{{ params.target_dataset }}.sof_ta_e_regulierer`;