-- Original Script: all_types_graph.ksh
-- UC4 Job: Not specified in source (invokes all_types_graph)

-- Parameters
DECLARE CCR_AI_CUBE_CODE STRING DEFAULT 'SPOS_FACTS'; -- NOTE: parameter accepted for interface compatibility; unused in original script logic
DECLARE CCR_AI_DAT_FILE_DIR STRING DEFAULT NULL; -- NOTE: parameter accepted for interface compatibility; unused in original script logic

-- Note on Error Handling:
-- The original script aborted on failure. This SQL script relies on BigQuery's
-- default statement-level abort behavior to stop execution upon error.

-- Step 1: Declare and assign the cutoff date (Tuesday of current ISO week)
DECLARE v_tuesday_cutoff DATE;
SET v_tuesday_cutoff = DATE_ADD(DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)), INTERVAL 1 DAY);

-- Step 2: Extract active and visible teams into a temporary lookup table
CREATE OR REPLACE TEMP TABLE lkp_teamvirt_ccos AS
SELECT DISTINCT 
  vir.stichtag, 
  tea.sdm_team_id
FROM `{{project_id}}.dataset.ccr_ta_f_teamsichtbarkeit` vir
INNER JOIN `{{project_id}}.dataset.ccr_ta_s_sdm_team` tea 
  ON tea.sdm_team_id = vir.sdm_team_id
INNER JOIN `{{project_id}}.dataset.ccr_ta_s_sdm_abteilung` abt 
  ON abt.sdm_abteilung_id = tea.sdm_abteilung_id
WHERE vir.team_sichtbarkeitstyp_id = 10
  AND (vir.UNSICHTBAR_FLAG = 0 OR abt.ABT_EXTERN = 1);

-- Step 3: Populate weekly cancellations dataset
CREATE OR REPLACE TABLE `{{project_id}}.dataset.tos_cancellations_wk` AS
SELECT 
  stichtag,
  kkm_kampagne_id,
  tos_offer_folder_id,
  tos_offer_id,
  tos_rank,
  vo_kenn,
  vt_segment_id,
  kd_segment_id,
  sdm_team_id,
  kkm_medium_id,
  kkm_richtung_id,
  mea_1 AS anzahl_stornos,
  vt_bic_segment_endntz_id,
  vt_bic_segment_entsch_id,
  one_segment_id
FROM `{{project_id}}.dataset.x_tos_measures`
WHERE tos_mea_group_name = 'CANCELLATIONS'
  AND stichtag < v_tuesday_cutoff;

-- Step 4: Populate standard cancellations dataset with masked team visibility
CREATE OR REPLACE TABLE `{{project_id}}.dataset.tos_cancellations` AS
SELECT 
  m.stichtag,
  m.kkm_kampagne_id,
  m.tos_offer_folder_id,
  m.tos_offer_id,
  m.tos_rank,
  m.vo_kenn,
  m.vt_segment_id,
  m.kd_segment_id,
  CASE WHEN m.sdm_team_id IS NOT NULL THEN lkp.sdm_team_id ELSE NULL END AS sdm_team_id,
  m.kkm_medium_id,
  m.kkm_richtung_id,
  m.mea_1 AS anzahl_stornos,
  m.vt_bic_segment_endntz_id,
  m.vt_bic_segment_entsch_id,
  m.one_segment_id
FROM `{{project_id}}.dataset.x_tos_measures` m
LEFT JOIN lkp_teamvirt_ccos lkp 
  ON lkp.stichtag = m.stichtag AND lkp.sdm_team_id = m.sdm_team_id
WHERE m.tos_mea_group_name = 'CANCELLATIONS';

-- Step 5: Populate weekly products dataset with concatenated offer product IDs
CREATE OR REPLACE TABLE `{{project_id}}.dataset.tos_products_wk` AS
SELECT 
  stichtag,
  kkm_kampagne_id,
  tos_offer_folder_id,
  tos_offer_id,
  kampagnen_kanal_id,
  kkm_kamp_exectype_id,
  tcn_product_id,
  CONCAT(TRIM(CAST(tos_offer_id AS STRING)), '~', TRIM(CAST(tcn_product_id AS STRING))) AS tcn_offer_product_id,
  tos_offer_status_id,
  tos_response_level_id,
  tos_rank,
  vo_kenn,
  vt_segment_id,
  kd_segment_id,
  sdm_team_id,
  kkm_medium_id,
  kkm_richtung_id,
  mea_1 AS anzahl_produkte,
  vt_bic_segment_endntz_id,
  vt_bic_segment_entsch_id,
  one_segment_id
FROM `{{project_id}}.dataset.x_tos_measures`
WHERE tos_mea_group_name = 'PRODUCTS'
  AND stichtag < v_tuesday_cutoff;

-- Step 6: Populate standard products dataset with masked team visibility
CREATE OR REPLACE TABLE `{{project_id}}.dataset.tos_products` AS
SELECT 
  m.stichtag,
  m.kkm_kampagne_id,
  m.tos_offer_folder_id,
  m.tos_offer_id,
  m.kampagnen_kanal_id,
  m.kkm_kamp_exectype_id,
  m.tcn_product_id,
  CONCAT(TRIM(CAST(m.tos_offer_id AS STRING)), '~', TRIM(CAST(m.tcn_product_id AS STRING))) AS tcn_offer_product_id,
  m.tos_offer_status_id,
  m.tos_response_level_id,
  m.tos_rank,
  m.vo_kenn,
  m.vt_segment_id,
  m.kd_segment_id,
  CASE WHEN m.sdm_team_id IS NOT NULL THEN lkp.sdm_team_id ELSE NULL END AS sdm_team_id,
  m.kkm_medium_id,
  m.kkm_richtung_id,
  m.mea_1 AS anzahl_produkte,
  m.vt_bic_segment_endntz_id,
  m.vt_bic_segment_entsch_id,
  m.one_segment_id
FROM `{{project_id}}.dataset.x_tos_measures` m
LEFT JOIN lkp_teamvirt_ccos lkp 
  ON lkp.stichtag = m.stichtag AND lkp.sdm_team_id = m.sdm_team_id
WHERE m.tos_mea_group_name = 'PRODUCTS';

-- Step 7: Populate weekly quotes and contracts dataset
CREATE OR REPLACE TABLE `{{project_id}}.dataset.tos_quotes_contracts_wk` AS
SELECT 
  stichtag,
  kkm_kampagne_id,
  tos_offer_folder_id,
  tos_offer_id,
  kampagnen_kanal_id,
  kkm_kamp_exectype_id,
  tos_offer_status_id,
  tos_response_level_id,
  tos_rank,
  vo_kenn,
  vt_segment_id,
  kd_segment_id,
  sdm_team_id,
  kkm_medium_id,
  kkm_richtung_id,
  CASE WHEN tos_mea_group_name = 'QUOTES' THEN mea_1 ELSE 0 END AS anzahl_angebote,
  CASE WHEN tos_mea_group_name = 'QUOTES' THEN CAST(mea_2 AS NUMERIC) ELSE 0 END AS subventionen,
  CASE WHEN tos_mea_group_name = 'CONTRACTS' THEN mea_1 ELSE 0 END AS anzahl_vertraege,
  vt_bic_segment_endntz_id,
  vt_bic_segment_entsch_id,
  one_segment_id
FROM `{{project_id}}.dataset.x_tos_measures`
WHERE (tos_mea_group_name = 'CONTRACTS' OR tos_mea_group_name = 'QUOTES')
  AND stichtag < v_tuesday_cutoff;

-- Step 8: Populate standard quotes and contracts dataset with masked team visibility
CREATE OR REPLACE TABLE `{{project_id}}.dataset.tos_quotes_contracts` AS
SELECT 
  m.stichtag,
  m.kkm_kampagne_id,
  m.tos_offer_folder_id,
  m.tos_offer_id,
  m.kampagnen_kanal_id,
  m.kkm_kamp_exectype_id,
  m.tos_offer_status_id,
  m.tos_response_level_id,
  m.tos_rank,
  m.vo_kenn,
  m.vt_segment_id,
  m.kd_segment_id,
  CASE WHEN m.sdm_team_id IS NOT NULL THEN lkp.sdm_team_id ELSE NULL END AS sdm_team_id,
  m.kkm_medium_id,
  m.kkm_richtung_id,
  CASE WHEN m.tos_mea_group_name = 'QUOTES' THEN m.mea_1 ELSE 0 END AS anzahl_angebote,
  CASE WHEN m.tos_mea_group_name = 'QUOTES' THEN CAST(m.mea_2 AS NUMERIC) ELSE 0 END AS subventionen,
  CASE WHEN m.tos_mea_group_name = 'CONTRACTS' THEN m.mea_1 ELSE 0 END AS anzahl_vertraege,
  m.vt_bic_segment_endntz_id,
  m.vt_bic_segment_entsch_id,
  m.one_segment_id
FROM `{{project_id}}.dataset.x_tos_measures` m
LEFT JOIN lkp_teamvirt_ccos lkp 
  ON lkp.stichtag = m.stichtag AND lkp.sdm_team_id = m.sdm_team_id
WHERE m.tos_mea_group_name = 'CONTRACTS' OR m.tos_mea_group_name = 'QUOTES';