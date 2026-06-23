-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aufbereitung/sql/d_ausd_v_ta_inv_def.sql
-- Legacy job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_inv_def.ksh

-- This script represents the core data manipulation logic for ta_inv_def.
-- It truncates a target table and then inserts data based on complex joins and conditions.

DECLARE v_datum DATE DEFAULT (
  SELECT COALESCE(MAX(DATE(timecreated)), DATE '1900-01-01')
  FROM `project_id.dataset_id.dwtk_meldungen`
  WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
);

TRUNCATE TABLE `project_id.dataset_id.sof$ta_inv_def`;

INSERT INTO `project_id.dataset_id.sof$ta_inv_def` (
  inv_definition_id,
  acc_ref_id,
  inv_pay_ty_cv,
  inv_media_cv,
  billcycle_id,
  sales_tax_freed,
  inv_cont_config_id,
  rechn_inh_konfig_text
)
SELECT
  id.inv_definition_id,
  id.acc_ref_id,
  id.inv_pay_ty_cv,
  id.inv_media_cv,
  id.billcycle_id,
  id.sales_tax_freed,
  id.inv_cont_config_id,
  d.cds_description AS rechn_inh_konfig_text
FROM `project_id.dataset_id.cds$ta_inv_definition` AS id
LEFT JOIN `project_id.dataset_id.cds$ta_inv_cont_config` AS icc
  ON id.inv_cont_config_id = icc.inv_cont_config_id
 AND icc.insert_at <= v_datum
 AND COALESCE(icc.modified_at, DATE_ADD(v_datum, INTERVAL 1 DAY)) > v_datum
 AND icc.valid_from <= v_datum
 AND COALESCE(icc.valid_to, DATE_ADD(v_datum, INTERVAL 1 DAY)) > v_datum
 AND icc.is_production = 1
LEFT JOIN `project_id.dataset_id.cds$ta_care_description` AS d
  ON icc.cds_description_id = d.cds_description_id
WHERE id.insert_at <= v_datum
  AND (id.modified_at IS NULL OR id.modified_at > v_datum)
  AND id.valid_from <= v_datum
  AND (id.valid_to IS NULL OR id.valid_to > v_datum)
  AND id.is_production = 1;