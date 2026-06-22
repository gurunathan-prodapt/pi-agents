-- Migrated from Oracle SQL script: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_cntrct_crs2.sql
-- Job: DW.BERT_AUSD_V_TA_CNTRCT_CRS2

-- Derive v_datum equivalent
DECLARE v_datum STRING DEFAULT (
  SELECT COALESCE(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), '19000101')
  FROM `isbert_schema.dwtk_meldungen` m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

-- Clear target table
TRUNCATE TABLE `sof_ta_cntrct_crs2`;

-- Insert transformed data
INSERT INTO `sof_ta_cntrct_crs2` (
  cntrct_id,
  obj_version,
  contract_number,
  cntrct_template_id,
  cntrct_validity_id,
  valid_from,
  com_per_ext_rea_cv,
  billcycle_id,
  vo_code,
  cntrct_start_date,
  cntrct_st,
  cntrct_parent,
  cntrct_ty,
  cost_centre,
  cost_centre_user,
  commitment_reference_date,
  order_number,
  rv_num
)
SELECT
  c.cntrct_id,
  c.obj_version,
  c.contract_number,
  c.cntrct_template_id,
  c.cntrct_validity_id,
  c.valid_from,
  c.com_per_ext_rea_cv,
  c.billcycle_id,
  c.vo_code,
  c.cntrct_start_date,
  c.cntrct_st,
  c.cntrct_parent,
  c.cntrct_ty,
  c.cost_centre,
  c.cost_centre_user,
  c.commitment_reference_date,
  c.order_number,
  cr.contract_number AS rv_num
FROM `sof_ta_cntrct_crs` c
LEFT JOIN `sof_ta_cntrct_crs` cr
  ON c.cntrct_parent = cr.cntrct_id
 AND cr.cntrct_ty = 10
WHERE c.cntrct_ty <> 10;