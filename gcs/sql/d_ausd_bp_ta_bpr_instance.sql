-- ===================================================================
-- BigQuery SQL Migration script for: d_ausd_bp_ta_bpr_instance.sql
-- ===================================================================

DECLARE v_datum STRING;

-- Resolve v_datum: use stichtag parameter if provided, otherwise fetch from dwtk_meldungen
SET v_datum = COALESCE(
  NULLIF('{{ dag_run.conf.get("stichtag", "") }}', ''),
  (
    SELECT FORMAT_DATE('%Y%m%d', DATE(MAX(timecreated)))
    FROM `{{ var.value.get("GCP_PROJECT_ID", "gcp-project") }}.isbert_schema.dwtk_meldungen`
    WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
  ),
  '19000101'
);

-- Truncate target table
TRUNCATE TABLE `{{ var.value.get("GCP_PROJECT_ID", "gcp-project") }}.sof.ta_bpr_instance`;

-- Populate target table
INSERT INTO `{{ var.value.get("GCP_PROJECT_ID", "gcp-project") }}.sof.ta_bpr_instance` (
  CNTRCT_ID,
  BPR_ID,
  BPR_INSTANCE_ID,
  ICCID,
  IMSI_MCC,
  IMSI_MNC,
  IMSI_HLR,
  IMSI_SI,
  CNTRCT_ID_REF
)
SELECT
  bp.cntrct_id,
  bp.bpr_id,
  bp.bpri_com_id AS bpr_instance_id,
  CONCAT(bp.iccid_mi, '-', bp.iccid_ii, '-', bp.iccid_iai, '-', bp.iccid_nr, '-', bp.iccid_cd) AS iccid,
  bp.imsi_mcc,
  bp.imsi_mnc,
  bp.imsi_hlr,
  bp.imsi_si,
  bp.cntrct_id_ref
FROM `{{ var.value.get("GCP_PROJECT_ID", "gcp-project") }}.cds.ta_cntrct` AS c
JOIN `{{ var.value.get("GCP_PROJECT_ID", "gcp-project") }}.pds.ta_bpri_com` AS bp
  ON c.cntrct_id = bp.cntrct_id
WHERE c.cntrct_st IN (5, 6)
  AND c.redundant_owner_id = 1
  AND c.insert_at <= PARSE_DATE('%Y%m%d', v_datum)
  AND (c.modified_at IS NULL OR c.modified_at > PARSE_DATE('%Y%m%d', v_datum))
  AND c.valid_from <= PARSE_DATE('%Y%m%d', v_datum)
  AND (c.valid_to IS NULL OR c.valid_to > PARSE_DATE('%Y%m%d', v_datum))
  AND c.is_production = 1
  AND (c.cntrct_ty NOT IN (1, 2, 5) OR c.cntrct_parent IS NOT NULL)
  AND bp.insert_at <= PARSE_DATE('%Y%m%d', v_datum)
  AND (bp.modified_at IS NULL OR bp.modified_at > PARSE_DATE('%Y%m%d', v_datum))
  AND bp.valid_from <= PARSE_DATE('%Y%m%d', v_datum)
  AND (bp.valid_to IS NULL OR bp.valid_to > PARSE_DATE('%Y%m%d', v_datum))
  AND bp.is_production = 1
  AND bp.bpri_com_id > CAST('{{ dag_run.conf.get("wiederanlauf_wert", "0") }}' AS INT64);