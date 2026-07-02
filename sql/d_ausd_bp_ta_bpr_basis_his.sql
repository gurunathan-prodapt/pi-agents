-- Declarations for local script execution and metadata references
DECLARE v_datum DATE;

-- Fetch v_datum from monitoring metadata table, mimicking step00
SET v_datum = (
  SELECT COALESCE(DATE(MAX(timecreated)), DATE('1900-01-01'))
  FROM `gcp-dwh-prod.isbert_schema.dwtk_meldungen`
  WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
);

-- Step 01: Truncate local target table to handle restarts idempotently
TRUNCATE TABLE `gcp-dwh-prod.sof.ta_bpr_basis_his`;

-- Step 03b: Populate target table using date filter criteria
INSERT INTO `gcp-dwh-prod.sof.ta_bpr_basis_his`
(
  CNTRCT_ID,
  BPR_ID,
  BPRI_COM_ID,
  ICCID,
  IMSI_MCC,
  IMSI_MNC,
  IMSI_HLR,
  IMSI_SI,
  CNTRCT_ID_REF,
  VALID_FROM,
  VALID_TO,
  MODIFIED_AT,
  INSERT_AT,
  SLAVE_NUMBER,
  E_ID
)
SELECT
  bp.cntrct_id,
  bp.bpr_id,
  bp.bpri_com_id,
  -- Safe concatenation of components with COALESCE to mimic Oracle string concatenation behavior
  CONCAT(
    IFNULL(bp.iccid_mi, ''), '-', 
    IFNULL(bp.iccid_ii, ''), '-', 
    IFNULL(bp.iccid_iai, ''), '-', 
    IFNULL(bp.iccid_nr, ''), '-', 
    IFNULL(bp.iccid_cd, '')
  ) AS iccid,
  bp.imsi_mcc,
  bp.imsi_mnc,
  bp.imsi_hlr,
  bp.imsi_si,
  bp.cntrct_id_ref,
  bp.valid_from,
  bp.valid_to,
  bp.modified_at,
  bp.insert_at,
  bp.slave_number,
  bp.eid
FROM `gcp-dwh-prod.cds.ta_cntrct` c
JOIN `gcp-dwh-prod.pds.ta_bpri_com` bp
  ON c.cntrct_id = bp.cntrct_id
WHERE c.cntrct_st IN (5, 6)                         -- Active and deactivated-reactivable
  AND c.redundant_owner_id = 1                      -- Exclude Service Provider Contracts
  AND DATE(c.insert_at) <= v_datum
  AND (c.modified_at IS NULL OR DATE(c.modified_at) > v_datum)
  AND DATE(c.valid_from) <= v_datum
  AND (c.valid_to IS NULL OR DATE(c.valid_to) > v_datum)
  AND c.is_production = 1
  AND (c.cntrct_ty NOT IN (1, 2, 5) OR c.cntrct_parent IS NOT NULL)
  -- Filter on target base product IDs
  AND bp.bpr_id IN (31, 2759, 2800, 2835, 2836, 2837, 3848)
  AND DATE(bp.insert_at) <= v_datum
  AND (bp.modified_at IS NULL OR DATE(bp.modified_at) > v_datum)
  AND DATE(bp.valid_from) <= v_datum
  AND bp.is_production = 1;