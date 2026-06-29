-- Legacy Source: d_ausd_bp_ta_bpr_basis_his.sql
-- Job: ausd_bp_ta_bpr_basis
-- Platform: BigQuery

DECLARE v_datum STRING;
DECLARE v_datum_date DATE;

SET v_datum = (
  SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(timecreated)), '19000101')
  FROM `core_bert.dwtk_meldungen`
  WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
);

SET v_datum_date = PARSE_DATE('%Y%m%d', v_datum);

-- Step 1: Truncate local target table
TRUNCATE TABLE `core_bert.sof$ta_bpr_basis_his`;

-- Step 2: Insert historical basis products
INSERT INTO `core_bert.sof$ta_bpr_basis_his` (
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
  CONCAT(bp.iccid_mi, '-', bp.iccid_ii, '-', bp.iccid_iai, '-', bp.iccid_nr, '-', bp.iccid_cd) AS iccid,
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
FROM `src_carmen.cds$ta_cntrct` c
INNER JOIN `src_carmen.pds$ta_bpri_com` bp ON c.cntrct_id = bp.cntrct_id
WHERE c.cntrct_st IN (5, 6)
  AND c.redundant_owner_id = 1
  AND c.insert_at <= v_datum_date
  AND (c.modified_at IS NULL OR c.modified_at > v_datum_date)
  AND c.valid_from <= v_datum_date
  AND (c.valid_to IS NULL OR c.valid_to > v_datum_date)
  AND c.is_production = 1
  AND (c.cntrct_ty NOT IN (1, 2, 5) OR c.cntrct_parent IS NOT NULL)
  -- Filter on target base product IDs
  AND bp.bpr_id IN (
    31,   -- tnv
    2759, -- twin-card
    2800, -- twin-bill-privat
    2835, -- da-anschluss
    2836, -- vda-anschluss
    2837, -- tk-anschluss
    3848  -- MultiSIM
  )
  AND bp.insert_at <= v_datum_date
  AND (bp.modified_at IS NULL OR bp.modified_at > v_datum_date)
  AND bp.valid_from <= v_datum_date
  AND bp.is_production = 1;