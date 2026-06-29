-- Legacy Source: d_ausd_bp_ta_bpr_basis.sql
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

-- Step 1: Truncate temporary target tables
TRUNCATE TABLE `core_bert.sof$ta_sim`;
TRUNCATE TABLE `core_bert.sof$ta_bpr_basis`;

-- Step 2: Populate local SIM table
INSERT INTO `core_bert.sof$ta_sim` (
  iccid,
  sim_card_type_id,
  card_type_name
)
SELECT 
  CONCAT(sim.iccid_mi, '-', sim.iccid_ii, '-', sim.iccid_iai, '-', sim.iccid_nr, '-', sim.iccid_cd) AS iccid,
  sim.sim_card_type_id,
  card.card_type_name
FROM `src_carmen.rma$ta_sim` sim
INNER JOIN `src_carmen.rma$ta_sim_card_type` card ON card.sim_card_type_id = sim.sim_card_type_id
WHERE sim.insert_at <= v_datum_date
  AND (sim.modified_at IS NULL OR sim.modified_at > v_datum_date)
  AND sim.valid_from <= v_datum_date
  AND (sim.valid_to IS NULL OR sim.valid_to > v_datum_date)
  AND card.insert_at <= v_datum_date
  AND (card.modified_at IS NULL OR card.modified_at > v_datum_date);

-- Step 3: Populate target base products consolidated with active SIM status
INSERT INTO `core_bert.sof$ta_bpr_basis` (
  cntrct_id,
  bpr_id,
  bpr_instance_id,
  iccid,
  imsi_mcc,
  imsi_mnc,
  imsi_hlr,
  imsi_si,
  valid_to,
  slave_number,
  e_id,
  card_type_name
)
SELECT 
  bp.cntrct_id,
  bp.bpr_id,
  bp.bpri_com_id AS bpr_instance_id,
  bp.iccid,
  bp.imsi_mcc,
  bp.imsi_mnc,
  bp.imsi_hlr,
  bp.imsi_si,
  COALESCE(bp.valid_to, DATE '4712-12-31') AS valid_to,
  bp.slave_number,
  bp.e_id,
  sim.card_type_name
FROM (
  SELECT 
    bp1.*,
    MAX(COALESCE(bp1.valid_to, DATE '4712-12-31')) OVER (PARTITION BY bp1.cntrct_id, bp1.bpr_id) AS max_valid_to
  FROM `core_bert.sof$ta_bpr_basis_his` bp1
) bp
LEFT OUTER JOIN `core_bert.sof$ta_sim` sim ON bp.iccid = sim.iccid
WHERE COALESCE(bp.valid_to, DATE '4712-12-31') = bp.max_valid_to;