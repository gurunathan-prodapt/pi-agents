-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_basis_his.ksh
-- and its dependency d_ausd_bp_ta_bpr_basis_his.sql
--
-- BigQuery Stored Procedure for processing PoolBasisprodukt data.
--
CREATE OR REPLACE PROCEDURE project.dataset.r_ausd_bp_ta_bpr_basis_his(
  p_job_kennung STRING,
  p_eintrags_nr STRING,
  p_stichtag DATE,
  p_wiederanlauf_wert STRING
)
BEGIN
  DECLARE v_records_processed INT64;

  -- Step 01: Truncate the target table sof$ta_bpr_basis_his
  -- Note: The design document mentions PoolBasisprodukt as the target,
  -- but the source SQL uses sof$ta_bpr_basis_his. We assume this maps to PoolBasisprodukt.
  TRUNCATE TABLE project.dataset.sof$ta_bpr_basis_his;

  -- Step 03b: Insert data into the target table
  INSERT INTO project.dataset.sof$ta_bpr_basis_his
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
    bp.e_id
  FROM
    isbert_schema.cds$ta_cntrct AS c -- Assuming 'isbert_schema' for source tables
  JOIN
    isbert_schema.pds$ta_bpri_com AS bp -- Assuming 'isbert_schema' for source tables
  ON
    c.cntrct_id = bp.cntrct_id
  WHERE
    c.cntrct_st IN (5, 6) -- nur Vertragsstatus aktiv und beendet (d.h. reaktivierbar)
    AND c.redundant_owner_id = 1 -- keine Service Provider Vertraege
    AND c.insert_at <= p_stichtag
    AND (c.modified_at IS NULL OR c.modified_at > p_stichtag)
    AND c.valid_from <= p_stichtag
    AND (c.valid_to IS NULL OR c.valid_to > p_stichtag)
    AND c.is_production = 1
    AND (c.cntrct_ty NOT IN (1, 2, 5) OR c.cntrct_parent IS NOT NULL)
    AND bp.bpr_id IN (31, 2759, 2800, 2835, 2836, 2837, 3848)
    AND bp.insert_at <= p_stichtag
    AND (bp.modified_at IS NULL OR bp.modified_at > p_stichtag)
    AND bp.valid_from <= p_stichtag
    AND bp.is_production = 1;

  SET v_records_processed = @@row_count;

  -- Log the job execution details to job_log table
  INSERT INTO project.dataset.job_log (
    job_name,
    status,
    error_nr,
    error_arg,
    stichtag,
    records_processed,
    created_at
  )
  VALUES (
    p_job_kennung,
    'SUCCESS',
    0,
    NULL,
    p_stichtag,
    v_records_processed,
    CURRENT_TIMESTAMP()
  );

EXCEPTION WHEN ERROR THEN
  INSERT INTO project.dataset.job_log (
    job_name,
    status,
    error_nr,
    error_arg,
    stichtag,
    records_processed,
    created_at
  )
  VALUES (
    p_job_kennung,
    'FAILED',
    ERROR_CODE(),
    ERROR_MESSAGE(),
    p_stichtag,
    NULL,
    CURRENT_TIMESTAMP()
  );
  RAISE; -- Re-raise the error to propagate it
END;