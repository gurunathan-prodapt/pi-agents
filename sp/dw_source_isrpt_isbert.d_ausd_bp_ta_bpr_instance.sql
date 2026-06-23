-- BigQuery Stored Procedure for data transformation logic
-- Replaces vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_bpr_instance.sql
-- Called by sp/dw_source_isrpt_isbert.r_ausd_bp_ta_bpr_instance.sql

CREATE OR REPLACE PROCEDURE `dw_source_isrpt_isbert.d_ausd_bp_ta_bpr_instance`(
  IN p_JobKennung     STRING,
  IN p_Stichtag_date  DATE
)
BEGIN
  DECLARE v_datum STRING DEFAULT (
    SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(DATE(timecreated))), '19000101')
    FROM `isbert_schema.dwtk_meldungen` -- BigQuery table
    WHERE job_kennung = p_JobKennung -- Use passed job_kennung for filtering
  );

  -- Step01: truncate target table
  TRUNCATE TABLE `dw_source_isrpt_isbert.sof_ta_bpr_instance`; -- BigQuery table

  -- Step03: insert local basis product instances
  INSERT INTO `dw_source_isrpt_isbert.sof_ta_bpr_instance`
  (
    CNTRCT_ID,
    BPR_ID,
    BPR_INSTANCE_ID,
    ICCID,
    IMSI_MCC,
    IMSI_MNC,
    IMSI_HLR,
    IMSI_SI,
    CNTRCT_ID_REF,
    processing_date
  )
  SELECT
    bp.cntrct_id,
    bp.bpr_id,
    bp.bpri_com_id AS bpr_instance_id,
    CONCAT(
      LPAD(CAST(bp.iccid_mi AS STRING), 2, '0'), '-',
      LPAD(CAST(bp.iccid_ii AS STRING), 6, '0'), '-',
      LPAD(CAST(bp.iccid_iai AS STRING), 1, '0'), '-',
      LPAD(CAST(bp.iccid_nr AS STRING), 9, '0'), '-',
      CAST(bp.iccid_cd AS STRING)
    ) AS iccid,
    bp.imsi_mcc,
    bp.imsi_mnc,
    bp.imsi_hlr,
    bp.imsi_si,
    bp.cntrct_id_ref,
    p_Stichtag_date -- Use the stichtag from the orchestrator
  FROM `dw_source_isrpt_isbert.cds_ta_cntrct` c -- BigQuery table
  JOIN `dw_source_isrpt_isbert.pds_ta_bpri_com` bp -- BigQuery table
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
    AND bp.is_production = 1;
END;