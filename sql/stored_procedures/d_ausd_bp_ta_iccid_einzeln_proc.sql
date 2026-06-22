-- BigQuery Stored Procedure for k_ausd_bp_ta_iccid_einzeln.ksh and d_ausd_bp_ta_iccid_einzeln.sql
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_iccid_einzeln.ksh
-- Language: BQSQL

CREATE OR REPLACE PROCEDURE `project.dataset.d_ausd_bp_ta_iccid_einzeln_proc`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING,
  IN p_Stichtag STRING,
  IN p_wiederanlaufWert INT64
)
BEGIN
  DECLARE ErrNr INT64 DEFAULT 0;
  DECLARE ErrArg STRING DEFAULT '';
  DECLARE v_TabName STRING DEFAULT 'PoolBasisprodukt'; -- Unused in migrated logic, keep for reference
  DECLARE v_datum_heute DATE;
  DECLARE v_datum_gestern DATE;
  DECLARE v_records STRING DEFAULT '';
  DECLARE v_stichtag_date DATE;
  DECLARE v_max_timecreated_datum STRING;

  -- Parameter validation
  IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
    SET ErrNr = 193; SET ErrArg = 'Jobkennung';
  END IF;

  IF ErrNr = 0 AND (p_Stichtag IS NULL OR TRIM(p_Stichtag) = '') THEN
    SET ErrNr = 193; SET ErrArg = 'Stichtag';
  END IF;

  IF ErrNr = 0 AND (p_EintragsNr IS NULL OR TRIM(p_EintragsNr) = '') THEN
    SET ErrNr = 193; SET ErrArg = 'EintragsNr';
  END IF;

  IF ErrNr != 0 THEN
    INSERT INTO `project.dataset.job_error_log`
    (job_kennung, eintragsnr, stichtag, err_nr, err_arg, created_at)
    VALUES
    (p_JobKennung, p_EintragsNr, p_Stichtag, ErrNr, ErrArg, CURRENT_TIMESTAMP());
    SELECT FORMAT('FEHLER: 0 E %d %s', ErrNr, ErrArg) AS message;
    RETURN; -- Exit procedure
  END IF;

  -- Date validation for DDMMYYYY
  SET v_stichtag_date = SAFE.PARSE_DATE('%d%m%Y', p_Stichtag);

  IF v_stichtag_date IS NULL THEN
    SET ErrNr = 193; SET ErrArg = 'Stichtag ungültig - Format DDMMYYYY erwartet';
    INSERT INTO `project.dataset.job_error_log`
    (job_kennung, eintragsnr, stichtag, err_nr, err_arg, created_at)
    VALUES
    (p_JobKennung, p_EintragsNr, p_Stichtag, ErrNr, ErrArg, CURRENT_TIMESTAMP());
    SELECT FORMAT('FEHLER: 0 E %d %s', ErrNr, ErrArg) AS message;
    RETURN; -- Exit procedure
  END IF;

  -- Derive today and yesterday dates
  SET v_datum_heute = CURRENT_DATE();
  SET v_datum_gestern = DATE_SUB(v_datum_heute, INTERVAL 1 DAY);

  -- Determine v_max_timecreated_datum (equivalent to Oracle's v_datum from dwtk_meldungen)
  SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
  INTO v_max_timecreated_datum
  FROM `project.dataset.dwtk_meldungen` AS m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';

  -- Default restart value
  -- Assuming p_wiederanlaufWert is passed and can be NULL, if so, default to 0.
  IF p_wiederanlaufWert IS NULL THEN
    SET p_wiederanlaufWert = 0;
  END IF;

  -- Truncate target table
  TRUNCATE TABLE `project.dataset.sof_ta_iccid_einzeln`;

  -- Core logic: INSERT into target table
  INSERT INTO `project.dataset.sof_ta_iccid_einzeln`
  ( CNTRCT_ID,
    TN_ICCID, TN_IMSI_MCC, TN_IMSI_MNC, TN_IMSI_HLR, TN_IMSI_SI, TN_STATUS, TN_VALID_TO, TN_E_ID, TN_CARD_TYPE_NAME,
    TC_ICCID, TC_IMSI_MCC, TC_IMSI_MNC, TC_IMSI_HLR, TC_IMSI_SI, TC_STATUS, TC_VALID_TO, TC_E_ID, TC_CARD_TYPE_NAME,
    TB_ICCID, TB_IMSI_MCC, TB_IMSI_MNC, TB_IMSI_HLR, TB_IMSI_SI, TB_STATUS, TB_VALID_TO, TB_E_ID, TB_CARD_TYPE_NAME,
    MS1_ICCID, MS1_IMSI_MCC, MS1_IMSI_MNC, MS1_IMSI_HLR, MS1_IMSI_SI, MS1_STATUS, MS1_VALID_TO, MS1_E_ID, MS1_CARD_TYPE_NAME,
    MS2_ICCID, MS2_IMSI_MCC, MS2_IMSI_MNC, MS2_IMSI_HLR, MS2_IMSI_SI, MS2_STATUS, MS2_VALID_TO, MS2_E_ID, MS2_CARD_TYPE_NAME,
    MS3_ICCID, MS3_IMSI_MCC, MS3_IMSI_MNC, MS3_IMSI_HLR, MS3_IMSI_SI, MS3_STATUS, MS3_VALID_TO DATE, MS3_E_ID STRING, MS3_CARD_TYPE_NAME STRING,
    MS4_ICCID STRING, MS4_IMSI_MCC STRING, MS4_IMSI_MNC STRING, MS4_IMSI_HLR STRING, MS4_IMSI_SI STRING, MS4_STATUS STRING, MS4_VALID_TO DATE, MS4_E_ID STRING, MS4_CARD_TYPE_NAME STRING,
    MS5_ICCID STRING, MS5_IMSI_MCC STRING, MS5_IMSI_MNC STRING, MS5_IMSI_HLR STRING, MS5_IMSI_SI STRING, MS5_STATUS STRING, MS5_VALID_TO DATE, MS5_E_ID STRING, MS5_CARD_TYPE_NAME STRING,
    MS6_ICCID STRING, MS6_IMSI_MCC STRING, MS6_IMSI_MNC STRING, MS6_IMSI_HLR STRING, MS6_IMSI_SI STRING, MS6_STATUS STRING, MS6_VALID_TO DATE, MS6_E_ID STRING, MS6_CARD_TYPE_NAME STRING,
    MS7_ICCID STRING, MS7_IMSI_MCC STRING, MS7_IMSI_MNC STRING, MS7_IMSI_HLR STRING, MS7_IMSI_SI STRING, MS7_STATUS STRING, MS7_VALID_TO DATE, MS7_E_ID STRING, MS7_CARD_TYPE_NAME STRING,
    MS8_ICCID STRING, MS8_IMSI_MCC STRING, MS8_IMSI_MNC STRING, MS8_IMSI_HLR STRING, MS8_IMSI_SI STRING, MS8_STATUS STRING, MS8_VALID_TO DATE, MS8_E_ID STRING, MS8_CARD_TYPE_NAME STRING,
    MS9_ICCID STRING, MS9_IMSI_MCC STRING, MS9_IMSI_MNC STRING, MS9_IMSI_HLR STRING, MS9_IMSI_SI STRING, MS9_STATUS STRING, MS9_VALID_TO DATE, MS9_E_ID STRING, MS9_CARD_TYPE_NAME STRING,
    MS10_ICCID STRING, MS10_IMSI_MCC STRING, MS10_IMSI_MNC STRING, MS10_IMSI_HLR STRING, MS10_IMSI_SI STRING, MS10_STATUS STRING, MS10_VALID_TO DATE, MS10_E_ID STRING, MS10_CARD_TYPE_NAME STRING
  )
  SELECT
        bp.cntrct_id,
        CASE WHEN bp.bpr_id = 31 THEN iccid ELSE NULL END AS TN_ICCID,
        CASE WHEN bp.bpr_id = 31 THEN imsi_mcc ELSE NULL END AS TN_IMSI_MCC,
        CASE WHEN bp.bpr_id = 31 THEN imsi_mnc ELSE NULL END AS TN_IMSI_MNC,
        CASE WHEN bp.bpr_id = 31 THEN imsi_hlr ELSE NULL END AS TN_IMSI_HLR,
        CASE WHEN bp.bpr_id = 31 THEN imsi_si ELSE NULL END AS TN_IMSI_SI,
        CASE WHEN bp.bpr_id = 31 THEN CASE WHEN bp.valid_to <= PARSE_DATE('%Y%m%d', v_max_timecreated_datum) THEN 'L' ELSE 'A' END ELSE NULL END AS TN_STATUS,
        CASE WHEN bp.bpr_id = 31 THEN bp.valid_to ELSE NULL END AS TN_VALID_TO,
        CASE WHEN bp.bpr_id = 31 THEN E_ID ELSE NULL END AS TN_E_ID,
        CASE WHEN bp.bpr_id = 31 THEN CARD_TYPE_NAME ELSE NULL END AS TN_CARD_TYPE_NAME,
        CASE WHEN bp.bpr_id = 2759 THEN iccid ELSE NULL END AS TC_ICCID,
        CASE WHEN bp.bpr_id = 2759 THEN imsi_mcc ELSE NULL END AS TC_IMSI_MCC,
        CASE WHEN bp.bpr_id = 2759 THEN imsi_mnc ELSE NULL END AS TC_IMSI_MNC,
        CASE WHEN bp.bpr_id = 2759 THEN imsi_hlr ELSE NULL END AS TC_IMSI_HLR,
        CASE WHEN bp.bpr_id = 2759 THEN imsi_si ELSE NULL END AS TC_IMSI_SI,
        CASE WHEN bp.bpr_id = 2759 THEN CASE WHEN bp.valid_to <= PARSE_DATE('%Y%m%d', v_max_timecreated_datum) THEN 'L' ELSE 'A' END ELSE NULL END AS TC_STATUS,
        CASE WHEN bp.bpr_id = 2759 THEN bp.valid_to ELSE NULL END AS TC_VALID_TO,
        CASE WHEN bp.bpr_id = 2759 THEN E_ID ELSE NULL END AS TC_E_ID,
        CASE WHEN bp.bpr_id = 2759 THEN CARD_TYPE_NAME ELSE NULL END AS TC_CARD_TYPE_NAME,
        CASE WHEN bp.bpr_id = 2800 THEN iccid ELSE NULL END AS TB_ICCID,
        CASE WHEN bp.bpr_id = 2800 THEN imsi_mcc ELSE NULL END AS TB_IMSI_MCC,
        CASE WHEN bp.bpr_id = 2800 THEN imsi_mnc ELSE NULL END AS TB_IMSI_MNC,
        CASE WHEN bp.bpr_id = 2800 THEN imsi_hlr ELSE NULL END AS TB_IMSI_HLR,
        CASE WHEN bp.bpr_id = 2800 THEN imsi_si ELSE NULL END AS TB_IMSI_SI,
        CASE WHEN bp.bpr_id = 2800 THEN CASE WHEN bp.valid_to <= PARSE_DATE('%Y%m%d', v_max_timecreated_datum) THEN 'L' ELSE 'A' END ELSE NULL END AS TB_STATUS,
        CASE WHEN bp.bpr_id = 2800 THEN bp.valid_to ELSE NULL END AS TB_VALID_TO,
        CASE WHEN bp.bpr_id = 2800 THEN E_ID ELSE NULL END AS TB_E_ID,
        CASE WHEN bp.bpr_id = 2800 THEN CARD_TYPE_NAME ELSE NULL END AS TB_CARD_TYPE_NAME,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384801 THEN bp.iccid ELSE NULL END AS MS1_ICCID,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384801 THEN bp.imsi_mcc ELSE NULL END AS MS1_IMSI_MCC,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384801 THEN bp.imsi_mnc ELSE NULL END AS MS1_IMSI_MNC,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384801 THEN bp.imsi_hlr ELSE NULL END AS MS1_IMSI_HLR,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384801 THEN bp.imsi_si ELSE NULL END AS MS1_IMSI_SI,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384801 THEN IF(bp.valid_to > PARSE_DATE('%Y%m%d', v_max_timecreated_datum), 'A', 'L') ELSE NULL END AS MS1_STATUS,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384801 THEN bp.valid_to ELSE NULL END AS MS1_VALID_TO,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384801 THEN bp.E_ID ELSE NULL END AS MS1_E_ID,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384801 THEN bp.CARD_TYPE_NAME ELSE NULL END AS MS1_CARD_TYPE_NAME,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384802 THEN bp.iccid ELSE NULL END AS MS2_ICCID,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384802 THEN bp.imsi_mcc ELSE NULL END AS MS2_IMSI_MCC,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384802 THEN bp.imsi_mnc ELSE NULL END AS MS2_IMSI_MNC,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384802 THEN bp.imsi_hlr ELSE NULL END AS MS2_IMSI_HLR,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384802 THEN bp.imsi_si ELSE NULL END AS MS2_IMSI_SI,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384802 THEN IF(bp.valid_to > PARSE_DATE('%Y%m%d', v_max_timecreated_datum), 'A', 'L') ELSE NULL END AS MS2_STATUS,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384802 THEN bp.valid_to ELSE NULL END AS MS2_VALID_TO,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384802 THEN bp.E_ID ELSE NULL END AS MS2_E_ID,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384802 THEN bp.CARD_TYPE_NAME ELSE NULL END AS MS2_CARD_TYPE_NAME,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384803 THEN bp.iccid ELSE NULL END AS MS3_ICCID,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384803 THEN bp.imsi_mcc ELSE NULL END AS MS3_IMSI_MCC,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384803 THEN bp.imsi_mnc ELSE NULL END AS MS3_IMSI_MNC,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384803 THEN bp.imsi_hlr ELSE NULL END AS MS3_IMSI_HLR,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384803 THEN bp.imsi_si ELSE NULL END AS MS3_IMSI_SI,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384803 THEN IF(bp.valid_to > PARSE_DATE('%Y%m%d', v_max_timecreated_datum), 'A', 'L') ELSE NULL END AS MS3_STATUS,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384803 THEN bp.valid_to ELSE NULL END AS MS3_VALID_TO,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384803 THEN bp.E_ID ELSE NULL END AS MS3_E_ID,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384803 THEN bp.CARD_TYPE_NAME ELSE NULL END AS MS3_CARD_TYPE_NAME,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384804 THEN bp.iccid ELSE NULL END AS MS4_ICCID,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384804 THEN bp.imsi_mcc ELSE NULL END AS MS4_IMSI_MCC,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384804 THEN bp.imsi_mnc ELSE NULL END AS MS4_IMSI_MNC,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384804 THEN bp.imsi_hlr ELSE NULL END AS MS4_IMSI_HLR,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384804 THEN bp.imsi_si ELSE NULL END AS MS4_IMSI_SI,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384804 THEN IF(bp.valid_to > PARSE_DATE('%Y%m%d', v_max_timecreated_datum), 'A', 'L') ELSE NULL END AS MS4_STATUS,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384804 THEN bp.valid_to ELSE NULL END AS MS4_VALID_TO,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384804 THEN bp.E_ID ELSE NULL END AS MS4_E_ID,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384804 THEN bp.CARD_TYPE_NAME ELSE NULL END AS MS4_CARD_TYPE_NAME,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384805 THEN bp.iccid ELSE NULL END AS MS5_ICCID,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384805 THEN bp.imsi_mcc ELSE NULL END AS MS5_IMSI_MCC,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384805 THEN bp.imsi_mnc ELSE NULL END AS MS5_IMSI_MNC,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384805 THEN bp.imsi_hlr ELSE NULL END AS MS5_IMSI_HLR,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384805 THEN bp.imsi_si ELSE NULL END AS MS5_IMSI_SI,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384805 THEN IF(bp.valid_to > PARSE_DATE('%Y%m%d', v_max_timecreated_datum), 'A', 'L') ELSE NULL END AS MS5_STATUS,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384805 THEN bp.valid_to ELSE NULL END AS MS5_VALID_TO,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384805 THEN bp.E_ID ELSE NULL END AS MS5_E_ID,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384805 THEN bp.CARD_TYPE_NAME ELSE NULL END AS MS5_CARD_TYPE_NAME,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384806 THEN bp.iccid ELSE NULL END AS MS6_ICCID,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384806 THEN bp.imsi_mcc ELSE NULL END AS MS6_IMSI_MCC,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384806 THEN bp.imsi_mnc ELSE NULL END AS MS6_IMSI_MNC,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384806 THEN bp.imsi_hlr ELSE NULL END AS MS6_IMSI_HLR,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384806 THEN bp.imsi_si ELSE NULL END AS MS6_IMSI_SI,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384806 THEN IF(bp.valid_to > PARSE_DATE('%Y%m%d', v_max_timecreated_datum), 'A', 'L') ELSE NULL END AS MS6_STATUS,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384806 THEN bp.valid_to ELSE NULL END AS MS6_VALID_TO,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384806 THEN bp.E_ID ELSE NULL END AS MS6_E_ID,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384806 THEN bp.CARD_TYPE_NAME ELSE NULL END AS MS6_CARD_TYPE_NAME,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384807 THEN bp.iccid ELSE NULL END AS MS7_ICCID,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384807 THEN bp.imsi_mcc ELSE NULL END AS MS7_IMSI_MCC,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384807 THEN bp.imsi_mnc ELSE NULL END AS MS7_IMSI_MNC,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384807 THEN bp.imsi_hlr ELSE NULL END AS MS7_IMSI_HLR,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384807 THEN bp.imsi_si ELSE NULL END AS MS7_IMSI_SI,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384807 THEN IF(bp.valid_to > PARSE_DATE('%Y%m%d', v_max_timecreated_datum), 'A', 'L') ELSE NULL END AS MS7_STATUS,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384807 THEN bp.valid_to ELSE NULL END AS MS7_VALID_TO,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384807 THEN bp.E_ID ELSE NULL END AS MS7_E_ID,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384807 THEN bp.CARD_TYPE_NAME ELSE NULL END AS MS7_CARD_TYPE_NAME,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384808 THEN bp.iccid ELSE NULL END AS MS8_ICCID,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384808 THEN bp.imsi_mcc ELSE NULL END AS MS8_IMSI_MCC,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384808 THEN bp.imsi_mnc ELSE NULL END AS MS8_IMSI_MNC,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384808 THEN bp.imsi_hlr ELSE NULL END AS MS8_IMSI_HLR,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384808 THEN bp.imsi_si ELSE NULL END AS MS8_IMSI_SI,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384808 THEN IF(bp.valid_to > PARSE_DATE('%Y%m%d', v_max_timecreated_datum), 'A', 'L') ELSE NULL END AS MS8_STATUS,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384808 THEN bp.valid_to ELSE NULL END AS MS8_VALID_TO,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384808 THEN bp.E_ID ELSE NULL END AS MS8_E_ID,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384808 THEN bp.CARD_TYPE_NAME ELSE NULL END AS MS8_CARD_TYPE_NAME,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384809 THEN bp.iccid ELSE NULL END AS MS9_ICCID,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384809 THEN bp.imsi_mcc ELSE NULL END AS MS9_IMSI_MCC,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384809 THEN bp.imsi_mnc ELSE NULL END AS MS9_IMSI_MNC,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384809 THEN bp.imsi_hlr ELSE NULL END AS MS9_IMSI_HLR,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384809 THEN bp.imsi_si ELSE NULL END AS MS9_IMSI_SI,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384809 THEN IF(bp.valid_to > PARSE_DATE('%Y%m%d', v_max_timecreated_datum), 'A', 'L') ELSE NULL END AS MS9_STATUS,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384809 THEN bp.valid_to ELSE NULL END AS MS9_VALID_TO,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384809 THEN bp.E_ID ELSE NULL END AS MS9_E_ID,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384809 THEN bp.CARD_TYPE_NAME ELSE NULL END AS MS9_CARD_TYPE_NAME,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384810 THEN bp.iccid ELSE NULL END AS MS10_ICCID,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384810 THEN bp.imsi_mcc ELSE NULL END AS MS10_IMSI_MCC,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384810 THEN bp.imsi_mnc ELSE NULL END AS MS10_IMSI_MNC,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384810 THEN bp.imsi_hlr ELSE NULL END AS MS10_IMSI_HLR,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384810 THEN bp.imsi_si ELSE NULL END AS MS10_IMSI_SI,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384810 THEN IF(bp.valid_to > PARSE_DATE('%Y%m%d', v_max_timecreated_datum), 'A', 'L') ELSE NULL END AS MS10_STATUS,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384810 THEN bp.valid_to ELSE NULL END AS MS10_VALID_TO,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384810 THEN bp.E_ID ELSE NULL END AS MS10_E_ID,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384810 THEN bp.CARD_TYPE_NAME ELSE NULL END AS MS10_CARD_TYPE_NAME
  FROM    `project.dataset.sof_ta_bpr_basis` AS bp
  WHERE   bp.bpr_id IN (31, 2759, 2800, 3848);

  -- Log run results
  INSERT INTO `project.dataset.job_run_result`
  (job_kennung, eintragsnr, stichtag, tab_name, datum_heute, datum_gestern, restart_value, record_count, created_at)
  VALUES
  (p_JobKennung, p_EintragsNr, p_Stichtag, v_TabName, v_datum_heute, v_datum_gestern, p_wiederanlaufWert, (SELECT CAST(COUNT(*) AS STRING) FROM `project.dataset.sof_ta_iccid_einzeln`), CURRENT_TIMESTAMP());

  SELECT '---------- ENDE Datenverarbeitung ----------' AS message;

END;