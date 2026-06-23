-- Migrated from legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_p_basisprod.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_p_basisprod.ksh

CREATE OR REPLACE PROCEDURE `project.dataset.sp_k_ausd_bp_ta_p_basisprod`(
  p_JobKennung STRING,
  p_EintragsNr STRING,
  p_Stichtag STRING, -- Expected format: DDMMYYYY
  p_wiederanlaufWert STRING
)
BEGIN
  DECLARE v_stichtag_date DATE;
  DECLARE v_today DATE;
  DECLARE v_yesterday DATE;
  DECLARE v_records INT64;
  DECLARE v_start_time TIMESTAMP;
  DECLARE v_end_time TIMESTAMP;
  DECLARE v_status STRING;
  DECLARE v_error_message STRING;

  SET v_start_time = CURRENT_TIMESTAMP();
  SET v_status = 'RUNNING';

  -- Parameter Validation
  IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
    SET v_error_message = 'ERROR: p_JobKennung is not set.';
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
  END IF;

  IF p_EintragsNr IS NULL OR p_EintragsNr = '' THEN
    SET v_error_message = 'ERROR: p_EintragsNr is not set.';
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
  END IF;

  IF p_Stichtag IS NULL OR p_Stichtag = '' THEN
    SET v_error_message = 'ERROR: p_Stichtag is not set.';
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
  END IF;

  -- Date Validation (DDMMYYYY) and Conversion
  IF NOT REGEXP_CONTAINS(p_Stichtag, r'^\d{8}$') THEN
    SET v_error_message = 'ERROR: p_Stichtag format is invalid. Expected DDMMYYYY.';
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
  END IF;

  BEGIN
    SET v_stichtag_date = PARSE_DATE('%d%m%Y', p_Stichtag);
  EXCEPTION WHEN ERROR THEN
    SET v_error_message = 'ERROR: Could not parse p_Stichtag to a valid date.';
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
  END;

  -- Date Derivation (replacing gestern.ksh logic)
  SET v_today = CURRENT_DATE();
  SET v_yesterday = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

  -- Log start of job
  INSERT INTO `project.dataset.job_audit_table` (
    job_name, job_kennung, eintrags_nr, stichtag, wiederanlauf_wert, start_time, status, message
  ) VALUES (
    'k_ausd_bp_ta_p_basisprod.ksh', p_JobKennung, p_EintragsNr, v_stichtag_date, p_wiederanlaufWert, v_start_time, 'STARTED', 'Job started.'
  );

  BEGIN
    -- Core SQL Execution (d_ausd_bp_ta_p_basisprod.sql equivalent)
    -- IMPORTANT: The following SQL block needs to be manually translated from Oracle SQL to BigQuery SQL.
    -- This is a placeholder for the translated DML.
    -- The original Oracle SQL can be found in `bigquery/d_ausd_bp_ta_p_basisprod_bq.sql`.

    EXECUTE IMMEDIATE FORMAT("""
      -- Manual BigQuery SQL translation of d_ausd_bp_ta_p_basisprod.sql goes here.
      -- Use %s for parameter substitution (e.g., v_stichtag_date, v_today, v_yesterday)
      -- Example DML (replace with actual translated logic):

      TRUNCATE TABLE `project.dataset.sof_ta_p_basisprod`; -- Equivalent to TRUNCATE REUSE STORAGE

      INSERT INTO `project.dataset.sof_ta_p_basisprod`
      (CNTRCT_ID, EVN, TNV_ICCID, TNV_MCC, TNV_MNC, TNV_HLR, TNV_SI, TNV_ICC_STAT, TNV_ICC_VALID,
       TC_ICCID, TC_MCC, TC_MNC, TC_HLR, TC_SI, TC_ICC_STAT, TC_ICC_VALID,
       TB_ICCID, TB_MCC, TB_MNC, TB_HLR, TB_SI, TB_ICC_STAT, TB_ICC_VALID,
       MS1_ICCID, MS1_MCC, MS1_MNC, MS1_HLR, MS1_SI, MS1_STAT, MS1_VALID,
       MS2_ICCID, MS2_MCC, MS2_MNC, MS2_HLR, MS2_SI, MS2_STAT, MS2_VALID,
       TNV_E_ID, TNV_CARD_TYPE_NAME, TC_E_ID, TC_CARD_TYPE_NAME, TB_E_ID, TB_CARD_TYPE_NAME,
       MS1_E_ID, MS1_CARD_TYPE_NAME, MS2_E_ID, MS2_CARD_TYPE_NAME,
       TNV_MULTI_SINGLE, TC_MULTI_SINGLE, TB_MULTI_SINGLE,
       TNV_MSISDN, TNV_MS_STAT, TNV_MS_VALID, TNV_DAT_MSISDN, TNV_DAT_STAT, TNV_DAT_VALID,
       TNV_FAX_MSISDN, TNV_FAX_STAT, TNV_FAX_VALID,
       TC_MSISDN, TC_MS_STAT, TC_MS_VALID, TC_DAT_MSISDN, TC_DAT_STAT, TC_DAT_VALID,
       TC_FAX_MSISDN, TC_FAX_STAT, TC_FAX_VALID,
       TB_MSISDN, TB_MS_STAT, TB_MS_VALID, TB_DAT_MSISDN, TB_DAT_STAT, TB_DAT_VALID,
       TB_FAX_MSISDN, TB_FAX_STAT, TB_FAX_VALID,
       MS1_MSISDN, MS1_MS_STAT, MS1_MS_VALID, MS2_MSISDN, MS2_MS_STAT, MS2_MS_VALID,
       DA_MSISDN, DA_MS_STAT, DA_MS_VALID,
       VDA_MSISDN, VDA_MS_STAT, VDA_MS_VALID,
       TK_MSISDN, TK_MS_STAT, TK_MS_VALID,
       BCP_VERTRAG, BCP_ICCID, BCP_HLR, APN, BCP_TN_TEL,
       DATA_OPTION_REIN, VOICE_OPTION_REIN, MIX_OPTION, MULTI_OPTION, ROAMING_OPTION, SONSTIGE_OPTION,
       MS3_ICCID, MS3_E_ID, MS3_CARD_TYPE_NAME, MS3_MCC, MS3_MNC, MS3_HLR, MS3_SI, MS3_STAT, MS3_VALID,
       MS4_ICCID, MS4_E_ID, MS4_CARD_TYPE_NAME, MS4_MCC, MS4_MNC, MS4_HLR, MS4_SI, MS4_STAT, MS4_VALID,
       MS5_ICCID, MS5_E_ID, MS5_CARD_TYPE_NAME, MS5_MCC, MS5_MNC, MS5_HLR, MS5_SI, MS5_STAT, MS5_VALID,
       MS6_ICCID, MS6_E_ID, MS6_CARD_TYPE_NAME, MS6_MCC, MS6_MNC, MS6_HLR, MS6_SI, MS6_STAT, MS6_VALID,
       MS7_ICCID, MS7_E_ID, MS7_CARD_TYPE_NAME, MS7_MCC, MS7_MNC, MS7_HLR, MS7_SI, MS7_STAT, MS7_VALID,
       MS8_ICCID, MS8_E_ID, MS8_CARD_TYPE_NAME, MS8_MCC, MS8_MNC, MS8_HLR, MS8_SI, MS8_STAT, MS8_VALID,
       MS9_ICCID, MS9_E_ID, MS9_CARD_TYPE_NAME, MS9_MCC, MS9_MNC, MS9_HLR, MS9_SI, MS9_STAT, MS9_VALID,
       MS10_ICCID, MS10_E_ID, MS10_CARD_TYPE_NAME, MS10_MCC, MS10_MNC, MS10_HLR, MS10_SI, MS10_STAT, MS10_VALID
      )
      SELECT
        cn.cntrct_id,
        ev.evn,
        icc.tn_iccid,
        icc.tn_imsi_mcc,
        icc.tn_imsi_mnc,
        icc.tn_imsi_hlr,
        icc.tn_imsi_si,
        icc.tn_status,
        icc.tn_valid_to,
        icc.tc_iccid,
        icc.tc_imsi_mcc,
        icc.tc_imsi_mnc,
        icc.tc_imsi_hlr,
        icc.tc_imsi_si,
        icc.tc_status,
        icc.tc_valid_to,
        icc.tb_iccid,
        icc.tb_imsi_mcc,
        icc.tb_imsi_mnc,
        icc.tb_imsi_hlr,
        icc.tb_imsi_si,
        icc.tb_status,
        icc.tb_valid_to,
        icc.ms1_iccid,
        icc.ms1_imsi_mcc,
        icc.ms1_imsi_mnc,
        icc.ms1_imsi_hlr,
        icc.ms1_imsi_si,
        icc.ms1_status,
        icc.ms1_valid_to,
        icc.ms2_iccid,
        icc.ms2_imsi_mcc,
        icc.ms2_imsi_mnc,
        icc.ms2_imsi_hlr,
        icc.ms2_imsi_si,
        icc.ms2_status,
        icc.ms2_valid_to,
        icc.tn_e_id,
        icc.tn_card_type_name,
        icc.tc_e_id,
        icc.tc_card_type_name,
        icc.tb_e_id,
        icc.tb_card_type_name,
        icc.ms1_e_id,
        icc.ms1_card_type_name,
        icc.ms2_e_id,
        icc.ms2_card_type_name,
        msi.tn_multi_single,
        msi.tc_multi_single,
        msi.tb_multi_single,
        msi.tn_tel_msisdn,
        msi.tn_tel_status,
        msi.tn_tel_valid_to,
        msi.tn_dat_msisdn,
        msi.tn_dat_status,
        msi.tn_dat_valid_to,
        msi.tn_fax_msisdn,
        msi.tn_fax_status,
        msi.tn_fax_valid_to,
        msi.tc_tel_msisdn,
        msi.tc_tel_status,
        msi.tc_tel_valid_to,
        msi.tc_dat_msisdn,
        msi.tc_dat_status,
        msi.tc_dat_valid_to,
        msi.tc_fax_msisdn,
        msi.tc_fax_status,
        msi.tc_fax_valid_to,
        msi.tb_tel_msisdn,
        msi.tb_tel_status,
        msi.tb_tel_valid_to,
        msi.tb_dat_msisdn,
        msi.tb_dat_status,
        msi.tb_dat_valid_to,
        msi.tb_fax_msisdn,
        msi.tb_fax_status,
        msi.tb_fax_valid_to,
        msi.ms_rn_1_msisdn,
        msi.ms_rn_1_status,
        msi.ms_rn_1_valid_to,
        msi.ms_rn_2_msisdn,
        msi.ms_rn_2_status,
        msi.ms_rn_2_valid_to,
        msd.da_rn_msisdn,
        msd.da_rn_status,
        msd.da_rn_valid_to,
        msd.vda_rn_msisdn,
        msd.vda_rn_status,
        msd.vda_rn_valid_to,
        msd.tk_rn_msisdn,
        msd.tk_rn_status,
        msd.tk_rn_valid_to,
        bccm.cntrct_id_ref,
        bccm.tn_iccid,
        bccm.tn_imsi_hlr,
        CASE WHEN av.apn IS NULL THEN av.apn ELSE CONCAT(av.apn, ',', av.apn_cntrct) END, -- Oracle decode to BigQuery CASE
        bccm.tn_tel_msisdn,
        opt.data_option_rein,
        opt.voice_option_rein,
        opt.mix_option,
        opt.multi_option,
        opt.roaming_option,
        opt.sonstige_option,
        icc.ms3_iccid, icc.ms3_e_id, icc.ms3_card_type_name, icc.ms3_imsi_mcc, icc.ms3_imsi_mnc, icc.ms3_imsi_hlr, icc.ms3_imsi_si, icc.ms3_status, icc.ms3_valid_to,
        icc.ms4_iccid, icc.ms4_e_id, icc.ms4_card_type_name, icc.ms4_imsi_mcc, icc.ms4_imsi_mnc, icc.ms4_imsi_hlr, icc.ms4_imsi_si, icc.ms4_status, icc.ms4_valid_to,
        icc.ms5_iccid, icc.ms5_e_id, icc.ms5_card_type_name, icc.ms5_imsi_mcc, icc.ms5_imsi_mnc, icc.ms5_imsi_hlr, icc.ms5_imsi_si, icc.ms5_status, icc.ms5_valid_to,
        icc.ms6_iccid, icc.ms6_e_id, icc.ms6_card_type_name, icc.ms6_imsi_mcc, icc.ms6_imsi_mnc, icc.ms6_imsi_hlr, icc.ms6_imsi_si, icc.ms6_status, icc.ms6_valid_to,
        icc.ms7_iccid, icc.ms7_e_id, icc.ms7_card_type_name, icc.ms7_imsi_mcc, icc.ms7_imsi_mnc, icc.ms7_imsi_hlr, icc.ms7_imsi_si, icc.ms7_status, icc.ms7_valid_to,
        icc.ms8_iccid, icc.ms8_e_id, icc.ms8_card_type_name, icc.ms8_imsi_mcc, icc.ms8_imsi_mnc, icc.ms8_imsi_hlr, icc.ms8_imsi_si, icc.ms8_status, icc.ms8_valid_to,
        icc.ms9_iccid, icc.ms9_e_id, icc.ms9_card_type_name, icc.ms9_imsi_mcc, icc.ms9_imsi_mnc, icc.ms9_imsi_hlr, icc.ms9_imsi_si, icc.ms9_status, icc.ms9_valid_to,
        icc.ms10_iccid, icc.ms10_e_id, icc.ms10_card_type_name, icc.ms10_imsi_mcc, icc.ms10_imsi_mnc, icc.ms10_imsi_hlr, icc.ms10_imsi_si, icc.ms10_status, icc.ms10_valid_to
      FROM
        `project.dataset.sof_ta_cntrct_dist` AS cn
      LEFT JOIN
        (
        SELECT
              BC.CNTRCT_ID,
              BC.CNTRCT_ID_REF,
              BC.TN_ICCID,
              BC.TN_IMSI_HLR,
              BCM.TN_TEL_MSISDN
         FROM `project.dataset.sof_ta_bcp_iccid` AS BC
         INNER JOIN `project.dataset.sof_ta_bcp_msisdn` AS BCM
            ON BC.CNTRCT_ID    = BCM.CNTRCT_ID
           AND BC.CNTRCT_ID_REF = BCM.CNTRCT_ID_REF
       ) AS bccm
      ON cn.cntrct_id = bccm.cntrct_id
      LEFT JOIN `project.dataset.sof_ta_cntrct_evn` AS ev ON cn.cntrct_id = ev.cntrct_id
      LEFT JOIN `project.dataset.sof_ta_iccid_vertrag` AS icc ON cn.cntrct_id = icc.cntrct_id
      LEFT JOIN `project.dataset.sof_ta_rn_vertrag` AS msi ON cn.cntrct_id = msi.cntrct_id
      LEFT JOIN `project.dataset.sof_ta_rn_da_vda_tk` AS msd ON cn.cntrct_id = msd.cntrct_id
      LEFT JOIN `project.dataset.sof_ta_tarifoption` AS opt ON cn.cntrct_id = opt.cntrct_id
      LEFT JOIN `project.dataset.sof_ta_apn_vertrag` AS av ON cn.cntrct_id = av.cntrct_id
      ;
    """); -- End of EXECUTE IMMEDIATE

    -- Capture record count
    SELECT COUNT(*) INTO v_records FROM `project.dataset.sof_ta_p_basisprod`;

    SET v_end_time = CURRENT_TIMESTAMP();
    SET v_status = 'COMPLETED';

    -- Log successful completion
    UPDATE `project.dataset.job_audit_table`
    SET
      end_time = v_end_time,
      status = v_status,
      records_processed = v_records,
      message = 'Job completed successfully.'
    WHERE
      job_name = 'k_ausd_bp_ta_p_basisprod.ksh' AND
      job_kennung = p_JobKennung AND
      eintrags_nr = p_EintragsNr AND
      stichtag = v_stichtag_date AND
      start_time = v_start_time;

  EXCEPTION WHEN ERROR THEN
    SET v_end_time = CURRENT_TIMESTAMP();
    SET v_status = 'FAILED';
    SET v_error_message = CONCAT('Job failed: ', @@error.message);

    -- Log failure
    UPDATE `project.dataset.job_audit_table`
    SET
      end_time = v_end_time,
      status = v_status,
      message = v_error_message
    WHERE
      job_name = 'k_ausd_bp_ta_p_basisprod.ksh' AND
      job_kennung = p_JobKennung AND
      eintrags_nr = p_EintragsNr AND
      stichtag = v_stichtag_date AND
      start_time = v_start_time;

    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
  END;
END;