CREATE OR REPLACE PROCEDURE `metadata.dw_init`(
  INOUT io_oracle_home STRING,
  IN io_oracle_sid STRING,
  IN i_home_dir STRING,
  -- Input flags to simulate filesystem checks (-d /appl/local/oracle/...)
  IN i_dir_oracle_12_exists BOOLEAN,
  IN i_dir_oracle_11_exists BOOLEAN
)
BEGIN
  -- Declaring environment path variables
  DECLARE DW_DIR_ROOT STRING;
  DECLARE DW_DIR_PROT STRING;
  DECLARE DW_DIR_CUBES STRING;
  DECLARE DW_DIR_IMP_D1 STRING;
  DECLARE DW_DIR_IMP_BWA STRING;
  DECLARE DW_DIR_IMP_XTRA STRING;
  DECLARE DW_DIR_IMP_CTEL STRING;
  DECLARE DW_DIR_IMP_VO STRING;
  DECLARE DW_DIR_IMP_RV STRING;
  DECLARE DW_DIR_IMP_IF STRING;
  DECLARE DW_DIR_IMP_NNV STRING;
  DECLARE DW_DIR_IMP_SIGMA STRING;
  DECLARE DW_DIR_EXP_SIGMA STRING;
  DECLARE DW_DIR_IMP_TRF STRING;
  DECLARE DW_DIR_IMP_AUF STRING;
  DECLARE DW_DIR_IMP_GUT STRING;
  DECLARE DW_DIR_IMP_KDG STRING;
  DECLARE DW_DIR_IMP_MP_KDG STRING;
  DECLARE DW_DIR_IMP_MP_TS STRING;
  DECLARE DW_DIR_IMP_MP_ZM STRING;
  DECLARE DW_DIR_IMP_TS STRING;
  DECLARE DW_DIR_IMP_ZM STRING;
  DECLARE DW_DIR_EXP STRING;
  DECLARE DW_DIR_IMP_BPM STRING;
  DECLARE DW_DIR_IMP_ZTS STRING;
  DECLARE DW_DIR_IMP_VRS STRING;
  DECLARE DW_DIR_IMP_BRUNET STRING;
  DECLARE DW_DIR_IMP_DWH STRING;
  DECLARE DW_DIR_IMP_PLATO STRING;
  DECLARE DW_DIR_IMP_CARMEN STRING;
  DECLARE DW_DIR_IMP_SAP STRING;
  DECLARE DW_DIR_IMP_SR_RV STRING;
  DECLARE DW_DIR_IMP_SAP_L STRING;
  DECLARE DW_DIR_IMP_L_MAHNSTYP_IST STRING;
  DECLARE DW_DIR_IMP_L_MAHNV_FI STRING;
  DECLARE DW_DIR_IMP_L_MAHNV_IST STRING;
  DECLARE DW_DIR_IMP_L_GUTGR STRING;
  DECLARE DW_DIR_IMP_L_LEIST STRING;
  DECLARE DW_DIR_IMP_L_PROD STRING;
  DECLARE DW_DIR_IMP_LKODE STRING;
  DECLARE DW_DIR_IMP_SUBSE STRING;
  DECLARE DW_DIR_SMS_PRG STRING;
  DECLARE DW_DIR_SMS_ADR STRING;
  DECLARE DW_DIR_SMS_TMP STRING;
  DECLARE DW_DIR_IMP_DPPS STRING;
  DECLARE DW_DIR_IMP_PLANF2 STRING;
  DECLARE DW_HOST_CUSTOMER STRING;
  DECLARE DW_DIR_UTL_FILE STRING;

  -- Declaring specific variables to receive OUT parameters from metadata.sp_dw_global
  DECLARE v_out_nls_lang STRING;
  DECLARE v_out_nls_date_format STRING;
  DECLARE v_out_nls_date_language STRING;
  DECLARE v_out_lang STRING;

  -- 1. Initialize environment directory paths
  SET DW_DIR_ROOT = CONCAT(i_home_dir, '/aktuell');
  SET DW_DIR_PROT = CONCAT(i_home_dir, '/daten/logfiles');
  SET DW_DIR_CUBES = CONCAT(i_home_dir, '/daten/cubes');

  SET DW_DIR_IMP_D1 = CONCAT(i_home_dir, '/daten/d1');
  SET DW_DIR_IMP_BWA = CONCAT(i_home_dir, '/daten/dpps/bwa');
  SET DW_DIR_IMP_XTRA = CONCAT(i_home_dir, '/daten/xtra');
  SET DW_DIR_IMP_CTEL = CONCAT(i_home_dir, '/daten/ctel');
  SET DW_DIR_IMP_VO = CONCAT(i_home_dir, '/daten/vo');
  SET DW_DIR_IMP_RV = CONCAT(i_home_dir, '/daten/rv');
  SET DW_DIR_IMP_IF = CONCAT(i_home_dir, '/daten/ees');
  SET DW_DIR_IMP_NNV = CONCAT(i_home_dir, '/daten/nnv');
  SET DW_DIR_IMP_SIGMA = CONCAT(i_home_dir, '/daten/gd/sigma');
  SET DW_DIR_EXP_SIGMA = CONCAT(i_home_dir, '/daten/gd/sigma/export');
  SET DW_DIR_IMP_TRF = CONCAT(i_home_dir, '/daten/trf');
  SET DW_DIR_IMP_AUF = CONCAT(i_home_dir, '/daten/sd/auf');
  SET DW_DIR_IMP_GUT = CONCAT(i_home_dir, '/daten/sd/gut');
  SET DW_DIR_IMP_KDG = CONCAT(i_home_dir, '/daten/sd/kdg');
  SET DW_DIR_IMP_MP_KDG = CONCAT(i_home_dir, '/daten/mp/kdg');
  SET DW_DIR_IMP_MP_TS = CONCAT(i_home_dir, '/daten/mp/ts');
  SET DW_DIR_IMP_MP_ZM = CONCAT(i_home_dir, '/daten/mp/zm');
  SET DW_DIR_IMP_TS = CONCAT(i_home_dir, '/daten/sd/ts');
  SET DW_DIR_IMP_ZM = CONCAT(i_home_dir, '/daten/sd/zm');
  SET DW_DIR_EXP = CONCAT(i_home_dir, '/daten/exporter');
  SET DW_DIR_IMP_BPM = CONCAT(i_home_dir, '/daten/bm');
  SET DW_DIR_IMP_ZTS = CONCAT(i_home_dir, '/daten/zts');
  SET DW_DIR_IMP_VRS = CONCAT(i_home_dir, '/daten/vrs');

  SET DW_DIR_IMP_BRUNET = CONCAT(i_home_dir, '/daten/brunet');
  SET DW_DIR_IMP_DWH = CONCAT(i_home_dir, '/daten/dwh');
  SET DW_DIR_IMP_PLATO = CONCAT(i_home_dir, '/daten/dwh/plato');
  SET DW_DIR_IMP_CARMEN = CONCAT(i_home_dir, '/daten/carmen');
  SET DW_DIR_IMP_SAP = CONCAT(i_home_dir, '/daten/sap');
  SET DW_DIR_IMP_SR_RV = CONCAT(i_home_dir, '/daten/sap/sr_rv_dpps');
  SET DW_DIR_IMP_SAP_L = CONCAT(i_home_dir, '/daten/sap/sap_l_gutgr');
  SET DW_DIR_IMP_L_MAHNSTYP_IST = CONCAT(i_home_dir, '/daten/sap/mahn');
  SET DW_DIR_IMP_L_MAHNV_FI = CONCAT(i_home_dir, '/daten/sap/mahn');
  SET DW_DIR_IMP_L_MAHNV_IST = CONCAT(i_home_dir, '/daten/sap/mahn');
  SET DW_DIR_IMP_L_GUTGR = CONCAT(i_home_dir, '/daten/sd/l_gutschr');
  SET DW_DIR_IMP_L_LEIST = CONCAT(i_home_dir, '/daten/sd/l_leist');
  SET DW_DIR_IMP_L_PROD = CONCAT(i_home_dir, '/daten/sd/l_prod');
  SET DW_DIR_IMP_LKODE = CONCAT(i_home_dir, '/daten/sd/lkode');

  SET DW_DIR_IMP_SUBSE = CONCAT(i_home_dir, '/daten/subse');

  SET DW_DIR_SMS_PRG = CONCAT(i_home_dir, '/aktuell/allgemein/is/util');
  SET DW_DIR_SMS_ADR = CONCAT(i_home_dir, '/daten/sms/adressen');
  SET DW_DIR_SMS_TMP = CONCAT(i_home_dir, '/daten/sms/tmp');

  SET DW_DIR_IMP_DPPS = CONCAT(i_home_dir, '/daten/dpps');
  SET DW_DIR_IMP_PLANF2 = CONCAT(i_home_dir, '/daten/planf2');

  -- Remote Host configuration
  SET DW_HOST_CUSTOMER = 'dxcst3.bn.detemobil.de';

  -- 2. Determine ORACLE_HOME dynamically based on system flags if not already provided
  IF io_oracle_home IS NULL OR io_oracle_home = '' THEN
    IF i_dir_oracle_12_exists THEN
      SET io_oracle_home = '/appl/local/oracle/12.2.0.1.0';
    ELSEIF i_dir_oracle_11_exists THEN
      SET io_oracle_home = '/appl/local/oracle/11.2.0';
    ELSE
      -- Log configuration failure and raise exception with valid syntax
      RAISE USING MESSAGE = 'Fehler in .dw_init: Konnte ORACLE_HOME nicht setzen !';
    END IF;
  END IF;

  -- 3. Execute Global and Local Configurations (Sourced Scripts)
  -- Call metadata.sp_dw_global passing declared output variables instead of placeholders
  CALL `metadata.sp_dw_global`(
    DW_DIR_ROOT, DW_DIR_PROT, DW_DIR_CUBES, DW_DIR_IMP_D1, DW_DIR_IMP_XTRA, 
    DW_DIR_IMP_CTEL, DW_DIR_IMP_VO, DW_DIR_IMP_RV, DW_DIR_IMP_IF, DW_DIR_IMP_NNV, 
    io_oracle_home, v_out_nls_lang, v_out_nls_date_format, v_out_nls_date_language, v_out_lang
  );

  -- 4. Calculate final derived variable path
  SET DW_DIR_UTL_FILE = CONCAT('/appl/local/oracle/admin/', io_oracle_sid, '/utl_file');

END;