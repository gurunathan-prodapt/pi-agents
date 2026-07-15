CREATE OR REPLACE PROCEDURE `GCP_PROJECT.BQ_DATASET.dw_init`(
  IN p_home_uri STRING,
  IN p_oracle_sid STRING,
  OUT v_dw_dir_root STRING,
  OUT v_dw_dir_prot STRING,
  OUT v_dw_dir_cubes STRING,
  OUT v_dw_dir_imp_d1 STRING,
  OUT v_dw_dir_imp_bwa STRING,
  OUT v_dw_dir_imp_xtra STRING,
  OUT v_dw_dir_imp_ctel STRING,
  OUT v_dw_dir_imp_vo STRING,
  OUT v_dw_dir_imp_rv STRING,
  OUT v_dw_dir_imp_if STRING,
  OUT v_dw_dir_imp_nnv STRING,
  OUT v_dw_dir_imp_sigma STRING,
  OUT v_dw_dir_exp_sigma STRING,
  OUT v_dw_dir_imp_trf STRING,
  OUT v_dw_dir_imp_auf STRING,
  OUT v_dw_dir_imp_gut STRING,
  OUT v_dw_dir_imp_kdg STRING,
  OUT v_dw_dir_imp_mp_kdg STRING,
  OUT v_dw_dir_imp_mp_ts STRING,
  OUT v_dw_dir_imp_mp_zm STRING,
  OUT v_dw_dir_imp_ts STRING,
  OUT v_dw_dir_imp_zm STRING,
  OUT v_dw_dir_exp STRING,
  OUT v_dw_dir_imp_bpm STRING,
  OUT v_dw_dir_imp_zts STRING,
  OUT v_dw_dir_imp_vrs STRING,
  OUT v_dw_dir_imp_brunet STRING,
  OUT v_dw_dir_imp_dwh STRING,
  OUT v_dw_dir_imp_plato STRING,
  OUT v_dw_dir_imp_carmen STRING,
  OUT v_dw_dir_imp_sap STRING,
  OUT v_dw_dir_imp_sr_rv STRING,
  OUT v_dw_dir_imp_sap_l STRING,
  OUT v_dw_dir_imp_l_mahnstyp_ist STRING,
  OUT v_dw_dir_imp_l_mahnv_fi STRING,
  OUT v_dw_dir_imp_l_mahnv_ist STRING,
  OUT v_dw_dir_imp_l_gutgr STRING,
  OUT v_dw_dir_imp_l_leist STRING,
  OUT v_dw_dir_imp_l_prod STRING,
  OUT v_dw_dir_imp_lkode STRING,
  OUT v_dw_dir_imp_subse STRING,
  OUT v_dw_dir_sms_prg STRING,
  OUT v_dw_dir_sms_adr STRING,
  OUT v_dw_dir_sms_tmp STRING,
  OUT v_dw_dir_imp_dpps STRING,
  OUT v_dw_dir_imp_planf2 STRING,
  OUT v_dw_host_customer STRING,
  INOUT v_oracle_home STRING,
  OUT v_dw_dir_utl_file STRING
)
BEGIN
  -- Temporary diagnostic variables for directory validation simulation
  DECLARE dir_check_12_2 BOOLEAN DEFAULT FALSE;
  DECLARE dir_check_11_2 BOOLEAN DEFAULT FALSE;

  -- 1. Initialize Root, Logs, and Cube Directories (GCS URI representations)
  SET v_dw_dir_root = CONCAT(p_home_uri, '/aktuell');
  SET v_dw_dir_prot = CONCAT(p_home_uri, '/daten/logfiles');
  SET v_dw_dir_cubes = CONCAT(p_home_uri, '/daten/cubes');

  -- 2. Initialize Import and Export Directory Paths
  SET v_dw_dir_imp_d1 = CONCAT(p_home_uri, '/daten/d1');
  SET v_dw_dir_imp_bwa = CONCAT(p_home_uri, '/daten/dpps/bwa');
  SET v_dw_dir_imp_xtra = CONCAT(p_home_uri, '/daten/xtra');
  SET v_dw_dir_imp_ctel = CONCAT(p_home_uri, '/daten/ctel');
  SET v_dw_dir_imp_vo = CONCAT(p_home_uri, '/daten/vo');
  SET v_dw_dir_imp_rv = CONCAT(p_home_uri, '/daten/rv');
  SET v_dw_dir_imp_if = CONCAT(p_home_uri, '/daten/ees');
  SET v_dw_dir_imp_nnv = CONCAT(p_home_uri, '/daten/nnv');
  SET v_dw_dir_imp_sigma = CONCAT(p_home_uri, '/daten/gd/sigma');
  SET v_dw_dir_exp_sigma = CONCAT(p_home_uri, '/daten/gd/sigma/export');
  SET v_dw_dir_imp_trf = CONCAT(p_home_uri, '/daten/trf');
  SET v_dw_dir_imp_auf = CONCAT(p_home_uri, '/daten/sd/auf');
  SET v_dw_dir_imp_gut = CONCAT(p_home_uri, '/daten/sd/gut');
  SET v_dw_dir_imp_kdg = CONCAT(p_home_uri, '/daten/sd/kdg');
  SET v_dw_dir_imp_mp_kdg = CONCAT(p_home_uri, '/daten/mp/kdg');
  SET v_dw_dir_imp_mp_ts = CONCAT(p_home_uri, '/daten/mp/ts');
  SET v_dw_dir_imp_mp_zm = CONCAT(p_home_uri, '/daten/mp/zm');
  SET v_dw_dir_imp_ts = CONCAT(p_home_uri, '/daten/sd/ts');
  SET v_dw_dir_imp_zm = CONCAT(p_home_uri, '/daten/sd/zm');
  SET v_dw_dir_exp = CONCAT(p_home_uri, '/daten/exporter');
  SET v_dw_dir_imp_bpm = CONCAT(p_home_uri, '/daten/bm');
  SET v_dw_dir_imp_zts = CONCAT(p_home_uri, '/daten/zts');
  SET v_dw_dir_imp_vrs = CONCAT(p_home_uri, '/daten/vrs');
  SET v_dw_dir_imp_brunet = CONCAT(p_home_uri, '/daten/brunet');
  SET v_dw_dir_imp_dwh = CONCAT(p_home_uri, '/daten/dwh');
  SET v_dw_dir_imp_plato = CONCAT(p_home_uri, '/daten/dwh/plato');
  SET v_dw_dir_imp_carmen = CONCAT(p_home_uri, '/daten/carmen');
  SET v_dw_dir_imp_sap = CONCAT(p_home_uri, '/daten/sap');
  SET v_dw_dir_imp_sr_rv = CONCAT(p_home_uri, '/daten/sap/sr_rv_dpps');
  SET v_dw_dir_imp_sap_l = CONCAT(p_home_uri, '/daten/sap/sap_l_gutgr');
  SET v_dw_dir_imp_l_mahnstyp_ist = CONCAT(p_home_uri, '/daten/sap/mahn');
  SET v_dw_dir_imp_l_mahnv_fi = CONCAT(p_home_uri, '/daten/sap/mahn');
  SET v_dw_dir_imp_l_mahnv_ist = CONCAT(p_home_uri, '/daten/sap/mahn');
  SET v_dw_dir_imp_l_gutgr = CONCAT(p_home_uri, '/daten/sd/l_gutschr');
  SET v_dw_dir_imp_l_leist = CONCAT(p_home_uri, '/daten/sd/l_leist');
  SET v_dw_dir_imp_l_prod = CONCAT(p_home_uri, '/daten/sd/l_prod');
  SET v_dw_dir_imp_lkode = CONCAT(p_home_uri, '/daten/sd/lkode');
  SET v_dw_dir_imp_subse = CONCAT(p_home_uri, '/daten/subse');
  SET v_dw_dir_sms_prg = CONCAT(p_home_uri, '/aktuell/allgemein/is/util');
  SET v_dw_dir_sms_adr = CONCAT(p_home_uri, '/daten/sms/adressen');
  SET v_dw_dir_sms_tmp = CONCAT(p_home_uri, '/daten/sms/tmp');
  SET v_dw_dir_imp_dpps = CONCAT(p_home_uri, '/daten/dpps');
  SET v_dw_dir_imp_planf2 = CONCAT(p_home_uri, '/daten/planf2');

  -- 3. Set Remote Host Parameters
  SET v_dw_host_customer = 'dxcst3.bn.detemobil.de';

  -- 4. Environment Check for ORACLE_HOME
  IF v_oracle_home IS NULL OR v_oracle_home = '' THEN
    -- Check for system-wide database configuration tables on BigQuery
    SET dir_check_12_2 = EXISTS(SELECT 1 FROM `GCP_PROJECT.BQ_DATASET.system_paths` WHERE path = '/appl/local/oracle/12.2.0.1.0' AND active = TRUE);
    SET dir_check_11_2 = EXISTS(SELECT 1 FROM `GCP_PROJECT.BQ_DATASET.system_paths` WHERE path = '/appl/local/oracle/11.2.0' AND active = TRUE);

    IF dir_check_12_2 THEN
      SET v_oracle_home = '/appl/local/oracle/12.2.0.1.0';
    ELSEIF dir_check_11_2 THEN
      SET v_oracle_home = '/appl/local/oracle/11.2.0';
    ELSE
      -- Log structural mismatch error with exactly the legacy wording
      INSERT INTO `GCP_PROJECT.BQ_DATASET.error_logs` (log_time, module, message)
      VALUES (CURRENT_TIMESTAMP(), '.dw_init', 'Fehler in .dw_init:\n   Konnte ORACLE_HOME nicht setzen !');
    END IF;
  END IF;

  -- 5. Invoke downstream global config operations
  CALL `GCP_PROJECT.BQ_DATASET.dw_global`(
    v_dw_dir_root, v_dw_dir_prot, v_dw_dir_cubes, v_dw_dir_imp_d1,
    v_dw_dir_imp_xtra, v_dw_dir_imp_ctel, v_dw_dir_imp_vo, v_dw_dir_imp_rv,
    v_dw_dir_imp_if, v_dw_dir_imp_nnv, v_oracle_home
  );

  -- 6. Set dependent legacy variable configurations
  SET v_dw_dir_utl_file = CONCAT('/appl/local/oracle/admin/', p_oracle_sid, '/utl_file');
END;