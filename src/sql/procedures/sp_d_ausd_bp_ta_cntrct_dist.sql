-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_cntrct_dist.ksh
-- Target: BigQuery Stored Procedure for contract distribution and auditing

CREATE OR REPLACE PROCEDURE `project.dataset.sp_d_ausd_bp_ta_cntrct_dist`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING,
  IN p_Stichtag STRING,
  IN p_wiederanlaufWert STRING
)
BEGIN
  DECLARE v_stichtag_date DATE;
  DECLARE v_datum_heute DATE;
  DECLARE v_datum_gestern DATE;
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_audit_id STRING DEFAULT GENERATE_UUID();
  DECLARE v_status STRING DEFAULT 'S';
  DECLARE v_error_message STRING DEFAULT NULL;

  -- =========================================================
  -- 1. Parameter Validation and Date Logic
  -- =========================================================
  IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
    RAISE USING MESSAGE = 'Missing mandatory parameter: p_JobKennung';
  END IF;

  IF p_EintragsNr IS NULL OR TRIM(p_EintragsNr) = '' THEN
    RAISE USING MESSAGE = 'Missing mandatory parameter: p_EintragsNr';
  END IF;

  IF p_Stichtag IS NULL OR TRIM(p_Stichtag) = '' THEN
    RAISE USING MESSAGE = 'Missing mandatory parameter: p_Stichtag';
  END IF;

  SET v_stichtag_date = SAFE.PARSE_DATE('%d%m%Y', p_Stichtag);

  IF v_stichtag_date IS NULL THEN
    RAISE USING MESSAGE = 'Invalid Stichtag format. Expected DDMMYYYY.';
  END IF;

  SET v_datum_heute = v_stichtag_date;
  SET v_datum_gestern = DATE_SUB(v_stichtag_date, INTERVAL 1 DAY);

  -- =========================================================
  -- 2. Core Transformation Logic (Transformed SQL Core)
  -- =========================================================
  BEGIN
    -- Remove any pre-existing records for the same reporting date (Stichtag)
    DELETE FROM `project.dataset.PoolBasisprodukt`
    WHERE stichtag = v_stichtag_date;

    -- Process and populate data from Staging into Target Table
    INSERT INTO `project.dataset.PoolBasisprodukt` (
      stichtag,
      datum_heute,
      datum_gestern,
      job_kennung,
      eintrags_nr,
      contract_id,
      distribution_channel,
      account_balance,
      load_timestamp
    )
    SELECT
      v_stichtag_date,
      v_datum_heute,
      v_datum_gestern,
      p_JobKennung,
      p_EintragsNr,
      stg.contract_id,
      stg.distribution_channel,
      stg.account_balance,
      CURRENT_TIMESTAMP()
    FROM `project.dataset.PoolBasisprodukt_Staging` AS stg
    WHERE stg.stichtag = v_stichtag_date;

    -- Capture processing record count
    SET v_records = @@row_count;

  EXCEPTION WHEN ERROR THEN
    SET v_status = 'F';
    SET v_error_message = @@error.message;
    
    INSERT INTO `project.dataset.job_audit_log` (
      audit_id,
      tab_name,
      status,
      type_code,
      stichtag_from,
      stichtag_to,
      active_flag,
      record_count,
      job_kennung,
      eintrags_nr,
      wiederanlauf_wert,
      run_timestamp
    )
    VALUES (
      v_audit_id,
      'PoolBasisprodukt',
      'F',
      'I',
      v_stichtag_date,
      v_stichtag_date,
      'N',
      v_records,
      p_JobKennung,
      p_EintragsNr,
      p_wiederanlaufWert,
      CURRENT_TIMESTAMP()
    );

    RAISE USING MESSAGE = CONCAT(
      'sp_d_ausd_bp_ta_cntrct_dist execution failed: ',
      COALESCE(v_error_message, 'unknown database exception')
    );
  END;

  -- =========================================================
  -- 3. Execution Auditing - Success Recording
  -- =========================================================
  INSERT INTO `project.dataset.job_audit_log` (
    audit_id,
    tab_name,
    status,
    type_code,
    stichtag_from,
    stichtag_to,
    active_flag,
    record_count,
    job_kennung,
    eintrags_nr,
    wiederanlauf_wert,
    run_timestamp
  )
  VALUES (
    v_audit_id,
    'PoolBasisprodukt',
    v_status,
    'I',
    v_stichtag_date,
    v_stichtag_date,
    'N',
    v_records,
    p_JobKennung,
    p_EintragsNr,
    p_wiederanlaufWert,
    CURRENT_TIMESTAMP()
  );

END;