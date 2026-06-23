-- BigQuery Stored Procedure for orchestrating data processing
-- Replaces the functionality of k_ausd_bp_ta_cntrct_dist.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_cntrct_dist.ksh

CREATE OR REPLACE PROCEDURE `your_project_id.your_dataset_id.r_ausd_bp_ta_cntrct_dist`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING,
  IN p_Stichtag_raw STRING, -- Raw string input for date validation
  IN p_wiederanlaufWert_raw STRING -- Raw string input for restart value
)
BEGIN
  DECLARE v_ErrNr INT64 DEFAULT 0;
  DECLARE v_ErrArg STRING DEFAULT '';
  DECLARE v_TabName STRING DEFAULT 'PoolBasisprodukt';
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_datum_heute DATE DEFAULT CURRENT_DATE();
  DECLARE v_datum_gestern DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
  DECLARE v_Stichtag DATE; -- Validated and parsed date
  DECLARE v_wiederanlaufWert STRING;
  DECLARE v_start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

  -- Log job start
  INSERT INTO `your_project_id.your_dataset_id.job_log` (
    job_id, entry_number, reference_date, start_time, status, comment
  ) VALUES (
    p_JobKennung, p_EintragsNr, SAFE.PARSE_DATE('%d%m%Y', p_Stichtag_raw), v_start_time, 'RUNNING', 'Job started'
  );

  -- Parameter validation
  IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
    SET v_ErrNr = 193; -- Equivalent to "Notwendiges Argument fehlt"
    SET v_ErrArg = 'Jobkennung';
  END IF;

  IF v_ErrNr = 0 AND (p_Stichtag_raw IS NULL OR TRIM(p_Stichtag_raw) = '') THEN
    SET v_ErrNr = 193;
    SET v_ErrArg = 'Stichtag';
  END IF;

  IF v_ErrNr = 0 AND (p_EintragsNr IS NULL OR TRIM(p_EintragsNr) = '') THEN
    SET v_ErrNr = 193;
    SET v_ErrArg = 'EintragsNr';
  END IF;

  IF v_ErrNr <> 0 THEN
    INSERT INTO `your_project_id.your_dataset_id.error_log` (
      job_id, entry_number, error_message, component, severity
    ) VALUES (
      p_JobKennung, p_EintragsNr, FORMAT('FEHLER: 0 E %d %s - Bitte ueber Rahmenscript aufrufen', v_ErrNr, v_ErrArg), 'r_ausd_bp_ta_cntrct_dist', 'ERROR'
    );
    RAISE USING MESSAGE = FORMAT('Parameter validation failed: Missing required argument %s', v_ErrArg);
  END IF;

  -- Date validation
  SET v_Stichtag = SAFE.PARSE_DATE('%d%m%Y', p_Stichtag_raw);
  IF v_Stichtag IS NULL THEN
    INSERT INTO `your_project_id.your_dataset_id.error_log` (
      job_id, entry_number, reference_date, error_message, component, severity
    ) VALUES (
      p_JobKennung, p_EintragsNr, NULL, FORMAT('Ungueltiges Datum im Format DDMMYYYY: %s', p_Stichtag_raw), 'r_ausd_bp_ta_cntrct_dist', 'ERROR'
    );
    RAISE USING MESSAGE = FORMAT('Invalid date format for Stichtag: %s. Expected DDMMYYYY.', p_Stichtag_raw);
  END IF;

  -- Default restart value
  SET v_wiederanlaufWert = COALESCE(TRIM(p_wiederanlaufWert_raw), '0');

  -- Call the core business logic procedure
  CALL `your_project_id.your_dataset_id.d_ausd_bp_ta_cntrct_dist_core`(
    p_EintragsNr,
    p_JobKennung,
    v_Stichtag,
    v_datum_heute,
    v_datum_gestern
  );

  -- Record count capture from the target table
  SET v_records = (
    SELECT COUNT(*)\n    FROM `your_project_id.your_dataset_id.PoolBasisprodukt`
    WHERE stichtag = v_Stichtag
  );

  -- Log job end
  UPDATE `your_project_id.your_dataset_id.job_log`
  SET
    end_time = CURRENT_TIMESTAMP(),
    status = 'SUCCESS',
    record_count = v_records,
    restart_value = v_wiederanlaufWert,
    comment = 'Job completed successfully'
  WHERE job_id = p_JobKennung AND entry_number = p_EintragsNr AND start_time = v_start_time;

  SELECT '---------- ENDE Datenverarbeitung ----------' AS message, v_records AS records_processed;

EXCEPTION WHEN ERROR THEN
  -- Log error in job_log and error_log
  UPDATE `your_project_id.your_dataset_id.job_log`
  SET
    end_time = CURRENT_TIMESTAMP(),
    status = 'FAILED',
    comment = ERROR_MESSAGE()
  WHERE job_id = p_JobKennung AND entry_number = p_EintragsNr AND start_time = v_start_time;

  INSERT INTO `your_project_id.your_dataset_id.error_log` (
    job_id, entry_number, reference_date, error_message, component, severity
  ) VALUES (
    p_JobKennung, p_EintragsNr, v_Stichtag, ERROR_MESSAGE(), 'r_ausd_bp_ta_cntrct_dist', 'ERROR'
  );
  RAISE; -- Re-raise the error to propagate it to the caller (e.g., Airflow)
END;