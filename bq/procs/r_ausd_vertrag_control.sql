-- BigQuery Stored Procedure r_ausd_vertrag_control
-- Replaces: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_geschaeftspartner.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_geschaeftspartner.ksh

CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_vertrag_control`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING,
  IN p_Stichtag STRING, -- Passed as STRING, then parsed to DATE
  IN p_wiederanlaufWert INT64
)
OPTIONS(description="Main orchestration procedure for business partner data processing, replacing k_ausd_geschaeftspartner.ksh.")
BEGIN
  DECLARE v_TabName STRING DEFAULT 'PoolVertrag';
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_sql_procedure_name STRING DEFAULT 'project.dataset.d_ausd_geschaeftspartner_proc';
  DECLARE v_datum_heute DATE DEFAULT CURRENT_DATE();
  DECLARE v_datum_gestern DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
  DECLARE v_stichtag_date DATE;
  DECLARE v_restart INT64 DEFAULT 0;

  -- Block for parameter validation and error logging
  BEGIN
    IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
      RAISE USING MESSAGE = 'Jobkennung fehlt';
    END IF;
    IF p_Stichtag IS NULL OR TRIM(p_Stichtag) = '' THEN
      RAISE USING MESSAGE = 'Stichtag fehlt';
    END IF;
    IF p_EintragsNr IS NULL OR TRIM(p_EintragsNr) = '' THEN
      RAISE USING MESSAGE = 'EintragsNr fehlt';
    END IF;
  EXCEPTION WHEN ERROR THEN
    INSERT INTO `project.dataset.job_error_log` (job_name, entry_nr, stichtag, error_message, created_ts)
    VALUES (p_JobKennung, p_EintragsNr, NULL, @@error.message, CURRENT_TIMESTAMP());
    RAISE; -- Re-raise the error after logging
  END;

  -- Date Validation (DDMMYYYY format)
  SET v_stichtag_date = SAFE.PARSE_DATE('%d%m%Y', p_Stichtag);
  IF v_stichtag_date IS NULL THEN
    INSERT INTO `project.dataset.job_error_log` (job_name, entry_nr, stichtag, error_message, created_ts)
    VALUES (p_JobKennung, p_EintragsNr, NULL, 'Datum hat nicht das Format DDMMYYYY: ' || p_Stichtag, CURRENT_TIMESTAMP());
    RAISE USING MESSAGE = 'Datum hat nicht das Format DDMMYYYY';
  END IF;

  -- Initialize restart value
  IF p_wiederanlaufWert IS NULL THEN
    SET v_restart = 0;
  ELSE
    SET v_restart = p_wiederanlaufWert;
  END IF;

  -- Deactivation of old active jobs (as per legacy script's commented functionality)
  -- This logic should be reviewed to ensure it aligns with current business requirements.
  -- The original script had this commented out, so it's an optional, but designed, migration piece.
  UPDATE `project.dataset.job_table`
  SET
    active_flag = 'N',
    last_updated_ts = CURRENT_TIMESTAMP()
  WHERE
    tab_name = v_TabName AND active_flag = 'A';

  -- Call the migrated SQL logic procedure
  -- Wrap in a BEGIN...EXCEPTION block to catch errors from the child procedure
  BEGIN
    CALL `project.dataset.d_ausd_geschaeftspartner_proc`(
      p_EintragsNr,
      p_JobKennung,
      v_stichtag_date,
      v_restart,
      v_datum_heute,
      v_datum_gestern,
      v_records -- OUT parameter
    );

    -- Log successful run
    INSERT INTO `project.dataset.job_run_log`
    (tab_name, job_kennung, eintrags_nr, stichtag, records_processed, status, created_ts)
    VALUES (v_TabName, p_JobKennung, p_EintragsNr, v_stichtag_date, v_records, 'SUCCESS', CURRENT_TIMESTAMP());

    -- Update job control table with new active entry
    INSERT INTO `project.dataset.job_table`
    (tab_name, active_flag, process_flag, from_date, to_date, job_type, restart_flag, record_count, description, last_updated_ts)
    VALUES (v_TabName, 'A', 'I', v_stichtag_date, v_stichtag_date, 'J', 'N', v_records, 'Initialbefuellung', CURRENT_TIMESTAMP());

    SELECT ' ---------- ENDE Datenverarbeitung ----------' AS message;

  EXCEPTION WHEN ERROR THEN
    -- Log error from the child procedure
    INSERT INTO `project.dataset.job_error_log` (job_name, entry_nr, stichtag, error_message, created_ts)
    VALUES (p_JobKennung, p_EintragsNr, v_stichtag_date, 'Error during child procedure execution: ' || @@error.message, CURRENT_TIMESTAMP());

    -- Optionally, update job_table status to FAILED or similar
    -- UPDATE `project.dataset.job_table`
    -- SET
    --   active_flag = 'N',
    --   process_flag = 'F', -- Failed
    --   last_updated_ts = CURRENT_TIMESTAMP()
    -- WHERE
    --   tab_name = v_TabName AND active_flag = 'A'; -- Or target the specific entry if job_table allows unique run identification

    RAISE; -- Re-raise the error after logging
  END;

END;