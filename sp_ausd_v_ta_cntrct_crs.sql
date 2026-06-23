-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs.ksh
-- BigQuery Stored Procedure: control wrapper for ta_cntrct_crs processing
CREATE OR REPLACE PROCEDURE `project.dataset.sp_ausd_v_ta_cntrct_crs`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING
)
BEGIN
  DECLARE v_TabName STRING DEFAULT 'ta_cntrct_crs';
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_err_nr INT64 DEFAULT 0;
  DECLARE v_err_arg STRING DEFAULT '';
  DECLARE v_now TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

  -- Parameter validation
  IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
    SET v_err_nr = 193;
    SET v_err_arg = 'Jobkennung';
  END IF;

  IF p_EintragsNr IS NULL OR TRIM(p_EintragsNr) = '' THEN
    IF v_err_nr = 0 THEN -- Only set if no prior error
        SET v_err_nr = 193;
        SET v_err_arg = 'EintragsNr';
    END IF;
  END IF;

  IF v_err_nr <> 0 THEN
    INSERT INTO `project.dataset.job_error_log`
      (event_ts, procedure_name, err_nr, err_arg, message)
    VALUES
      (v_now, 'sp_ausd_v_ta_cntrct_crs', v_err_nr, v_err_arg, 'Bitte ueber Rahmenscript aufrufen');

    SELECT ERROR(CONCAT('FEHLER: 0 E ', CAST(v_err_nr AS STRING), ' ', v_err_arg));
  END IF;

  -- Job control: mark current job as active / insert job entry
  INSERT INTO `project.dataset.job_table`
    (job_kennung, eintrags_nr, tab_name, status, created_ts, updated_ts)
  VALUES
    (p_JobKennung, p_EintragsNr, v_TabName, 'ACTIVE', v_now, v_now);

  -- Deactivate older active jobs for same logical process
  UPDATE `project.dataset.job_table`
  SET status = 'INACTIVE',
      updated_ts = CURRENT_TIMESTAMP()
  WHERE tab_name = v_TabName
    AND status = 'ACTIVE'
    AND NOT (job_kennung = p_JobKennung AND eintrags_nr = p_EintragsNr);

  -- Core processing equivalent of d_ausd_v_ta_cntrct_crs.sql
  -- (*** Placeholder: Actual SQL logic from d_ausd_v_ta_cntrct_crs.sql goes here ***)
  -- The following is an example pattern and must be replaced with the actual content
  -- of d_ausd_v_ta_cntrct_crs.sql translated to BigQuery SQL.
  /*
  INSERT INTO `project.dataset.target_output_table` -- Replace with actual target table
  SELECT
    * -- Replace with actual columns and transformations
  FROM `project.dataset.source_input_table` -- Replace with actual source table
  WHERE job_kennung = p_JobKennung
    AND eintrags_nr = p_EintragsNr; -- Example filtering, adapt as per original SQL
  */
  -- Example:
  -- INSERT INTO `project.dataset.ta_cntrct_crs_target` (...)
  -- SELECT ... FROM `project.dataset.ta_cntrct_crs_source` ...;
  -- For now, let's assume no records are processed by this placeholder logic.
  SET v_records = 0; -- Set to 0 as a placeholder since actual logic is missing.
  -- You would typically set v_records = @@row_count; after an INSERT/UPDATE/DELETE statement.


  -- Persist record count / completion status
  UPDATE `project.dataset.job_table`
  SET status = 'DONE',
      record_count = v_records,
      updated_ts = CURRENT_TIMESTAMP()
  WHERE job_kennung = p_JobKennung
    AND eintrags_nr = p_EintragsNr
    AND tab_name = v_TabName;

  -- Optional completion log
  INSERT INTO `project.dataset.job_audit_log`
    (event_ts, procedure_name, job_kennung, eintrags_nr, tab_name, record_count, message)
  VALUES
    (CURRENT_TIMESTAMP(), 'sp_ausd_v_ta_cntrct_crs', p_JobKennung, p_EintragsNr, v_TabName, v_records, 'ENDE Datenverarbeitung');

END;