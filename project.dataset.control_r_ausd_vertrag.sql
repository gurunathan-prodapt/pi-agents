-- BigQuery Stored Procedure for orchestration
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_apn_ve.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_apn_ve.ksh

CREATE OR REPLACE PROCEDURE `project.dataset.control_r_ausd_vertrag`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING
)
BEGIN
  DECLARE ErrNr INT64 DEFAULT 0;
  DECLARE ErrArg STRING DEFAULT '';
  DECLARE v_TabName STRING DEFAULT 'ta_apn_ve'; -- Hardcoded table name from design
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE error_message STRING;

  -- Parameter validation (replaces pruefeParameterGesetzt)
  IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
    SET ErrNr = 193; -- Example error code
    SET ErrArg = 'Jobkennung';
  END IF;

  IF p_EintragsNr IS NULL OR p_EintragsNr = '' AND ErrNr = 0 THEN
    SET ErrNr = 193; -- Example error code
    SET ErrArg = 'EintragsNr';
  END IF;

  -- Error handling (replaces DWMSG_MeldeFehler and shell exit)
  IF ErrNr != 0 THEN
    SET error_message = CONCAT('FEHLER: 0 E ', CAST(ErrNr AS STRING), ' Parameter ', ErrArg, ' ist nicht gesetzt.');
    -- Log to a BigQuery error table
    INSERT INTO `project.dataset.error_log`
      (error_source, error_type, error_number, error_argument, created_at)
    VALUES
      ('control_r_ausd_vertrag', 'E', ErrNr, ErrArg, CURRENT_TIMESTAMP());
    RAISE USING MESSAGE = error_message;
  END IF;

  BEGIN
    -- Main SQL execution wrapper replacement (replaces starteSQLSkript)
    -- Calls the BigQuery Stored Procedure that encapsulates the converted d_ausd_v_ta_apn_ve.sql logic
    CALL `project.dataset.d_ausd_v_ta_apn_ve_sp`(
      p_EintragsNr,
      p_JobKennung
    );

    -- Log job completion
    INSERT INTO `project.dataset.job_log`
      (job_kennung, eintrags_nr, tab_name, status, created_at)
    VALUES
      (p_JobKennung, p_EintragsNr, v_TabName, 'ENDE', CURRENT_TIMESTAMP());

    -- Replace temp-file read with direct query result for record count
    -- This assumes `project.dataset.ta_apn_ve` is the target table where records are added/updated
    -- The actual filter criteria might need adjustment based on the original d_ausd_v_ta_apn_ve.sql logic.
    -- For this example, we assume `eintrags_nr` is a valid filter.
    EXECUTE IMMEDIATE FORMAT("""
      SELECT COUNT(*)
      FROM `%s.%s.ta_apn_ve` -- Target table in BQ
      WHERE eintrags_nr = '%s'
    """, @@project_id, @@dataset_id, p_EintragsNr) INTO v_records;


    -- Persist record count
    INSERT INTO `project.dataset.record_count_log`
      (job_kennung, eintrags_nr, tab_name, record_count, created_at)
    VALUES
      (p_JobKennung, p_EintragsNr, v_TabName, v_records, CURRENT_TIMESTAMP());

  EXCEPTION WHEN ERROR THEN
    SET error_message = CONCAT('An error occurred during execution: ', @@error.message);
    -- Log any unhandled exceptions during the core SQL execution or record counting.
    INSERT INTO `project.dataset.error_log`
      (error_source, error_type, error_number, error_argument, created_at)
    VALUES
      ('control_r_ausd_vertrag', 'E', 9999, error_message, CURRENT_TIMESTAMP());
    RAISE USING MESSAGE = error_message;
  END;

END;