-- Target: BigQuery Stored Procedure for k_ausd_v_ta_disc_zusgf.ksh
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_disc_zusgf.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_disc_zusgf.ksh

CREATE OR REPLACE PROCEDURE `your_project_id.your_dataset_id.r_ausd_vertrag_control`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING
)
BEGIN
  DECLARE ErrNr INT64 DEFAULT 0;
  DECLARE ErrArg STRING DEFAULT '';
  DECLARE v_TabName STRING DEFAULT 'ta_disc_zusgf';
  DECLARE v_records INT64 DEFAULT 0;

  -- Parameter validation
  IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
    SET ErrNr = 193;
    SET ErrArg = 'Jobkennung';
  END IF;

  IF p_EintragsNr IS NULL OR p_EintragsNr = '' THEN
    SET ErrNr = 193;
    SET ErrArg = 'EintragsNr';
  END IF;

  IF ErrNr <> 0 THEN
    INSERT INTO `your_project_id.your_dataset_id.job_error_log`
    (error_ts, procedure_name, err_nr, err_arg, job_kennung, eintrags_nr)
    VALUES
    (CURRENT_TIMESTAMP(), 'r_ausd_vertrag_control', ErrNr, ErrArg, p_JobKennung, p_EintragsNr);

    SELECT FORMAT('FEHLER: 0 E %d %s', ErrNr, ErrArg) AS message;
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'Bitte ueber Rahmenscript aufrufen';
  END IF;

  -- Call the core SQL logic (equivalent of d_ausd_v_ta_disc_zusgf.sql)
  -- This assumes d_ausd_v_ta_disc_zusgf has been migrated to a separate SP or as inline SQL.
  -- The actual d_ausd_v_ta_disc_zusgf procedure needs to be implemented.
  CALL `your_project_id.your_dataset_id.d_ausd_v_ta_disc_zusgf`(p_EintragsNr, p_JobKennung);

  -- Example result counting - assuming d_ausd_v_ta_disc_zusgf updates ta_disc_zusgf
  -- The actual record counting logic will depend on how d_ausd_v_ta_disc_zusgf is implemented
  -- and how it reports affected rows. This is a placeholder.
  SET v_records = (
    SELECT COUNT(*)
    FROM `your_project_id.your_dataset_id.ta_disc_zusgf`
    WHERE eintragsnr = p_EintragsNr -- Example condition, adjust as per actual SQL logic in d_ausd_v_ta_disc_zusgf
  );

  INSERT INTO `your_project_id.your_dataset_id.job_run_log`
  (log_ts, procedure_name, job_kennung, eintrags_nr, tab_name, records_processed, status)
  VALUES
  (CURRENT_TIMESTAMP(), 'r_ausd_vertrag_control', p_JobKennung, p_EintragsNr, v_TabName, v_records, 'DONE');

  SELECT ' ---------- ENDE Datenverarbeitung ----------' AS message;
  SELECT v_records AS records_processed;
END;