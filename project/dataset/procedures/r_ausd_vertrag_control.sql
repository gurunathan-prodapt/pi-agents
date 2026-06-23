-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_apn_ve.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_apn_ve.ksh
CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_vertrag_control`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING
)
BEGIN
  DECLARE ErrNr INT64 DEFAULT 0;
  DECLARE ErrArg STRING DEFAULT '';
  DECLARE v_TabName STRING DEFAULT 'ta_apn_ve';
  DECLARE v_records INT64 DEFAULT 0;

  -- Parameter validation
  IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
    SET ErrNr = 0; -- Original script sets ErrNr=0 then checks for it. Replicating this for consistency.
    SET ErrArg = 'Jobkennung';
  END IF;

  IF p_EintragsNr IS NULL OR p_EintragsNr = '' THEN
    SET ErrNr = 0;
    SET ErrArg = 'EintragsNr';
  END IF;

  IF ErrNr = 0 AND (p_JobKennung IS NULL OR p_JobKennung = '' OR p_EintragsNr IS NULL OR p_EintragsNr = '') THEN
    INSERT INTO `project.dataset.job_error_log`
    (job_kennung, eintrags_nr, error_nr, error_arg, error_ts)
    VALUES
    (p_JobKennung, p_EintragsNr, ErrNr, ErrArg, CURRENT_TIMESTAMP());
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Bitte ueber Rahmenscript aufrufen';
  END IF;

  BEGIN -- Main processing block
    CALL `project.dataset.d_ausd_v_ta_apn_ve`(
      p_EintragsNr,
      p_JobKennung
    );

    SET v_records = (
      SELECT COUNT(*)
      FROM `project.dataset.ta_apn_ve`
      WHERE eintrags_nr = p_EintragsNr
    );

    INSERT INTO `project.dataset.job_run_log`
    (job_kennung, eintrags_nr, tab_name, records, run_ts)
    VALUES
    (p_JobKennung, p_EintragsNr, v_TabName, v_records, CURRENT_TIMESTAMP());

    SELECT FORMAT('Job completed successfully. Processed %d records for EintragsNr: %s', v_records, p_EintragsNr) AS message;

  EXCEPTION WHEN ERROR THEN
    INSERT INTO `project.dataset.job_error_log`
    (job_kennung, eintrags_nr, error_nr, error_arg, error_ts)
    VALUES
    (p_JobKennung, p_EintragsNr, 1, 'SQL execution failed', CURRENT_TIMESTAMP());
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'SQL execution failed';
  END;
END;