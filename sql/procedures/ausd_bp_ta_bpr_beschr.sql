-- BigQuery Stored Procedure: Wrapper equivalent for r_ausd_bp_ta_bpr_beschr.ksh
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_beschr.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_beschr.ksh

CREATE OR REPLACE PROCEDURE `project.dataset.ausd_bp_ta_bpr_beschr`(
  IN p_stichtag STRING,
  IN p_wiederanlaufWert INT64
)
BEGIN
  DECLARE v_sysdate STRING;
  DECLARE v_stichtag STRING;
  DECLARE v_wiederanlaufWert INT64 DEFAULT 0;
  DECLARE DW_EintragsNr INT64;
  DECLARE JobKennung STRING DEFAULT 'AUSD_BP_TA_BPR_BESCHR';
  DECLARE LogDatei STRING;
  DECLARE ErrNr INT64 DEFAULT 0;
  DECLARE ErrArg STRING DEFAULT '';
  DECLARE Name_Kernskript STRING DEFAULT 'project.dataset.k_ausd_bp_ta_bpr_beschr';

  -- Default restart value
  SET v_wiederanlaufWert = IFNULL(p_wiederanlaufWert, 0);

  -- System date equivalent in DDMMYYYY
  SET v_sysdate = FORMAT_DATE('%d%m%Y', CURRENT_DATE());

  -- Default stichtag if not provided
  SET v_stichtag = IFNULL(NULLIF(p_stichtag, ''), v_sysdate);

  -- Parameter validation equivalent
  IF v_stichtag IS NULL OR v_stichtag = '' THEN
    SET ErrNr = 193;
    SET ErrArg = 'Stichtag';
  END IF;

  IF ErrNr <> 0 THEN
    -- Replace with audit/error table insert
    INSERT INTO `project.dataset.job_error_log`
    (
      job_name,
      error_number,
      error_argument,
      created_at
    )
    VALUES
    (
      JobKennung,
      ErrNr,
      ErrArg,
      CURRENT_TIMESTAMP()
    );

    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = CONCAT('Parameter error: ', ErrArg, ', ErrNr=', CAST(ErrNr AS STRING));
  END IF;

  -- Job number generation equivalent
  -- Using a transaction to ensure atomic increment and retrieval for DW_EintragsNr
  BEGIN TRANSACTION;
    SET DW_EintragsNr = (
      SELECT IFNULL(MAX(job_entry_nr), 0) + 1
      FROM `project.dataset.job_control`
      WHERE job_name = JobKennung
    );
  COMMIT TRANSACTION;

  -- Log file equivalent replaced by audit table reference
  SET LogDatei = CONCAT('job_', JobKennung, '_', CAST(DW_EintragsNr AS STRING));

  -- Create job start entry
  INSERT INTO `project.dataset.job_control`
  (
    job_entry_nr,
    job_name,
    script_name,
    log_reference,
    stichtag,
    status,
    created_at
  )
  VALUES
  (
    DW_EintragsNr,
    JobKennung,
    'ausd_bp_ta_bpr_beschr',
    LogDatei,
    v_stichtag,
    'STARTED',
    CURRENT_TIMESTAMP()
  );

  BEGIN
    -- Call core processing procedure
    CALL `project.dataset.k_ausd_bp_ta_bpr_beschr`(
      JobKennung,
      v_stichtag,
      DW_EintragsNr,
      v_wiederanlaufWert
    );

    -- Mark success
    UPDATE `project.dataset.job_control`
    SET status = 'OK',
        finished_at = CURRENT_TIMESTAMP()
    WHERE job_entry_nr = DW_EintragsNr
      AND job_name = JobKennung;

  EXCEPTION WHEN ERROR THEN
    UPDATE `project.dataset.job_control`
    SET status = 'ERROR',
        finished_at = CURRENT_TIMESTAMP()
    WHERE job_entry_nr = DW_EintragsNr
      AND job_name = JobKennung;

    INSERT INTO `project.dataset.job_error_log`
    (
      job_name,
      job_entry_nr,
      error_message,
      created_at
    )
    VALUES
    (
      JobKennung,
      DW_EintragsNr,
      @@error.message,
      CURRENT_TIMESTAMP()
    );

    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = @@error.message;
  END;
END;