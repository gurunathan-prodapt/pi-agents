-- BigQuery Stored Procedure for orchestration logic
-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_beschr.ksh
--
-- This procedure encapsulates the parameter parsing, validation, date derivation,
-- and invocation of the business SQL logic from the original KornShell script.
--
CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_bp_ta_bpr_beschr`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING,
  IN p_Stichtag STRING, -- Expected format DDMMYYYY
  IN p_wiederanlaufWert INT64
)
BEGIN
  -- Declare variables
  DECLARE v_TabName STRING DEFAULT 'PoolBasisprodukt';
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_stichtag_date DATE;
  DECLARE v_datum_heute DATE DEFAULT CURRENT_DATE();
  DECLARE v_datum_gestern DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
  DECLARE v_restart INT64 DEFAULT 0;

  -- 1. Default restart value
  SET v_restart = IFNULL(p_wiederanlaufWert, 0);

  -- 2. Required parameter checks
  IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
    RAISE USING MESSAGE = 'Jobkennung fehlt';
  END IF;
  IF p_Stichtag IS NULL OR TRIM(p_Stichtag) = '' THEN
    RAISE USING MESSAGE = 'Stichtag fehlt';
  END IF;
  IF p_EintragsNr IS NULL OR TRIM(p_EintragsNr) = '' THEN
    RAISE USING MESSAGE = 'EintragsNr fehlt';
  END IF;

  -- 3. Date validation for DDMMYYYY
  SET v_stichtag_date = SAFE.PARSE_DATE('%d%m%Y', p_Stichtag);
  IF v_stichtag_date IS NULL THEN
    RAISE USING MESSAGE = 'Ungueltiges Datumformat fuer Stichtag. Erwartet DDMMYYYY.';
  END IF;

  -- 4. Execute business SQL logic (calling the migrated d_ausd_bp_ta_bpr_beschr.sql)
  CALL `project.dataset.d_ausd_bp_ta_bpr_beschr_proc`(
    p_EintragsNr, p_JobKennung, p_Stichtag, v_restart, v_datum_heute, v_datum_gestern
  );

  -- 5. Record count retrieval
  -- NOTE: The WHERE clause for counting records in PoolBasisprodukt needs to be
  --       determined based on the actual business logic of d_ausd_bp_ta_bpr_beschr.sql.
  --       This example assumes a `stichtag_date` column in `PoolBasisprodukt`
  --       that aligns with the input `p_Stichtag`. Adjust if your actual schema differs.
  SELECT COUNT(*) INTO v_records
  FROM `project.dataset.PoolBasisprodukt`
  WHERE stichtag_date = v_stichtag_date;

  -- 6. Persist audit / job entry (replacement for FOSJobErzeugeEintrag)
  INSERT INTO `project.dataset.job_audit_table` (
    tab_name, job_status, load_type, stichtag, run_date, job_kind, restart_flag, record_count, message, insert_timestamp
  )
  VALUES (
    v_TabName, 'A', 'I', p_Stichtag, v_stichtag_date, 'J', CASE WHEN v_restart = 1 THEN 'Y' ELSE 'N' END, v_records, 'Job executed successfully', CURRENT_TIMESTAMP()
  );

  -- Optional: Log success message
  SELECT FORMAT("Job '%s' for Stichtag '%s' completed successfully. Processed %d records.", p_JobKennung, p_Stichtag, v_records) AS job_status_message;

EXCEPTION WHEN ERROR THEN
  -- Handle errors: Log to audit table with error status
  INSERT INTO `project.dataset.job_audit_table` (
    tab_name, job_status, load_type, stichtag, run_date, job_kind, restart_flag, record_count, message, insert_timestamp
  )
  VALUES (
    v_TabName, 'E', 'I', p_Stichtag, v_stichtag_date, 'J', CASE WHEN v_restart = 1 THEN 'Y' ELSE 'N' END, 0, CONCAT('Job failed: ', ERROR_MESSAGE()), CURRENT_TIMESTAMP()
  );
  RAISE; -- Re-raise the error after logging
END;