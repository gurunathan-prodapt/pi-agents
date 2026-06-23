-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_cntrct_dist.ksh
-- This BigQuery Stored Procedure replaces the KornShell orchestration script and its embedded SQL logic.

CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_bp_ta_cntrct_dist`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING,
  IN p_Stichtag STRING, -- Expected format: DDMMYYYY
  IN p_wiederanlaufWert STRING
)
BEGIN
  -- Declare variables
  DECLARE v_TabName STRING DEFAULT 'sof_ta_cntrct_dist'; -- Inferred from target table name
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_stichtag_date DATE;
  DECLARE v_restart STRING DEFAULT '0';
  DECLARE v_log_message STRING;

  -- -----------------------------------------------------------------------------
  -- Parameter validation and assignment
  -- -----------------------------------------------------------------------------

  -- Default restart value
  IF p_wiederanlaufWert IS NULL OR p_wiederanlaufWert = '' THEN
    SET v_restart = '0';
  ELSE
    SET v_restart = p_wiederanlaufWert;
  END IF;

  -- Required parameter validation
  IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
    RAISE USING MESSAGE = 'FEHLER: Jobkennung fehlt';
  END IF;
  IF p_Stichtag IS NULL OR p_Stichtag = '' THEN
    RAISE USING MESSAGE = 'FEHLER: Stichtag fehlt';
  END IF;
  IF p_EintragsNr IS NULL OR p_EintragsNr = '' THEN
    RAISE USING MESSAGE = 'FEHLER: EintragsNr fehlt';
  END IF;

  -- Date validation and conversion
  BEGIN
    SET v_stichtag_date = PARSE_DATE('%d%m%Y', p_Stichtag);
  EXCEPTION WHEN ERROR THEN
    RAISE USING MESSAGE = 'FEHLER: Ungültiges Datumsformat für p_Stichtag. Erwartet DDMMYYYY, erhalten: ' || p_Stichtag;
  END;

  -- Log start of processing (example)
  SET v_log_message = FORMAT("START processing for JobKennung=%s, EintragsNr=%s, Stichtag=%s (Date: %s), WiederanlaufWert=%s",
                             p_JobKennung, p_EintragsNr, p_Stichtag, CAST(v_stichtag_date AS STRING), v_restart);
  SELECT v_log_message AS message;
  -- If job logging table is active, insert here:
  -- INSERT INTO `project.dataset.job_log_table` (job_kennung, entry_nr, stichtag_date, status, message, created_at)
  -- VALUES (p_JobKennung, p_EintragsNr, v_stichtag_date, 'RUNNING', v_log_message, CURRENT_TIMESTAMP());


  -- -----------------------------------------------------------------------------
  -- Core logic translated from d_ausd_bp_ta_cntrct_dist.sql
  -- -----------------------------------------------------------------------------
  -- Note: The original SQL script included logic to determine a 'v_datum' from a
  -- 'dwtk_meldungen' table for specific internal Oracle job control. For this
  -- migration, 'p_Stichtag' (converted to v_stichtag_date) is assumed to be the
  -- primary driving date for data filtering and processing, consistent with the
  -- KornShell script's parameter passing. If the 'dwtk_meldungen' logic is critical
  -- for data filtering within the SQL logic, it needs to be explicitly added here.

  -- prompt step01: löschen der temporären-tabellen...
  TRUNCATE TABLE `project.dataset.sof_ta_cntrct_dist`;
  SELECT 'TRUNCATE TABLE `project.dataset.sof_ta_cntrct_dist` executed.' AS message;

  -- prompt step7: erzeuge separate vertragstabelle (distinct cntrct_id auf basis bpr-instanz)...
  INSERT INTO `project.dataset.sof_ta_cntrct_dist`
    (CNTRCT_ID)
  SELECT
      DISTINCT cntrct_id
  FROM
      `project.dataset.sof_ta_bpr_basis`; -- Filter by v_stichtag_date might be needed here based on actual source table schema
  SELECT 'INSERT INTO `project.dataset.sof_ta_cntrct_dist` completed.' AS message;

  -- -----------------------------------------------------------------------------
  -- Record count
  -- -----------------------------------------------------------------------------
  -- Get the number of records processed/inserted
  SET v_records = (
    SELECT COUNT(*)
    FROM `project.dataset.sof_ta_cntrct_dist`
  );

  SET v_log_message = FORMAT("---------- ENDE Datenverarbeitung ----------. Processed records: %d", v_records);
  SELECT v_log_message AS message;

  -- Optional job logging (if functionality is activated)
  -- INSERT INTO `project.dataset.job_log_table` (job_kennung, entry_nr, stichtag_date, status, message, processed_records, created_at)
  -- VALUES (p_JobKennung, p_EintragsNr, v_stichtag_date, 'SUCCESS', v_log_message, v_records, CURRENT_TIMESTAMP());

END;