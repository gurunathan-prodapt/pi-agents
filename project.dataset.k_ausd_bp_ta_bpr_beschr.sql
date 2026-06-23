-- Original file: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_beschr.ksh
-- Legacy Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_beschr.ksh
-- This BigQuery Stored Procedure migrates the orchestration logic from k_ausd_bp_ta_bpr_beschr.ksh.

CREATE OR REPLACE PROCEDURE `project.dataset.k_ausd_bp_ta_bpr_beschr`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING,
  IN p_Stichtag STRING, -- Expected format DDMMYYYY
  IN p_wiederanlaufWert STRING
)
BEGIN
  DECLARE v_TabName STRING DEFAULT 'PoolBasisprodukt';
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_datum_heute DATE;
  DECLARE v_datum_gestern DATE;
  DECLARE v_Stichtag_date DATE;
  DECLARE v_restart STRING DEFAULT '0';
  DECLARE v_sql_script_proc_name STRING DEFAULT 'project.dataset.d_ausd_bp_ta_bpr_beschr'; -- Name of the called procedure
  DECLARE v_err STRING DEFAULT NULL;

  -- Parameter validation
  IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
    SET v_err = 'Jobkennung fehlt';
    RAISE USING MESSAGE = v_err;
  END IF;

  IF p_Stichtag IS NULL OR TRIM(p_Stichtag) = '' THEN
    SET v_err = 'Stichtag fehlt';
    RAISE USING MESSAGE = v_err;
  END IF;

  IF p_EintragsNr IS NULL OR TRIM(p_EintragsNr) = '' THEN
    SET v_err = 'EintragsNr fehlt';
    RAISE USING MESSAGE = v_err;
  END IF;

  -- Date validation for DDMMYYYY
  BEGIN
    SET v_Stichtag_date = PARSE_DATE('%d%m%Y', p_Stichtag);
  EXCEPTION WHEN ERROR THEN
    SET v_err = CONCAT('Ungueltiges Datum: ', p_Stichtag, '. Erwartetes Format: DDMMYYYY');
    RAISE USING MESSAGE = v_err;
  END;

  -- Restart value defaulting
  IF p_wiederanlaufWert IS NULL OR TRIM(p_wiederanlaufWert) = '' THEN
    SET v_restart = '0';
  ELSE
    SET v_restart = p_wiederanlaufWert;
  END IF;

  -- Derive today and yesterday (equivalent to gestern.ksh or date commands)
  SET v_datum_heute = CURRENT_DATE();
  SET v_datum_gestern = DATE_SUB(v_datum_heute, INTERVAL 1 DAY);

  -- Execute migrated SQL logic by calling the dedicated stored procedure
  CALL `project.dataset.d_ausd_bp_ta_bpr_beschr`(
    p_EintragsNr,
    p_JobKennung,
    p_Stichtag,
    v_restart,
    v_TabName,
    v_datum_heute,
    v_datum_gestern
  );

  -- Capture record count by querying the target table directly after the INSERT
  -- This provides the current record count for the executed job.
  SET v_records = (SELECT COUNT(*) FROM `dw_target.isrpt.sof_ta_bpr_beschr`);

  -- Optional job table entry replacement (FOSJobErzeugeEintrag)
  -- This assumes `project.dataset.job_table` exists and has the specified schema.
  -- An `insert_datetime` column is added for better auditing in BigQuery.
  INSERT INTO `project.dataset.job_table`
  (
    tab_name,
    status_a,
    status_i,
    stichtag_from,
    stichtag_to,
    job_type,
    restart_flag,
    record_count,
    description,
    insert_datetime
  )
  VALUES
  (
    v_TabName,
    'A',
    'I',
    v_Stichtag_date,
    v_Stichtag_date,
    'J',
    CASE WHEN v_restart = '0' THEN 'N' ELSE 'Y' END, -- Map restart value to 'N'/'Y'
    v_records,
    'Initialbefuellung',
    CURRENT_TIMESTAMP()
  );

  -- If `project.dataset.job_run_log` is intended for a more granular history of each run,
  -- a separate INSERT statement to that table would be placed here.

END;