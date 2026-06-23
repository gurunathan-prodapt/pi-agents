-- BigQuery Stored Procedure for legacy job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_action_assoc.ksh
-- This procedure orchestrates data processing, handling job parameters, error logging,
-- and execution of the core SQL logic for 'ta_action_assoc' data.
CREATE OR REPLACE PROCEDURE `project.dataset.sp_ausd_v_ta_action_assoc`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING
)
BEGIN
  DECLARE v_TabName STRING DEFAULT 'ta_action_assoc';
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_sql STRING;
  DECLARE v_error_message STRING DEFAULT '';
  DECLARE v_error_code INT64 DEFAULT 0;

  -- Parameter validation (replaces shell getopts and pruefeParameterGesetzt)
  IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
    SET v_error_code = 193;
    SET v_error_message = 'Jobkennung';
  END IF;

  IF v_error_code = 0 AND (p_EintragsNr IS NULL OR TRIM(p_EintragsNr) = '') THEN
    SET v_error_code = 193;
    SET v_error_message = 'EintragsNr';
  END IF;

  IF v_error_code != 0 THEN
    -- Equivalent to DWMSG_MeldeFehler / echo / exit
    INSERT INTO `project.dataset.error_log`
      (error_ts, error_code, error_arg, job_kennung, eintrags_nr, message)
    VALUES
      (CURRENT_TIMESTAMP(), v_error_code, v_error_message, p_JobKennung, p_EintragsNr,
       CONCAT('FEHLER: 0 E ', CAST(v_error_code AS STRING), ' ', v_error_message));

    RAISE USING MESSAGE = CONCAT('Bitte ueber Rahmenscript aufrufen | FEHLER: 0 E ', CAST(v_error_code AS STRING), ' ', v_error_message);
  END IF;

  -- Optional job control initialization (replaces job table updates)
  INSERT INTO `project.dataset.job_control`
    (job_kennung, eintrags_nr, tab_name, status, created_ts)
  VALUES
    (p_JobKennung, p_EintragsNr, v_TabName, 'STARTED', CURRENT_TIMESTAMP());

  BEGIN
    -- Downstream SQL script equivalent (replaces starteSQLSkript)
    -- The actual transformation logic from d_ausd_v_ta_action_assoc.sql
    -- needs to be translated into BigQuery SQL and inlined here or called as a separate procedure.
    -- The original KornShell script calls: ${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_v_ta_action_assoc.sql
    -- Example placeholder for the core logic, assuming it uses p_EintragsNr
    -- If the SQL script has complex logic, it should be migrated to a separate
    -- stored procedure and called from here.
    EXECUTE IMMEDIATE """
      -- Placeholder for the logic from d_ausd_v_ta_action_assoc.sql
      -- Example:
      -- UPDATE `project.dataset.ta_action_assoc`
      -- SET status = 'processed', updated_at = CURRENT_TIMESTAMP()
      -- WHERE entry_nr = @p_EintragsNr;

      -- If d_ausd_v_ta_action_assoc.sql contains a SELECT statement that
      -- generates data, consider storing it in a temporary table or
      -- using it directly to update a target table.
      SELECT 1; -- Dummy statement for compilation
    """
    USING p_EintragsNr AS p_EintragsNr; -- Example parameter passing

    -- Record count equivalent to temp file read
    -- This assumes the core SQL logic has processed records related to p_EintragsNr
    -- Adjust filtering conditions based on the actual logic of d_ausd_v_ta_action_assoc.sql
    SET v_records = (
      SELECT COUNT(*)
      FROM `project.dataset.ta_action_assoc`
      WHERE entry_nr = p_EintragsNr -- Assuming entry_nr is a key for this job execution
        AND status = 'processed' -- Example condition, refine based on actual SQL
    );

    -- Persist record count instead of temp file
    INSERT INTO `project.dataset.job_result`
      (job_kennung, eintrags_nr, tab_name, records, result_ts)
    VALUES
      (p_JobKennung, p_EintragsNr, v_TabName, v_records, CURRENT_TIMESTAMP());

    -- Mark job complete
    UPDATE `project.dataset.job_control`
    SET status = 'FINISHED',
        finished_ts = CURRENT_TIMESTAMP(),
        record_count = v_records
    WHERE job_kennung = p_JobKennung
      AND eintrags_nr = p_EintragsNr
      AND tab_name = v_TabName;

  EXCEPTION WHEN ERROR THEN
    -- Handle errors during SQL execution
    SET v_error_message = @@error.message;
    SET v_error_code = -1; -- Or derive a more specific code

    INSERT INTO `project.dataset.error_log`
      (error_ts, error_code, error_arg, job_kennung, eintrags_nr, message)
    VALUES
      (CURRENT_TIMESTAMP(), v_error_code, 'SQL Execution Error', p_JobKennung, p_EintragsNr, v_error_message);

    UPDATE `project.dataset.job_control`
    SET status = 'FAILED',
        finished_ts = CURRENT_TIMESTAMP()
    WHERE job_kennung = p_JobKennung
      AND eintrags_nr = p_EintragsNr
      AND tab_name = v_TabName;

    RAISE USING MESSAGE = CONCAT('SQL Execution Failed for job ', p_JobKennung, ' / ', p_EintragsNr, ': ', v_error_message);
  END;

END;