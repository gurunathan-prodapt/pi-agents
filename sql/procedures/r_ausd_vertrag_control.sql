-- BigQuery Stored Procedure for k_ausd_v_ta_period.ksh migration
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_period.ksh

CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_vertrag_control`(
  p_JobKennung STRING,
  p_EintragsNr STRING
)
BEGIN
  DECLARE v_TabName STRING DEFAULT 'ta_period';
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_error_code INT64 DEFAULT 0;
  DECLARE v_error_arg STRING DEFAULT NULL;
  DECLARE v_proc_name STRING DEFAULT 'r_ausd_vertrag_control';

  -- =========================================================
  -- Parameter validation
  -- =========================================================
  IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
    SET v_error_code = 193;
    SET v_error_arg = 'Jobkennung';
  END IF;

  IF v_error_code = 0 AND (p_EintragsNr IS NULL OR TRIM(p_EintragsNr) = '') THEN
    SET v_error_code = 193;
    SET v_error_arg = 'EintragsNr';
  END IF;

  IF v_error_code <> 0 THEN
    INSERT INTO `project.dataset.error_log` (
      error_ts,
      error_code,
      error_arg,
      procedure_name,
      message
    )
    VALUES (
      CURRENT_TIMESTAMP(),
      v_error_code,
      v_error_arg,
      v_proc_name,
      FORMAT('FEHLER: 0 E %d %s', v_error_code, v_error_arg)
    );

    SELECT
      v_error_code AS error_code,
      v_error_arg AS error_arg,
      FORMAT('FEHLER: 0 E %d %s', v_error_code, v_error_arg) AS message;

    RETURN;
  END IF;

  -- =========================================================
  -- Job deactivation / activation handling
  -- =========================================================
  UPDATE `project.dataset.job_table`
  SET
    active_flag = FALSE,
    updated_ts = CURRENT_TIMESTAMP()
  WHERE job_kennung = p_JobKennung
    AND active_flag = TRUE;

  INSERT INTO `project.dataset.job_table` (
    job_kennung,
    eintrags_nr,
    tab_name,
    active_flag,
    created_ts,
    updated_ts,
    completed_ts,
    record_count
  )
  VALUES (
    p_JobKennung,
    p_EintragsNr,
    v_TabName,
    TRUE,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP(),
    NULL,
    NULL
  );

  -- =========================================================
  -- Core SQL logic migrated from d_ausd_v_ta_period.sql
  -- Replace the placeholder below with the actual BigQuery SQL
  -- =========================================================
  BEGIN
    -- EXAMPLE PLACEHOLDER:
    -- The content of d_ausd_v_ta_period.sql needs to be migrated to BigQuery Standard SQL
    -- and inserted here. If the logic is dynamic or complex, consider a separate
    -- BigQuery Stored Procedure that is called here, or use EXECUTE IMMEDIATE.

    -- Example DML:
    -- EXECUTE IMMEDIATE """
    --   INSERT INTO `project.dataset.target_data_table` (col1, col2, ..., eintrags_nr)
    --   SELECT source_col1, source_col2, ..., @p_EintragsNr
    --   FROM `project.dataset.source_data_table`
    --   WHERE some_condition = @p_EintragsNr;
    -- """ USING p_EintragsNr AS p_EintragsNr;

    -- For now, v_records is set to 0. This should be replaced by the actual
    -- count of records processed by the migrated d_ausd_v_ta_period.sql logic.
    -- Example record count retrieval after DML:
    -- SET v_records = (
    --   SELECT COUNT(*)
    --   FROM `project.dataset.target_data_table`
    --   WHERE eintrags_nr = p_EintragsNr -- Adjust filtering as needed for your target table
    -- );
    SET v_records = 0; -- Placeholder for actual record count

  EXCEPTION WHEN ERROR THEN
    INSERT INTO `project.dataset.error_log` (
      error_ts,
      error_code,
      error_arg,
      procedure_name,
      message
    )
    VALUES (
      CURRENT_TIMESTAMP(),
      500, -- Custom error code for SQL execution failure
      p_EintragsNr,
      v_proc_name,
      'Error during execution of migrated d_ausd_v_ta_period.sql logic'
    );

    UPDATE `project.dataset.job_table`
    SET
      active_flag = FALSE,
      updated_ts = CURRENT_TIMESTAMP()
    WHERE job_kennung = p_JobKennung
      AND eintrags_nr = p_EintragsNr
      AND active_flag = TRUE;

    RAISE USING MESSAGE = 'Error during execution of migrated d_ausd_v_ta_period.sql logic';
  END;

  -- =========================================================
  -- Final job completion update
  -- =========================================================
  UPDATE `project.dataset.job_table`
  SET
    record_count = v_records,
    active_flag = FALSE,
    completed_ts = CURRENT_TIMESTAMP(),
    updated_ts = CURRENT_TIMESTAMP()
  WHERE job_kennung = p_JobKennung
    AND eintrags_nr = p_EintragsNr;

  -- Return result set similar to the shell script's completion output
  SELECT
    p_JobKennung AS job_kennung,
    p_EintragsNr AS eintrags_nr,
    v_TabName AS tab_name,
    v_records AS records_processed,
    'COMPLETED' AS status;

EXCEPTION WHEN ERROR THEN
  -- Outer safety net for unexpected failures within the procedure
  INSERT INTO `project.dataset.error_log` (
    error_ts,
    error_code,
    error_arg,
    procedure_name,
    message
  )
  VALUES (
    CURRENT_TIMESTAMP(),
    999, -- General unexpected error code
    p_EintragsNr,
    v_proc_name,
    'Unexpected error in r_ausd_vertrag_control'
  );

  UPDATE `project.dataset.job_table`
  SET
    active_flag = FALSE,
    updated_ts = CURRENT_TIMESTAMP()
  WHERE job_kennung = p_JobKennung
    AND eintrags_nr = p_EintragsNr
    AND active_flag = TRUE;

  RAISE USING MESSAGE = 'Unexpected error in r_ausd_vertrag_control';
END;