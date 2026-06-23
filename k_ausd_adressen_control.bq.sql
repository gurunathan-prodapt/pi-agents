-- k_ausd_adressen_control.bq.sql
--
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_adressen.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_adressen.ksh
--
-- This BigQuery Stored Procedure orchestrates the execution of the address processing logic.
-- It handles parameter validation, date calculation, and job logging.
--
-- Note: Replace `project.` with your actual Google Cloud Project ID.
-- The `project.isbert_schema.job_table` is used for logging job status and details.

CREATE OR REPLACE PROCEDURE `project.isbert_schema.k_ausd_adressen_control`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING,
  IN p_Stichtag STRING, -- Expected format DDMMYYYY
  IN p_wiederanlaufWert INT64 -- Optional, will default to 0 if NULL
)
BEGIN
  -- Declare variables for internal use within the stored procedure.
  DECLARE v_TabName STRING DEFAULT 'PoolVertrag'; -- Logical name for job tracking
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_stichtag_date DATE;
  DECLARE v_datum_heute DATE DEFAULT CURRENT_DATE();
  DECLARE v_datum_gestern DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
  DECLARE v_restart_value INT64;

  -- Initialize restart value, defaulting to 0 if p_wiederanlaufWert is NULL.
  SET v_restart_value = IFNULL(p_wiederanlaufWert, 0);

  -- --- Parameter Validation ---
  -- Checks if required input parameters are provided.
  IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'FEHLER: Jobkennung fehlt. Parameter -j ist erforderlich.';
  END IF;

  IF p_EintragsNr IS NULL OR TRIM(p_EintragsNr) = '' THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'FEHLER: EintragsNr fehlt. Parameter -f ist erforderlich.';
  END IF;

  IF p_Stichtag IS NULL OR TRIM(p_Stichtag) = '' THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'FEHLER: Stichtag fehlt. Parameter -s ist erforderlich.';
  END IF;

  -- Date format validation for p_Stichtag (DDMMYYYY).
  IF NOT REGEXP_CONTAINS(p_Stichtag, r'^\\d{8}$') THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'FEHLER: Stichtag hat kein gueltiges Format DDMMYYYY (expected DDMMYYYY).';
  END IF;

  -- Convert the input stichtag string to a DATE type for internal use.
  SET v_stichtag_date = PARSE_DATE('%d%m%Y', p_Stichtag);

  -- --- Job Logging: Start of Processing ---
  -- Records the job's initial status in a tracking table.
  INSERT INTO `project.isbert_schema.job_table` (
    tab_name,
    job_status,
    job_type,
    stichtag,
    process_date,
    record_state,
    restart_flag,
    record_count,
    description,
    job_kennung,
    eintrags_nr,
    created_at,
    updated_at
  )
  VALUES (
    v_TabName,
    'RUNNING', -- Initial status
    'I',       -- Assuming 'I' for Initialbefuellung (initial load)
    p_Stichtag,
    CAST(v_stichtag_date AS STRING), -- Store date as string for consistency with original parameter type
    'J',       -- Placeholder for record state, adjust as needed
    CASE WHEN v_restart_value > 0 THEN 'Y' ELSE 'N' END,
    NULL,      -- Record count is not yet known
    'Processing started (Initialbefuellung)',
    p_JobKennung,
    p_EintragsNr,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
  );

  -- --- Core Data Processing ---
  -- Calls the separate stored procedure that contains the translated SQL logic
  -- from d_ausd_adressen.sql.
  CALL `project.sof.d_ausd_adressen_proc`(p_Stichtag);

  -- --- Capture Record Count ---
  -- After the data processing, count records from a representative target table.
  -- This replaces the original script's method of reading from a temporary file.
  -- Adjust `project.sof.ta_e_business_gp` to the most appropriate final target table
  -- for counting the processed records if needed.
  SELECT COUNT(*) INTO v_records FROM `project.sof.ta_e_business_gp`;

  -- --- Job Logging: Completion ---
  -- Updates the job tracking table with completion status and record count.
  -- Attempts to update an existing 'RUNNING' entry first.
  UPDATE `project.isbert_schema.job_table`
  SET
    job_status = 'COMPLETED',
    record_count = v_records,
    description = 'Processing completed successfully',
    updated_at = CURRENT_TIMESTAMP()
  WHERE
    job_kennung = p_JobKennung AND eintrags_nr = p_EintragsNr AND stichtag = p_Stichtag AND job_status = 'RUNNING';

  -- If no row was updated (e.g., if the initial INSERT failed or was skipped, or for simpler job tracking),
  -- insert a new completion record. This provides resilience for job tracking.
  IF @@row_count = 0 THEN
      INSERT INTO `project.isbert_schema.job_table` (
        tab_name,
        job_status,
        job_type,
        stichtag,
        process_date,
        record_state,
        restart_flag,
        record_count,
        description,
        job_kennung,
        eintrags_nr,
        created_at,
        updated_at
      )
      VALUES (
        v_TabName,
        'COMPLETED',
        'I',
        p_Stichtag,
        CAST(v_stichtag_date AS STRING),
        'J',
        CASE WHEN v_restart_value > 0 THEN 'Y' ELSE 'N' END,
        v_records,
        'Processing completed successfully (Initialbefuellung - Inserted post-completion)',
        p_JobKennung,
        p_EintragsNr,
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP()
      );
  END IF;

  -- --- Final Output / Logging ---
  -- Selects key information, similar to original script's final echo/print.
  SELECT
    p_JobKennung AS JobKennung,
    p_EintragsNr AS EintragsNr,
    p_Stichtag AS Stichtag,
    v_records AS RecordsProcessed,
    v_datum_heute AS CurrentDate,
    v_datum_gestern AS YesterdayDate,
    v_restart_value AS RestartValue;

EXCEPTION WHEN ERROR THEN
  -- --- Error Handling ---
  -- In case of any error during execution, update the job tracking table to 'FAILED'.
  UPDATE `project.isbert_schema.job_table`
  SET
    job_status = 'FAILED',
    description = 'Processing failed: ' || ERROR(),
    updated_at = CURRENT_TIMESTAMP()
  WHERE
    job_kennung = p_JobKennung AND eintrags_nr = p_EintragsNr AND stichtag = p_Stichtag AND job_status = 'RUNNING';

  -- Re-raise the error to ensure the calling system is aware of the failure.
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Job failed for ' || p_JobKennung || ' - ' || p_EintragsNr || ' on ' || p_Stichtag || ': ' || ERROR();
END;