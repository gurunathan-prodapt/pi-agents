-- BigQuery Stored Procedure for k_ausd_v_ta_notice.ksh migration
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_notice.ksh
-- This procedure orchestrates the data processing logic previously handled by the KornShell script
-- and the underlying SQL script d_ausd_v_ta_notice.sql.

-- IMPORTANT: Replace `your_project_id` and `your_dataset_id` with actual BigQuery project and dataset IDs.
-- For example, `your_project_id.your_dataset_id.r_ausd_v_ta_notice` might become `gcp-project-12345.isbert_reporting.r_ausd_v_ta_notice`.

CREATE OR REPLACE PROCEDURE `your_project_id.isbert_reporting.r_ausd_v_ta_notice`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING -- Expected format: YYYYMMDD, used for date filtering
)
BEGIN
  DECLARE v_TabName STRING DEFAULT 'ta_notice';
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE ErrNr INT64 DEFAULT 0;
  DECLARE ErrArg STRING DEFAULT '';
  DECLARE v_datum_date DATE; -- To store the parsed date for SQL logic

  -- Parameter validation: Check for NULL or empty JobKennung
  IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
    SET ErrNr = 1;
    SET ErrArg = 'Jobkennung';
  END IF;

  -- Parameter validation: Check for NULL or empty EintragsNr
  IF p_EintragsNr IS NULL OR p_EintragsNr = '' THEN
    SET ErrNr = 1;
    SET ErrArg = 'EintragsNr';
  END IF;

  -- If initial parameter validation failed, log error and exit
  IF ErrNr <> 0 THEN
    INSERT INTO `your_project_id.isbert_reporting.error_log`
    (error_number, error_argument, procedure_name, created_at, error_message)
    VALUES
    (ErrNr, ErrArg, 'r_ausd_v_ta_notice', CURRENT_TIMESTAMP(), 'Missing required input parameter.');

    SELECT FORMAT('FEHLER: 0 E %d %s - Missing required parameter.', ErrNr, ErrArg) AS message;
    LEAVE; -- Exit the stored procedure
  END IF;

  -- Attempt to parse p_EintragsNr into a DATE. Log error and exit if format is invalid.
  BEGIN
    SET v_datum_date = PARSE_DATE('%Y%m%d', p_EintragsNr);
  EXCEPTION WHEN ERROR THEN
    SET ErrNr = 2; -- Custom error number for invalid date format
    SET ErrArg = 'p_EintragsNr (invalid date format)';
    INSERT INTO `your_project_id.isbert_reporting.error_log`
    (error_number, error_argument, procedure_name, created_at, error_message)
    VALUES
    (ErrNr, ErrArg, 'r_ausd_v_ta_notice', CURRENT_TIMESTAMP(), 'Invalid date format for p_EintragsNr. Expected YYYYMMDD.');
    SELECT FORMAT('FEHLER: 0 E %d %s - Invalid date format.', ErrNr, ErrArg) AS message;
    LEAVE; -- Exit the stored procedure
  END;

  -- Main processing block with error handling
  BEGIN
    -- Insert a record into the job_table indicating the start of processing
    INSERT INTO `your_project_id.isbert_reporting.job_table`
    (job_kennung, eintrags_nr, tab_name, status, created_at)
    VALUES
    (p_JobKennung, p_EintragsNr, v_TabName, 'ACTIVE', CURRENT_TIMESTAMP());

    -- Equivalent of TRUNCATE TABLE sof$ta_notice from the original SQL script.
    -- This operation deletes all existing records in the target table before inserting new ones.
    DELETE FROM `your_project_id.isbert_reporting.ta_notice`
    WHERE TRUE;

    -- Core data processing logic, translated from d_ausd_v_ta_notice.sql
    -- Inserts data into the ta_notice table based on conditions from cds_ta_notice.
    INSERT `your_project_id.isbert_reporting.ta_notice` (
        cntrct_id,
        valid_from,
        valid_to,
        entry_date_of_notice
    )
    SELECT
        n.cntrct_id,
        CAST(n.valid_from AS DATE),         -- Cast TIMESTAMP to DATE
        CAST(n.valid_to AS DATE),           -- Cast TIMESTAMP to DATE
        CAST(n.entry_date_of_notice AS DATE) -- Cast TIMESTAMP to DATE
    FROM
        `your_project_id.isbert_reporting.cds_ta_notice` AS n
    WHERE
        CAST(n.insert_at AS DATE) <= v_datum_date
    AND
        (n.modified_at IS NULL OR CAST(n.modified_at AS DATE) > v_datum_date)
    -- The following line was commented out in the original Oracle SQL:
    -- AND (n.valid_from <= v_datum_date)
    AND
        (n.valid_to IS NULL OR CAST(n.valid_to AS DATE) > v_datum_date)
    AND
        n.is_production = 1;

    -- Retrieve the number of records processed for auditing/logging
    SET v_records = (
      SELECT COUNT(*)
      FROM `your_project_id.isbert_reporting.ta_notice`
      WHERE entry_date_of_notice = v_datum_date
    );

    -- Update the job_table to reflect successful completion and record count
    UPDATE `your_project_id.isbert_reporting.job_table`
    SET status = 'COMPLETED',
        record_count = v_records,
        updated_at = CURRENT_TIMESTAMP()
    WHERE job_kennung = p_JobKennung
      AND eintrags_nr = p_EintragsNr
      AND status = 'ACTIVE';

    SELECT '---------- ENDE Datenverarbeitung ----------' AS message;
    SELECT v_records AS records_processed;

  EXCEPTION WHEN ERROR THEN
    -- If any error occurs in the main processing block, update job_table status to FAILED
    UPDATE `your_project_id.isbert_reporting.job_table`
    SET status = 'FAILED',
        error_message = @@error.message,
        updated_at = CURRENT_TIMESTAMP()
    WHERE job_kennung = p_JobKennung
      AND eintrags_nr = p_EintragsNr
      AND status = 'ACTIVE';

    -- Log the detailed error in the error_log table
    INSERT INTO `your_project_id.isbert_reporting.error_log`
    (error_number, error_argument, procedure_name, created_at, error_message)
    VALUES
    (NULL, NULL, 'r_ausd_v_ta_notice', CURRENT_TIMESTAMP(), @@error.message);

    SELECT FORMAT('FEHLER: %s', @@error.message) AS message;
    RAISE; -- Re-raise the error to propagate failure to the calling Airflow DAG
  END;
END;