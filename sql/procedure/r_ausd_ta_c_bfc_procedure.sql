--
-- BigQuery Stored Procedure for k_ausd_v_ta_c_bfc.ksh
-- This procedure re-implements the orchestration and data processing logic
-- previously handled by the KornShell script and its invoked SQL file.
--
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_c_bfc.ksh
-- Original SQL:  vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_c_bfc.sql
--

CREATE OR REPLACE PROCEDURE `your_project.your_dataset.r_ausd_ta_c_bfc`(
  p_job_kennung STRING,
  p_eintrags_nr STRING
)
BEGIN
  DECLARE v_run_id STRING;
  DECLARE v_current_timestamp TIMESTAMP;
  DECLARE v_records_processed INT64;
  DECLARE v_error_message STRING;
  DECLARE v_error_code STRING DEFAULT 'UNKNOWN';
  -- `v_bfc_procedure_date` was derived from Oracle's `all_objects` for a package creation date.
  -- For BigQuery, this could be a fixed date, a lookup from a config table, or CURRENT_DATE().
  DECLARE v_bfc_procedure_date DATE;
  DECLARE v_max_update_limit INT64 DEFAULT 1000000; -- `DEFINE v_max_update` from Oracle script

  SET v_run_id = GENERATE_UUID();
  SET v_current_timestamp = CURRENT_TIMESTAMP();

  -- Parameter Validation (replaces `pruefeParameterGesetzt`)
  IF p_job_kennung IS NULL OR p_job_kennung = '' THEN
    SET v_error_message = 'Parameter p_job_kennung cannot be NULL or empty.';
    SET v_error_code = '192'; -- Arbitrary error code, reflecting original script's error handling
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
  END IF;

  IF p_eintrags_nr IS NULL OR p_eintrags_nr = '' THEN
    SET v_error_message = 'Parameter p_eintrags_nr cannot be NULL or empty.';
    SET v_error_code = '193'; -- Arbitrary error code
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
  END IF;

  -- Initial Log Entry (Job Start)
  INSERT INTO `your_project.your_dataset.job_run_log`
    (run_id, job_kenn_ung, eintrags_nr, event_type, event_message, event_timestamp, source_script)
  VALUES
    (v_run_id, p_job_kennung, p_eintrags_nr, 'START', 'Job started', v_current_timestamp, 'r_ausd_ta_c_bfc');

  -- Get v_bfc_procedure_date
  -- The original script fetched this from `all_objects.created` for the `CDS$VR_BINDEFRIST` package.
  -- In BigQuery, this can be a fixed date, a value from a configuration table, or simply CURRENT_DATE()
  -- if the procedure logic is always considered "current".
  SET v_bfc_procedure_date = CURRENT_DATE(); -- Placeholder: Assuming procedure is always 'current' date for now.
                                            -- Adjust this if a specific historical date is required.

  -- Job State Management: Deactivate old active jobs
  -- The original script commented: "alte aktive Jobs werden einfach dekativiert"
  -- This implies an UPDATE on a job control table.
  UPDATE `your_project.your_dataset.job_control_table`
  SET
    status = 'INACTIVE',
    end_timestamp = v_current_timestamp,
    last_update_timestamp = v_current_timestamp
  WHERE
    job_kenn_ung = p_job_kennung AND status = 'ACTIVE'; -- Refine WHERE clause as per actual logic if needed (e.g., considering p_eintrags_nr)

  -- Insert/Update job_control_table for current run
  INSERT INTO `your_project.your_dataset.job_control_table`
    (job_kenn_ung, eintrags_nr, run_id, status, start_timestamp, last_update_timestamp)
  VALUES
    (p_job_kennung, p_eintrags_nr, v_run_id, 'ACTIVE', v_current_timestamp, v_current_timestamp);

  -- Core Data Transformation Logic from d_ausd_v_ta_c_bfc.sql

  -- Create a temporary table to mimic Oracle's `sof$ta_c_bfc_akt`
  CREATE TEMPORARY TABLE IF NOT EXISTS `sof_ta_c_bfc_akt` (
    cntrct_id STRING,
    commitment_reference_date DATE,
    cntrct_validity_id STRING,
    bfc_age DATE,
    bfc_count INT64
  );

  -- Oracle original script used `isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_c_bfc_akt');`
  TRUNCATE TABLE `sof_ta_c_bfc_akt`;

  -- Step 1: Populate `sof_ta_c_bfc_akt`
  INSERT INTO `sof_ta_c_bfc_akt`
  SELECT
      c.cntrct_id,
      MAX(c.commitment_reference_date) AS commitment_reference_date,
      MAX(c.cntrct_validity_id) AS cntrct_validity_id,
      MAX(GREATEST(COALESCE(c.bfc_age, PARSE_DATE('%Y%m%d', '19000101')),
                   COALESCE(b.bfc_age, PARSE_DATE('%Y%m%d', '19000101')),
                   COALESCE(v.bfc_age, PARSE_DATE('%Y%m%d', '19000101')),
                   COALESCE(p_fi.bfc_age, PARSE_DATE('%Y%m%d', '19000101')),
                   COALESCE(p_fo.bfc_age, PARSE_DATE('%Y%m%d', '19000101')),
                   COALESCE(p_fi_n.bfc_age, PARSE_DATE('%Y%m%d', '19000101')),
                   COALESCE(p_fo_n.bfc_age, PARSE_DATE('%Y%m%d', '19000101')))) AS bfc_age,
      COUNT(1) AS bfc_count
  FROM
      `your_project.your_dataset.sof_ta_cntrct_crs` AS c
      LEFT JOIN `your_project.your_dataset.sof_ta_barrier` AS b ON c.cntrct_id = b.cntrct_id
      LEFT JOIN `your_project.your_dataset.sof_ta_cntrct_valid` AS v ON c.cntrct_validity_id = v.cntrct_validity_id
      LEFT JOIN `your_project.your_dataset.sof_ta_period` AS p_fi ON v.first_period_id = p_fi.period_id
      LEFT JOIN `your_project.your_dataset.sof_ta_period` AS p_fo ON v.following_period_id = p_fo.period_id
      LEFT JOIN `your_project.your_dataset.sof_ta_period` AS p_fi_n ON v.first_notice_period_id = p_fi_n.period_id
      LEFT JOIN `your_project.your_dataset.sof_ta_period` AS p_fo_n ON v.follow_notice_period_id = p_fo_n.period_id
  GROUP BY
      c.cntrct_id;

  -- Step 2: Conditional initial population of `your_project.your_dataset.ta_c_bfc`
  -- Only insert if the target table is empty.
  IF (SELECT COUNT(1) FROM `your_project.your_dataset.ta_c_bfc` LIMIT 1) = 0 THEN
      INSERT INTO `your_project.your_dataset.ta_c_bfc` (
          cntrct_id,
          bfc_age,
          bfc_count,
          bfc_procedure,
          commitment_reference_date,
          cntrct_validity_id
      )
      SELECT
          cntrct_id,
          bfc_age,
          bfc_count,
          PARSE_DATE('%Y%m%d', '19000101'), -- Default bfc_procedure date for initial entries
          commitment_reference_date,
          cntrct_validity_id
      FROM `sof_ta_c_bfc_akt`;
  END IF;

  -- Step 3: MERGE INTO `your_project.your_dataset.ta_c_bfc`
  MERGE INTO `your_project.your_dataset.ta_c_bfc` AS D
  USING `sof_ta_c_bfc_akt` AS S
  ON (D.cntrct_id = S.cntrct_id)
  WHEN MATCHED AND (
       D.bfc_age < S.bfc_age
    OR D.bfc_count <> S.bfc_count
  ) THEN UPDATE SET
      bindefrist = `your_project.your_dataset.bfc_get_bindefrist`(S.cntrct_id, S.commitment_reference_date, S.cntrct_validity_id),
      bfc_age = S.bfc_age,
      bfc_count = S.bfc_count,
      bfc_procedure = v_bfc_procedure_date,
      commitment_reference_date = S.commitment_reference_date,
      cntrct_validity_id = S.cntrct_validity_id
  WHEN NOT MATCHED THEN INSERT (
      cntrct_id,
      bindefrist,
      bfc_age,
      bfc_count,
      bfc_procedure,
      commitment_reference_date,
      cntrct_validity_id
  ) VALUES (
      S.cntrct_id,
      `your_project.your_dataset.bfc_get_bindefrist`(S.cntrct_id, S.commitment_reference_date, S.cntrct_validity_id),
      S.bfc_age,
      S.bfc_count,
      v_bfc_procedure_date,
      S.commitment_reference_date,
      S.cntrct_validity_id
  );

  -- Step 4: UPDATE `your_project.your_dataset.ta_c_bfc` for outdated procedures
  -- Original Oracle used `ROWNUM <= &v_max_update` to limit updates.
  -- In BigQuery, `ROWNUM` has no direct equivalent for limiting an UPDATE.
  -- This UPDATE will apply to ALL matching rows. If limiting is critical for
  -- business logic or performance, this section requires a specific BigQuery redesign
  -- (e.g., by selecting rows to update into a temp table, then MERGE, or using QUALIFY
  -- with a suitable window function and ordering).
  UPDATE `your_project.your_dataset.ta_c_bfc`
  SET
      bindefrist = `your_project.your_dataset.bfc_get_bindefrist`(
          cntrct_id,
          commitment_reference_date,
          cntrct_validity_id
      ),
      bfc_procedure = v_bfc_procedure_date
  WHERE
      bfc_procedure < v_bfc_procedure_date;
      -- `AND ROWNUM <= v_max_update_limit` from Oracle is removed here.

  SET v_records_processed = @@row_count; -- Get total rows affected by the last DML statement (the UPDATE)

  -- Final Log Entry (Job End)
  INSERT INTO `your_project.your_dataset.job_run_log`
    (run_id, job_kenn_ung, eintrags_nr, event_type, event_message, record_count, event_timestamp, source_script)
  VALUES
    (v_run_id, p_job_kennung, p_eintrags_nr, 'END', 'Job completed successfully', v_records_processed, CURRENT_TIMESTAMP(), 'r_ausd_ta_c_bfc');

  -- Update job_control_table to COMPLETED
  UPDATE `your_project.your_dataset.job_control_table`
  SET
    status = 'COMPLETED',
    end_timestamp = CURRENT_TIMESTAMP(),
    last_update_timestamp = CURRENT_TIMESTAMP()
  WHERE
    job_kenn_ung = p_job_kennung AND eintrags_nr = p_eintrags_nr AND run_id = v_run_id;

EXCEPTION WHEN ERROR THEN
  SET v_error_message = @@error.message;
  -- If a specific error code was set during parameter validation, use it. Otherwise, use a generic one.
  SET v_error_code = COALESCE(v_error_code, 'SQL_ERROR');

  -- Log Error
  INSERT INTO `your_project.your_dataset.job_error_log`
    (run_id, job_kenn_ung, eintrags_nr, error_code, error_message, error_timestamp, source_script)
  VALUES
    (v_run_id, p_job_kennung, p_eintrags_nr, v_error_code, v_error_message, CURRENT_TIMESTAMP(), 'r_ausd_ta_c_bfc');

  -- Update job_control_table to FAILED
  UPDATE `your_project.your_dataset.job_control_table`
  SET
    status = 'FAILED',
    end_timestamp = CURRENT_TIMESTAMP(),
    last_update_timestamp = CURRENT_TIMESTAMP()
  WHERE
    job_kenn_ung = p_job_kennung AND eintrags_nr = p_eintrags_nr AND run_id = v_run_id;

  RAISE; -- Re-raise the error to terminate procedure execution
END;