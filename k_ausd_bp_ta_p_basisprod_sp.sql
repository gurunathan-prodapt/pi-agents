-- BigQuery Stored Procedure for Orchestration
-- Replaces KornShell script vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_p_basisprod.ksh
-- Legacy Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_p_basisprod.ksh

CREATE OR REPLACE PROCEDURE `project.dataset.k_ausd_bp_ta_p_basisprod_sp`(
  IN p_job_kennung STRING,
  IN p_eintrags_nr STRING,
  IN p_stichtag STRING, -- Expected format: DDMMYYYY
  IN p_wiederanlauf_wert INT64 DEFAULT 0
)
BEGIN
  DECLARE v_job_name STRING DEFAULT 'k_ausd_bp_ta_p_basisprod';
  DECLARE v_run_id STRING;
  DECLARE v_start_time TIMESTAMP;
  DECLARE v_end_time TIMESTAMP;
  DECLARE v_status STRING;
  DECLARE v_record_count INT64;
  DECLARE v_error_message STRING;
  DECLARE v_stichtag_date DATE;
  DECLARE v_date_today DATE;
  DECLARE v_date_yesterday DATE;
  DECLARE v_max_timecreated_date STRING; -- Corresponds to Oracle v_datum from dwtk_meldungen

  -- Initialize start time and generate a unique run_id
  SET v_start_time = CURRENT_TIMESTAMP();
  SET v_run_id = GENERATE_UUID();

  -- Log job start
  INSERT INTO `project.dataset.job_audit`
    (job_name, run_id, start_time, status, stichtag, eintrags_nr)
  VALUES
    (v_job_name, v_run_id, v_start_time, 'RUNNING', NULL, p_eintrags_nr);

  BEGIN
    -- Parameter Validation
    IF p_job_kennung IS NULL OR TRIM(p_job_kennung) = '' THEN
      RAISE USING MESSAGE = 'Parameter p_job_kennung is mandatory.';
    END IF;

    IF p_eintrags_nr IS NULL OR TRIM(p_eintrags_nr) = '' THEN
      RAISE USING MESSAGE = 'Parameter p_eintrags_nr is mandatory.';
    END IF;

    IF p_stichtag IS NULL OR TRIM(p_stichtag) = '' THEN
      RAISE USING MESSAGE = 'Parameter p_stichtag is mandatory.';
    END IF;

    -- Date Format Validation for p_stichtag (DDMMYYYY)
    IF NOT REGEXP_CONTAINS(p_stichtag, r'^\d{8}$') THEN
      RAISE USING MESSAGE = CONCAT('Invalid date format for p_stichtag: ', p_stichtag, '. Expected: DDMMYYYY');
    END IF;

    SET v_stichtag_date = PARSE_DATE('%d%m%Y', p_stichtag);

    -- Calculate p_datum_heute and p_datum_gestern (from gestern.ksh)
    SET v_date_today = CURRENT_DATE();
    SET v_date_yesterday = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

    -- Determine v_max_timecreated_date (from isbert_schema.dwtk_meldungen)
    -- This was originally NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101')
    SELECT
      COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
    INTO v_max_timecreated_date
    FROM `project.dataset.dwtk_meldungen` m
    WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';

    -- Call the data transformation stored procedure
    CALL `project.dataset.d_ausd_bp_ta_p_basisprod_sp`();

    -- Capture record count after the transformation
    SELECT COUNT(*) INTO v_record_count FROM `project.dataset.sof_ta_p_basisprod`;

    SET v_status = 'SUCCESS';
    SET v_end_time = CURRENT_TIMESTAMP();

    -- Update job audit log for success
    UPDATE `project.dataset.job_audit`
    SET
      end_time = v_end_time,
      status = v_status,
      record_count = v_record_count,
      stichtag = v_stichtag_date
    WHERE run_id = v_run_id;

    -- Return a success message (optional)
    SELECT CONCAT('Job ', v_job_name, ' completed successfully. Records inserted: ', CAST(v_record_count AS STRING)) AS message;

  EXCEPTION WHEN ERROR THEN
    SET v_error_message = @@error.message;
    SET v_status = 'FAILED';
    SET v_end_time = CURRENT_TIMESTAMP();

    -- Update job audit log for failure
    UPDATE `project.dataset.job_audit`
    SET
      end_time = v_end_time,
      status = v_status,
      error_message = v_error_message,
      stichtag = v_stichtag_date -- Attempt to log stichtag even on parse error
    WHERE run_id = v_run_id;

    -- Re-raise the error to ensure the pipeline fails
    RAISE USING MESSAGE = CONCAT('Job ', v_job_name, ' failed: ', v_error_message);
  END;
END;