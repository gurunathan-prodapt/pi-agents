-- BigQuery Stored Procedure for orchestration logic
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_basis_his.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_basis_his.ksh

CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_bp_ta_bpr_basis_his`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING,
  IN p_Stichtag STRING,
  IN p_wiederanlaufWert STRING
)
BEGIN
  DECLARE v_TabName STRING DEFAULT 'PoolBasisprodukt';
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_stichtag_date DATE;
  DECLARE v_datum_heute DATE;
  DECLARE v_datum_gestern DATE;
  DECLARE v_start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  DECLARE v_end_time TIMESTAMP;
  DECLARE v_status STRING DEFAULT 'STARTED';
  DECLARE v_error_message STRING;
  DECLARE v_error_detail STRING;
  DECLARE v_job_name STRING DEFAULT 'k_ausd_bp_ta_bpr_basis_his'; -- Matches legacy job name
  DECLARE v_job_id STRING DEFAULT GENERATE_UUID(); -- Unique ID for this run

  -- Log job start
  INSERT INTO `project.audit.job_audit` (audit_timestamp, job_name, job_id, status, start_time, parameters, audited_by)
  VALUES (CURRENT_TIMESTAMP(), v_job_name, v_job_id, v_status, v_start_time, TO_JSON(STRUCT(p_JobKennung, p_EintragsNr, p_Stichtag, p_wiederanlaufWert)), 'r_ausd_bp_ta_bpr_basis_his');

  BEGIN
    -- Parameter checks
    IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
      SET v_error_message = 'FEHLER: JobKennung Parameter fehlt.';
      SET v_error_detail = 'Mandatory parameter p_JobKennung is missing or empty.';
      RAISE USING MESSAGE v_error_message;
    END IF;

    IF p_Stichtag IS NULL OR p_Stichtag = '' THEN
      SET v_error_message = 'FEHLER: Stichtag Parameter fehlt.';
      SET v_error_detail = 'Mandatory parameter p_Stichtag is missing or empty.';
      RAISE USING MESSAGE v_error_message;
    END IF;

    IF p_EintragsNr IS NULL OR p_EintragsNr = '' THEN
      SET v_error_message = 'FEHLER: EintragsNr Parameter fehlt.';
      SET v_error_detail = 'Mandatory parameter p_EintragsNr is missing or empty.';
      RAISE USING MESSAGE v_error_message;
    END IF;

    -- Date validation (DWDate_Datum_Check)
    SET v_stichtag_date = SAFE.PARSE_DATE('%d%m%Y', p_Stichtag);
    IF v_stichtag_date IS NULL THEN
      SET v_error_message = 'FEHLER: Ungueltiges Datumsformat fuer Stichtag.';
      SET v_error_detail = FORMAT("Input Stichtag '%s' does not match format 'DDMMYYYY'.", p_Stichtag);
      RAISE USING MESSAGE v_error_message;
    END IF;

    -- Derive today and yesterday dates (gestern.ksh logic)
    SET v_datum_heute = CURRENT_DATE();
    SET v_datum_gestern = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

    -- Initialize wiederanlaufWert if null (KSH script equivalent)
    DECLARE actual_wiederanlaufWert STRING DEFAULT IFNULL(p_wiederanlaufWert, '0');

    -- Call the migrated core SQL logic stored procedure
    CALL `project.dataset.d_ausd_bp_ta_bpr_basis_his`(
      p_EintragsNr,
      p_JobKennung,
      p_Stichtag,
      actual_wiederanlaufWert,
      FORMAT_DATE('%d%m%Y', v_datum_heute),
      FORMAT_DATE('%d%m%Y', v_datum_gestern)
    );

    -- Capture record count from target table (eval "v_records=`cat $tmpFile`")
    -- This assumes d_ausd_bp_ta_bpr_basis_his populates PoolBasisprodukt
    -- and there's a way to identify records for this run, e.g., by stichtag or job_id if added to table.
    -- For now, let's count all records added on the stichtag.
    -- TODO: Refine WHERE clause to match specific records processed by d_ausd_bp_ta_bpr_basis_his
    SET v_records = (SELECT COUNT(*) FROM `project.dataset.PoolBasisprodukt` WHERE stichtag = v_stichtag_date AND job_kennung = p_JobKennung);

    SET v_status = 'COMPLETED';
    SET v_end_time = CURRENT_TIMESTAMP();

    -- Log job completion
    INSERT INTO `project.audit.job_audit` (audit_timestamp, job_name, job_id, status, start_time, end_time, duration_seconds, processed_records, parameters, audited_by)
    VALUES (CURRENT_TIMESTAMP(), v_job_name, v_job_id, v_status, v_start_time, v_end_time, TIMESTAMP_DIFF(v_end_time, v_start_time, SECOND), v_records, TO_JSON(STRUCT(p_JobKennung, p_EintragsNr, p_Stichtag, p_wiederanlaufWert)), 'r_ausd_bp_ta_bpr_basis_his');

  EXCEPTION WHEN ERROR THEN
    SET v_status = 'FAILED';
    SET v_end_time = CURRENT_TIMESTAMP();
    SET v_error_message = @@error.message;
    SET v_error_detail = IFNULL(v_error_detail, v_error_message); -- Use specific detail if set, otherwise general message

    -- Log error
    INSERT INTO `project.audit.error_log` (log_timestamp, job_name, job_id, error_code, error_message, error_detail, parameters, logged_by)
    VALUES (CURRENT_TIMESTAMP(), v_job_name, v_job_id, NULL, v_error_message, v_error_detail, TO_JSON(STRUCT(p_JobKennung, p_EintragsNr, p_Stichtag, p_wiederanlaufWert)), 'r_ausd_bp_ta_bpr_basis_his');

    -- Update job audit to FAILED
    INSERT INTO `project.audit.job_audit` (audit_timestamp, job_name, job_id, status, start_time, end_time, duration_seconds, processed_records, parameters, audited_by)
    VALUES (CURRENT_TIMESTAMP(), v_job_name, v_job_id, v_status, v_start_time, v_end_time, TIMESTAMP_DIFF(v_end_time, v_start_time, SECOND), 0, TO_JSON(STRUCT(p_JobKennung, p_EintragsNr, p_Stichtag, p_wiederanlaufWert)), 'r_ausd_bp_ta_bpr_basis_his');

    RAISE; -- Re-raise the error to propagate it to the caller (Airflow)
  END;

END;