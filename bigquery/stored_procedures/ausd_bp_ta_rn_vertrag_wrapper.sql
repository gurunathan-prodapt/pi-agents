-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_vertrag.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_vertrag.ksh
-- Target: BigQuery SQL
-- Wrapper stored procedure for r_ausd_bp_ta_rn_vertrag.ksh migration
-- Replace `gcp-project-id.bq_dataset_name` with your actual project/dataset.

CREATE OR REPLACE PROCEDURE `gcp-project-id.bq_dataset_name.ausd_bp_ta_rn_vertrag_wrapper`(
  p_stichtag STRING,
  p_wiederanlaufWert STRING
)
BEGIN
  DECLARE v_jobkennung STRING DEFAULT 'ausd_bp_ta_rn_vertrag_wrapper';
  DECLARE v_eintragsnr INT64;
  DECLARE v_stichtag DATE;
  DECLARE v_stichtag_str STRING;
  DECLARE v_wiederanlaufWert STRING;
  DECLARE v_now TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  DECLARE v_error_message STRING;
  DECLARE v_error_stack STRING;
  DECLARE v_error_statement STRING;

  -- ---------------------------------------------------------------------------
  -- Helper: validate DDMMYYYY date string
  -- ---------------------------------------------------------------------------
  DECLARE v_is_valid_stichtag BOOL DEFAULT TRUE;

  BEGIN
    -- Default handling for wiederanlaufwert
    SET v_wiederanlaufWert = IFNULL(p_wiederanlaufWert, '0');

    -- Validate wiederanlaufwert is numeric
    IF NOT REGEXP_CONTAINS(v_wiederanlaufWert, r'^\d+$') THEN
      RAISE USING MESSAGE = CONCAT(
        'Invalid p_wiederanlaufWert: expected numeric string, got ''',
        IFNULL(v_wiederanlaufWert, 'NULL'),
        ''''
      );
    END IF;

    -- Parse/validate stichtag; default to CURRENT_DATE() if not provided
    IF p_stichtag IS NULL OR TRIM(p_stichtag) = '' THEN
      SET v_stichtag = CURRENT_DATE();
    ELSE
      IF NOT REGEXP_CONTAINS(TRIM(p_stichtag), r'^\d{8}$') THEN
        RAISE USING MESSAGE = CONCAT(
          'Invalid p_stichtag format: expected DDMMYYYY, got ''',
          p_stichtag,
          ''''
        );
      END IF;

      BEGIN
        SET v_stichtag = PARSE_DATE('%d%m%Y', TRIM(p_stichtag));
      EXCEPTION WHEN ERROR THEN
        RAISE USING MESSAGE = CONCAT(
          'Invalid p_stichtag value: unable to parse DDMMYYYY date ''',
          p_stichtag,
          ''''
        );
      END;
    END IF;

    SET v_stichtag_str = FORMAT_DATE('%d%m%Y', v_stichtag);

    -- -------------------------------------------------------------------------
    -- Job logging: determine next entry number and insert initial log record
    -- -------------------------------------------------------------------------
    SET v_eintragsnr = (
      SELECT IFNULL(MAX(eintragsnr), 0) + 1
      FROM `gcp-project-id.bq_dataset_name.job_log`
    );

    INSERT INTO `gcp-project-id.bq_dataset_name.job_log` (
      eintragsnr,
      jobkennung,
      stichtag,
      wiederanlaufwert,
      status,
      created_at,
      updated_at
    )
    VALUES (
      v_eintragsnr,
      v_jobkennung,
      v_stichtag,
      v_wiederanlaufWert,
      'RUNNING',
      v_now,
      v_now
    );

    -- -------------------------------------------------------------------------
    -- Call downstream core procedure
    -- -------------------------------------------------------------------------
    CALL `gcp-project-id.bq_dataset_name.k_ausd_bp_ta_rn_vertrag`(
      v_jobkennung,
      v_stichtag_str,
      v_eintragsnr,
      v_wiederanlaufWert
    );

    -- -------------------------------------------------------------------------
    -- Mark job as completed
    -- -------------------------------------------------------------------------
    UPDATE `gcp-project-id.bq_dataset_name.job_log`
    SET
      status = 'COMPLETED',
      updated_at = CURRENT_TIMESTAMP()
    WHERE eintragsnr = v_eintragsnr
      AND jobkennung = v_jobkennung;

  EXCEPTION WHEN ERROR THEN
    SET v_error_message = @@error.message;
    SET v_error_stack = @@error.stack_trace;
    SET v_error_statement = @@error.statement_text;

    -- Log error details
    INSERT INTO `gcp-project-id.bq_dataset_name.job_error_log` (
      jobkennung,
      eintragsnr,
      error_message,
      error_stack,
      error_statement,
      created_at
    )
    VALUES (
      v_jobkennung,
      v_eintragsnr,
      v_error_message,
      v_error_stack,
      v_error_statement,
      CURRENT_TIMESTAMP()
    );

    -- Mark job as failed if log entry exists
    IF v_eintragsnr IS NOT NULL THEN
      UPDATE `gcp-project-id.bq_dataset_name.job_log`
      SET
        status = 'FAILED',
        updated_at = CURRENT_TIMESTAMP()
      WHERE eintragsnr = v_eintragsnr
        AND jobkennung = v_jobkennung;
    END IF;

    RAISE USING MESSAGE = CONCAT(
      'Procedure failed: ',
      IFNULL(v_error_message, 'Unknown error')
    );
  END;
END;