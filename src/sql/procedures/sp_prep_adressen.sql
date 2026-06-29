-- Legacy Job: BERT_P_ADRESSEN
-- Legacy Source: r_ausd_adressen.ksh & DW.BERT_LESE_LOG include
-- Target Platform: BigQuery Stored Procedures
-- Purpose: Modularized transformation logic for master address cleansing, standardizing, and historicizing.

CREATE OR REPLACE PROCEDURE `dw_bert.sp_prep_adressen`()
BEGIN
  DECLARE v_start_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  DECLARE v_batch_id STRING DEFAULT GENERATE_UUID();
  DECLARE v_rows_affected INT64 DEFAULT 0;
  DECLARE v_last_run TIMESTAMP;

  -- 1. Identify previous successful execution to isolate source increments (delta-load logic)
  SET v_last_run = (
    SELECT COALESCE(MAX(end_time), TIMESTAMP('1970-01-01 00:00:00+00'))
    FROM `dw_bert.metadata_job_runs`
    WHERE job_name = 'BERT_P_ADRESSEN' AND status = 'SUCCESS'
  );

  -- 2. Stage and cleanse delta records in memory
  CREATE OR REPLACE TEMP TABLE temp_address_deltas AS (
    SELECT
      adr.address_id,
      TRIM(UPPER(adr.street_name)) AS street_name,
      TRIM(UPPER(adr.postal_code)) AS postal_code,
      TRIM(UPPER(adr.city)) AS city,
      TRIM(UPPER(adr.country_code)) AS country_code,
      adr.last_modified_timestamp AS source_last_modified
    FROM `dw_bert_staging.stg_addresses` adr
    WHERE adr.last_modified_timestamp > v_last_run
  );

  -- 3. SCD Type 2 - Invalidate matching active rows that have change discrepancies
  MERGE `dw_bert.t_adressen` T
  USING temp_address_deltas S
  ON T.address_id = S.address_id AND T.is_current = TRUE
  WHEN MATCHED AND (
    COALESCE(T.street_name, '') != COALESCE(S.street_name, '') OR
    COALESCE(T.postal_code, '') != COALESCE(S.postal_code, '') OR
    COALESCE(T.city, '') != COALESCE(S.city, '') OR
    COALESCE(T.country_code, '') != COALESCE(S.country_code, '')
  ) THEN
    UPDATE SET 
      T.valid_to = CURRENT_TIMESTAMP(), 
      T.is_current = FALSE;

  -- 4. SCD Type 2 - Insert the newly-standardized version of modified or new addresses
  INSERT INTO `dw_bert.t_adressen` (
    address_id, street_name, postal_code, city, country_code,
    valid_from, valid_to, is_current, source_last_modified, load_ts, batch_id
  )
  SELECT
    address_id,
    street_name,
    postal_code,
    city,
    country_code,
    CURRENT_TIMESTAMP() AS valid_from,
    TIMESTAMP('9999-12-31 23:59:59+00') AS valid_to,
    TRUE AS is_current,
    source_last_modified,
    CURRENT_TIMESTAMP() AS load_ts,
    v_batch_id AS batch_id
  FROM temp_address_deltas;

  -- 5. Logging and Auditing of Row Counts
  SET v_rows_affected = (SELECT COUNT(*) FROM temp_address_deltas);

  INSERT INTO `dw_bert.metadata_job_runs` (
    job_name, start_time, end_time, status, rows_affected, message, batch_id
  )
  VALUES (
    'BERT_P_ADRESSEN',
    v_start_ts,
    CURRENT_TIMESTAMP(),
    'SUCCESS',
    v_rows_affected,
    'Address preparation completed successfully',
    v_batch_id
  );

EXCEPTION WHEN ERROR THEN
  -- Centralized Exception Handling & Target Audit Registration
  INSERT INTO `dw_bert.metadata_job_runs` (
    job_name, start_time, end_time, status, rows_affected, message, batch_id
  )
  VALUES (
    'BERT_P_ADRESSEN',
    v_start_ts,
    CURRENT_TIMESTAMP(),
    'FAILED',
    NULL,
    @@error.message,
    v_batch_id
  );
  -- Escalate exception to ensure Cloud Composer / Airflow registers task failure
  RAISE USING MESSAGE = @@error.message;
END;