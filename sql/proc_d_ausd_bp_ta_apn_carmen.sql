CREATE OR REPLACE PROCEDURE `sof.proc_d_ausd_bp_ta_apn_carmen`()
BEGIN
  DECLARE v_datum DATE DEFAULT DATE '1900-01-01';
  DECLARE v_datum_str STRING;
  DECLARE v_job_name STRING DEFAULT 'BERT_DROP_TEMP_TABLE';
  DECLARE v_rows_inserted INT64 DEFAULT 0;
  DECLARE exit_message STRING DEFAULT 'SUCCESS';

  BEGIN
    -- Step 00: Determine dynamic cutoff date from isbert_schema.dwtk_meldungen
    SET v_datum = (
      SELECT COALESCE(MAX(DATE(timecreated)), DATE '1900-01-01')
      FROM `isbert_schema.dwtk_meldungen`
      WHERE job_kennung = v_job_name
    );

    SET v_datum_str = FORMAT_DATE('%Y%m%d', v_datum);

    -- Step 01: Truncate target table
    TRUNCATE TABLE `sof.ta_apn_carmen`;

    -- Step 10: Insert filtered join results into target table
    INSERT INTO `sof.ta_apn_carmen` (
      CNTRCT_ID,
      ACCESS_POINT_NAME
    )
    SELECT
      pca.cntrct_id AS CNTRCT_ID,
      ap.access_point_name AS ACCESS_POINT_NAME
    FROM `carmen_replica.pds_ta_pdp_context_assoc` AS pca
    JOIN `carmen_replica.pds_ta_pdp_context` AS pc
      ON pca.pdp_context_id = pc.pdp_context_id
    JOIN `carmen_replica.pds_ta_access_point` AS ap
      ON pc.access_point_id = ap.access_point_id
    WHERE pca.cntrct_id IS NOT NULL
      AND DATE(pca.insert_at) <= v_datum
      AND (pca.modified_at IS NULL OR DATE(pca.modified_at) > v_datum)
      AND DATE(pca.valid_from) <= v_datum
      AND (pca.valid_to IS NULL OR DATE(pca.valid_to) > v_datum)
      AND DATE(pc.insert_at) <= v_datum
      AND (pc.modified_at IS NULL OR DATE(pc.modified_at) > v_datum)
      AND pc.is_production = 1
      AND DATE(ap.insert_at) <= v_datum
      AND (ap.modified_at IS NULL OR DATE(ap.modified_at) > v_datum);

    SET v_rows_inserted = @@row_count;

    SET exit_message = CONCAT(
      'Processing completed successfully. Effective date=',
      v_datum_str,
      ', rows inserted=',
      CAST(v_rows_inserted AS STRING)
    );

    -- Write metadata into the log table
    INSERT INTO `isbert_schema.job_log` (
      job_name,
      event_type,
      event_ts,
      message
    )
    VALUES (
      'ausd_bp_ta_apn_carmen',
      'SUCCESS',
      CURRENT_TIMESTAMP(),
      exit_message
    );

  EXCEPTION WHEN ERROR THEN
    SET exit_message = CONCAT(
      'ERROR in proc_d_ausd_bp_ta_apn_carmen: ',
      @@error.message
    );
    
    INSERT INTO `isbert_schema.job_log` (
      job_name,
      event_type,
      event_ts,
      message
    )
    VALUES (
      'ausd_bp_ta_apn_carmen',
      'FAILURE',
      CURRENT_TIMESTAMP(),
      exit_message
    );
    
    RAISE USING MESSAGE = @@error.message;
  END;
END;