-- BigQuery Stored Procedure replacing vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_discount_rr.ksh
-- and executing logic from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_discount_rr.sql
CREATE OR REPLACE PROCEDURE `your_gcp_project.isrpt_isbert_stage.sp_ausd_v_ta_discount_rr`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING
)
BEGIN
  DECLARE v_TabName STRING DEFAULT 'ta_discount_rr';
  DECLARE v_records INT664 DEFAULT 0;
  DECLARE v_err_msg STRING DEFAULT '';
  DECLARE v_process_date STRING;

  -- 1. Parameter validation (replaces pruefeParameterGesetzt from KSH)
  IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
    SET v_err_msg = 'Fehler: Jobkennung fehlt. JobKennung: ' || COALESCE(p_JobKennung, 'NULL');
    -- Log error and raise exception
    INSERT INTO `your_gcp_project.isrpt_isbert_stage.job_log` (job_kennung, eintrags_nr, status, message, created_at)
    VALUES (p_JobKennung, p_EintragsNr, 'ERROR', v_err_msg, CURRENT_TIMESTAMP());
    RAISE USING MESSAGE = v_err_msg;
  END IF;

  IF p_EintragsNr IS NULL OR TRIM(p_EintragsNr) = '' THEN
    SET v_err_msg = 'Fehler: EintragsNr fehlt. EintragsNr: ' || COALESCE(p_EintragsNr, 'NULL');
    INSERT INTO `your_gcp_project.isrpt_isbert_stage.job_log` (job_kennung, eintrags_nr, status, message, created_at)
    VALUES (p_JobKennung, p_EintragsNr, 'ERROR', v_err_msg, CURRENT_TIMESTAMP());
    RAISE USING MESSAGE = v_err_msg;
  END IF;

  -- 2. Deactivate older active jobs (derived from KSH script's intent)
  UPDATE `your_gcp_project.isrpt_isbert_stage.job_table`
  SET active_flag = FALSE, updated_at = CURRENT_TIMESTAMP()
  WHERE job_name = v_TabName
    AND active_flag = TRUE
    AND eintrags_nr <> p_EintragsNr;

  -- 3. Ignore currently active jobs (derived from KSH script's intent)
  IF EXISTS (SELECT 1 FROM `your_gcp_project.isrpt_isbert_stage.job_table` WHERE job_name = v_TabName AND active_flag = TRUE AND eintrags_nr = p_EintragsNr) THEN
    INSERT INTO `your_gcp_project.isrpt_isbert_stage.job_log` (job_kennung, eintrags_nr, status, message, created_at)
    VALUES (p_JobKennung, p_EintragsNr, 'SKIPPED', 'Aktiver Job ignoriert, da bereits aktiv', CURRENT_TIMESTAMP());
    SELECT 0 AS records_processed;
    RETURN; -- Exit procedure
  END IF;

  -- 4. Create job entry (replaces implicit action in starteSQLSkript)
  INSERT INTO `your_gcp_project.isrpt_isbert_stage.job_table` (job_name, job_kennung, eintrags_nr, active_flag, created_at)
  VALUES (v_TabName, p_JobKennung, p_EintragsNr, TRUE, CURRENT_TIMESTAMP());

  -- Determine v_process_date (replaces Oracle COLUMN/SELECT for v_datum)
  SET v_process_date = (
    SELECT
      IFNULL(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
    FROM
      `your_gcp_project.isrpt_isbert_stage.dwtk_meldungen` AS m
    WHERE
      m.job_kennung = 'BERT_DROP_TEMP_TABLE'
  );

  -- 5. Execute core SQL logic (replaces invocation of d_ausd_v_ta_discount_rr.sql)
  -- Truncation (replaces DWPA_UTIL_SKRIPT.runstatement)
  TRUNCATE TABLE `your_gcp_project.isrpt_isbert_stage.sof_ta_discount_rr`;

  -- Main Data Transformation (replaces Oracle INSERT ... SELECT)
  INSERT INTO `your_gcp_project.isrpt_isbert_stage.sof_ta_discount_rr` (
    cntrct_id,
    discount_id,
    disc_vector_ty,
    cntrct_obj_version,
    cntrct_template_id,
    disc_invoice_item_id,
    rabatt,
    rabatthoehe,
    rabattierte_rech_pos,
    processing_timestamp
  )
  SELECT
    da.cntrct_id,
    da.discount_id,
    d.disc_vector_ty,
    da.cntrct_obj_version,
    d.cntrct_template_id,
    d.disc_invoice_item_id,
    cd.cds_description AS rabatt,
    dv.CALC_RULE_VALUE AS rabatthoehe,
    cdii.CDS_DESCRIPTION AS rabattierte_rech_pos,
    CURRENT_TIMESTAMP() AS processing_timestamp
  FROM
    `your_gcp_project.isrpt_isbert_stage.cds_ta_discount_bc_assoc` AS da
  JOIN
    `your_gcp_project.isrpt_isbert_stage.cds_ta_discount` AS d ON da.discount_id = d.discount_id
  JOIN
    `your_gcp_project.isrpt_isbert_stage.cds_ta_care_description` AS cd ON cd.cds_description_id = d.CDS_DESCRIPTION_ID
  JOIN
    `your_gcp_project.isrpt_isbert_stage.cds_ta_disc_vector` AS dv ON d.discount_id = dv.discount_id
      AND d.disc_vector_ty = dv.disc_vector_ty
      AND d.obj_version = dv.discount_obj_version
  JOIN
    `your_gcp_project.isrpt_isbert_stage.cds_ta_disc_invoice_item` AS dii ON d.DISC_INVOICE_ITEM_ID = dii.DISC_INVOICE_ITEM_ID
  JOIN
    `your_gcp_project.isrpt_isbert_stage.cds_ta_care_description` AS cdii ON dii.CDS_DESCRIPTION_ID = cdii.CDS_DESCRIPTION_ID
  WHERE
    cd.LANGUAGE = 1
    AND cdii.LANGUAGE = 1
    AND da.insert_at <= PARSE_DATE('%Y%m%d', v_process_date)
    AND (da.modified_at IS NULL OR da.modified_at > PARSE_DATE('%Y%m%d', v_process_date))
    AND d.insert_at <= PARSE_DATE('%Y%m%d', v_process_date)
    AND (d.modified_at IS NULL OR d.modified_at > PARSE_DATE('%Y%m%d', v_process_date))
    AND d.valid_from <= PARSE_DATE('%Y%m%d', v_process_date)
    AND (d.valid_to IS NULL OR d.valid_to > PARSE_DATE('%Y%m%d', v_process_date))
    AND dv.insert_at <= PARSE_DATE('%Y%m%d', v_process_date)
    AND (dv.modified_at IS NULL OR dv.modified_at > PARSE_DATE('%Y%m%d', v_process_date))
    AND d.is_production = 1
    AND dii.insert_at <= PARSE_DATE('%Y%m%d', v_process_date)
    AND (dii.modified_at IS NULL OR dii.modified_at > PARSE_DATE('%Y%m%d', v_process_date));

  -- 6. Capture record count (replaces reading from temp file)
  SET v_records = (SELECT COUNT(*) FROM `your_gcp_project.isrpt_isbert_stage.sof_ta_discount_rr` WHERE DATE(processing_timestamp) = CURRENT_DATE()); -- Assuming today's run inserted these records. More robust filtering might be needed if multiple runs can happen on the same day for the same job.

  -- 7. Log completion
  INSERT INTO `your_gcp_project.isrpt_isbert_stage.job_log` (job_kennung, eintrags_nr, status, message, records_processed, created_at)
  VALUES (p_JobKennung, p_EintragsNr, 'SUCCESS', '---------- ENDE Datenverarbeitung ----------', v_records, CURRENT_TIMESTAMP());

  -- Update job table for successful completion
  UPDATE `your_gcp_project.isrpt_isbert_stage.job_table`
  SET active_flag = FALSE, updated_at = CURRENT_TIMESTAMP()
  WHERE job_name = v_TabName
    AND job_kennung = p_JobKennung
    AND eintrags_nr = p_EintragsNr;

  SELECT v_records AS records_processed;
EXCEPTION WHEN ERROR THEN
  SET v_err_msg = 'Fehler in sp_ausd_v_ta_discount_rr: ' || ERROR_MESSAGE();
  INSERT INTO `your_gcp_project.isrpt_isbert_stage.job_log` (job_kennung, eintrags_nr, status, message, created_at)
  VALUES (p_JobKennung, p_EintragsNr, 'ERROR', v_err_msg, CURRENT_TIMESTAMP());
  -- Update job table to reflect error and deactivate
  UPDATE `your_gcp_project.isrpt_isbert_stage.job_table`
  SET active_flag = FALSE, updated_at = CURRENT_TIMESTAMP()
  WHERE job_name = v_TabName
    AND job_kennung = p_JobKennung
    AND eintrags_nr = p_EintragsNr;
  RAISE USING MESSAGE = v_err_msg;
END;