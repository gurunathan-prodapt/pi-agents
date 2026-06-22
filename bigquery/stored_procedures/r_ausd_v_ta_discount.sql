-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_discount.ksh
-- This stored procedure migrates the logic from d_ausd_v_ta_discount.sql to BigQuery.
CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_v_ta_discount`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING
)
BEGIN
  DECLARE ErrNr INT64 DEFAULT 0;
  DECLARE ErrArg STRING DEFAULT '';
  DECLARE v_datum_string STRING DEFAULT '';
  DECLARE v_datum_date DATE DEFAULT DATE '1900-01-01';
  DECLARE v_records INT64 DEFAULT 0;

  -- Parameter validation (replacing shell getopts and checks)
  IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
    SET ErrNr = 1;
    SET ErrArg = 'Jobkennung';
  END IF;

  IF p_EintragsNr IS NULL OR p_EintragsNr = '' THEN
    SET ErrNr = 1;
    SET ErrArg = 'EintragsNr';
  END IF;

  IF ErrNr <> 0 THEN
    SELECT FORMAT('FEHLER: %d %s ist ein Pflichtparameter und darf nicht leer sein.', ErrNr, ErrArg) AS error_message;
    RAISE; -- Raise an exception to stop execution
  END IF;

  -- Determine cutoff date from control table (replacing Oracle SELECT ... DWTK_MELDUNGEN)
  -- The original logic used YYYYMMDD string format. We will store it as a string first, then parse to date.
  SET v_datum_string = (
    SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(DATE(timecreated))), '19000101')
    FROM `project.isbert_schema.dwtk_meldungen`
    WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
  );

  SET v_datum_date = PARSE_DATE('%Y%m%d', v_datum_string);

  -- Refresh target table (replacing Oracle TRUNCATE TABLE)
  TRUNCATE TABLE `project.dataset.sof_ta_discount`;

  -- Insert refreshed data (translating Oracle INSERT SELECT with joins and filters)
  INSERT INTO `project.dataset.sof_ta_discount` (
    cntrct_id,
    discount_id,
    disc_vector_ty,
    cntrct_obj_version,
    rabatt,
    rabatthoehe
  )
  SELECT
    da.cntrct_id,
    da.discount_id,
    d.disc_vector_ty,
    da.cntrct_obj_version,
    cd.cds_description AS rabatt,
    CAST(dv.calc_rule_value AS STRING) AS rabatthoehe -- Assuming calc_rule_value can be cast to STRING
  FROM `project.source.cds_ta_discount_bc_assoc` AS da -- Source tables need to be present in BigQuery
  JOIN `project.source.cds_ta_discount` AS d
    ON da.discount_id = d.discount_id
  JOIN `project.source.cds_ta_care_description` AS cd
    ON cd.cds_description_id = d.cds_description_id
   AND cd.language = 1
  JOIN `project.source.cds_ta_disc_vector` AS dv
    ON d.discount_id = dv.discount_id
   AND d.disc_vector_ty = dv.disc_vector_ty
   AND d.obj_version = dv.discount_obj_version
  WHERE
    -- Date filtering for da
    (da.insert_at <= v_datum_date AND (da.modified_at IS NULL OR da.modified_at > v_datum_date))
    -- Date filtering for d
    AND (d.insert_at <= v_datum_date AND (d.modified_at IS NULL OR d.modified_at > v_datum_date))
    AND (d.valid_from <= v_datum_date AND (d.valid_to IS NULL OR d.valid_to > v_datum_date))
    -- Date filtering for dv
    AND (dv.insert_at <= v_datum_date AND (dv.modified_at IS NULL OR dv.modified_at > v_datum_date))
    -- Additional filters
    AND d.is_production = 1;

  SET v_records = (
    SELECT COUNT(*) FROM `project.dataset.sof_ta_discount`
  );

  -- Log completion and record count
  SELECT
    'VERARBEITUNG_FERTIG' AS status,
    v_datum_string AS cutoff_date,
    v_records AS records_loaded;
END;