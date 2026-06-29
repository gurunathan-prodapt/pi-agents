-- BigQuery Standard SQL
-- File: bigquery/stored_procedures/sp_k_ausd_bp_ta_bpr_apn.sql

CREATE OR REPLACE PROCEDURE `project_id.isbert_dataset.sp_k_ausd_bp_ta_bpr_apn`(
  p_JobKennung STRING,
  p_EintragsNr STRING,
  p_Stichtag STRING,
  p_wiederanlaufWert STRING
)
BEGIN
  ----------------------------------------------------------------------
  -- Reusable validation helpers
  ----------------------------------------------------------------------
  DECLARE v_TabName STRING DEFAULT 'PoolBasisprodukt';
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_datum_heute DATE DEFAULT CURRENT_DATE();
  DECLARE v_datum_gestern DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
  DECLARE v_stichtag_date DATE;
  DECLARE v_restart_value STRING DEFAULT '0';
  DECLARE v_err_msg STRING DEFAULT '';
  DECLARE v_err_nr INT64 DEFAULT 0;

  ----------------------------------------------------------------------
  -- Modular validation block
  ----------------------------------------------------------------------
  IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
    SET v_err_nr = 1;
    SET v_err_msg = 'Jobkennung fehlt';
  END IF;

  IF v_err_nr = 0 AND (p_EintragsNr IS NULL OR TRIM(p_EintragsNr) = '') THEN
    SET v_err_nr = 2;
    SET v_err_msg = 'EintragsNr fehlt';
  END IF;

  IF v_err_nr = 0 AND (p_Stichtag IS NULL OR TRIM(p_Stichtag) = '') THEN
    SET v_err_nr = 3;
    SET v_err_msg = 'Stichtag fehlt';
  END IF;

  IF v_err_nr != 0 THEN
    RAISE USING MESSAGE = CONCAT('FEHLER: ', CAST(v_err_nr AS STRING), ' - ', v_err_msg);
  END IF;

  ----------------------------------------------------------------------
  -- Modular date parsing / validation
  ----------------------------------------------------------------------
  BEGIN
    SET v_stichtag_date = PARSE_DATE('%d%m%Y', p_Stichtag);
  EXCEPTION WHEN ERROR THEN
    RAISE USING MESSAGE = CONCAT('FEHLER: Ungueltiges Datum im Format DDMMYYYY: ', p_Stichtag);
  END;

  SET v_restart_value = IFNULL(NULLIF(TRIM(p_wiederanlaufWert), ''), '0');

  ----------------------------------------------------------------------
  -- Core transformation placeholder
  -- Replace this section with migrated logic from d_ausd_bp_ta_bpr_apn.sql
  ----------------------------------------------------------------------
  CREATE TEMP TABLE tmp_poolbasisprodukt AS
  SELECT
    *
  FROM `project_id.isbert_dataset.source_poolbasisprodukt`
  WHERE DATE(stichtag) = v_stichtag_date;

  ----------------------------------------------------------------------
  -- Reusable record counting
  ----------------------------------------------------------------------
  SET v_records = (
    SELECT COUNT(1)
    FROM tmp_poolbasisprodukt
  );

  ----------------------------------------------------------------------
  -- Target persistence
  ----------------------------------------------------------------------
  INSERT INTO `project_id.isbert_dataset.PoolBasisprodukt`
  SELECT * FROM tmp_poolbasisprodukt;

  ----------------------------------------------------------------------
  -- Job tracking hook (corresponds to FOSJobErzeugeEintrag)
  ----------------------------------------------------------------------
  INSERT INTO `project_id.isbert_dataset.job_tracking`
    (tab_name, job_kennung, eintrags_nr, stichtag, wiederanlauf_wert, records, created_at)
  VALUES
    (v_TabName, p_JobKennung, p_EintragsNr, v_stichtag_date, v_restart_value, v_records, CURRENT_TIMESTAMP());

  ----------------------------------------------------------------------
  -- Output summary
  ----------------------------------------------------------------------
  SELECT
    v_TabName AS tab_name,
    p_JobKennung AS job_kennung,
    p_EintragsNr AS eintrags_nr,
    p_Stichtag AS stichtag_ddmmyyyy,
    v_stichtag_date AS stichtag_date,
    v_restart_value AS wiederanlauf_wert,
    v_datum_heute AS datum_heute,
    v_datum_gestern AS datum_gestern,
    v_records AS records_processed;
END;