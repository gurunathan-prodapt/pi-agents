-- Inner business logic procedure placeholder.
-- Migrate the Oracle SQL from d_ausd_bp_ta_bpr_apn.sql into BigQuery SQL here.
-- This procedure is intentionally modular so the wrapper can remain stable.

CREATE OR REPLACE PROCEDURE `${GCP_PROJECT_ID}.${GCP_DATASET}.d_ausd_bp_ta_bpr_apn`(
  p_JobKennung STRING,
  p_EintragsNr STRING,
  p_Stichtag STRING,
  p_wiederanlaufWert STRING,
  p_datum_heute DATE,
  p_datum_gestern DATE
)
BEGIN
  DECLARE v_stichtag_date DATE DEFAULT SAFE.PARSE_DATE('%d%m%Y', p_Stichtag);

  IF v_stichtag_date IS NULL THEN
    RAISE USING MESSAGE = CONCAT('Ungültiges Datum in innerer Prozedur: ', p_Stichtag);
  END IF;

  -- Example reusable staging pattern
  CREATE TEMP TABLE tmp_source AS
  SELECT
    p_JobKennung AS job_kennung,
    p_EintragsNr AS eintrags_nr,
    v_stichtag_date AS stichtag,
    p_wiederanlaufWert AS restart_value,
    p_datum_heute AS datum_heute,
    p_datum_gestern AS datum_gestern;

  -- Target write implementation
  INSERT INTO `${GCP_PROJECT_ID}.${GCP_DATASET}.poolbasisprodukt` (
    job_kennung,
    eintrags_nr,
    stichtag,
    restart_value,
    created_at
  )
  SELECT
    job_kennung,
    eintrags_nr,
    stichtag,
    restart_value,
    CURRENT_TIMESTAMP()
  FROM tmp_source;

END;