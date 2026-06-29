-- BigQuery Standard SQL
-- File: bigquery/stored_procedures/sp_merge_cibasis_legacy.sql
-- SQL equivalent for the commented sed/sort/join legacy logic

CREATE OR REPLACE PROCEDURE `project_id.isbert_dataset.sp_merge_cibasis_legacy`()
BEGIN
  ----------------------------------------------------------------------
  -- Reusable cleanup function pattern
  ----------------------------------------------------------------------
  CREATE TEMP TABLE data24_clean AS
  SELECT DISTINCT
    REGEXP_REPLACE(line, r'\s+', '') AS line
  FROM `project_id.isbert_dataset.cibasis_data24_source`;

  CREATE TEMP TABLE data96_clean AS
  SELECT DISTINCT
    REGEXP_REPLACE(line, r'\s+', '') AS line
  FROM `project_id.isbert_dataset.cibasis_data96_source`;

  CREATE TEMP TABLE fax_clean AS
  SELECT DISTINCT
    REGEXP_REPLACE(line, r'\s+', '') AS line
  FROM `project_id.isbert_dataset.cibasis_fax_source`;

  ----------------------------------------------------------------------
  -- Reusable parser for delimited lines
  ----------------------------------------------------------------------
  CREATE TEMP TABLE data24_parsed AS
  SELECT
    SPLIT(line, ';')[SAFE_OFFSET(0)] AS join_key,
    SPLIT(line, ';')[SAFE_OFFSET(1)] AS value_24,
    line AS raw_line
  FROM data24_clean;

  CREATE TEMP TABLE data96_parsed AS
  SELECT
    SPLIT(line, ';')[SAFE_OFFSET(0)] AS join_key,
    SPLIT(line, ';')[SAFE_OFFSET(1)] AS value_96,
    line AS raw_line
  FROM data96_clean;

  CREATE TEMP TABLE fax_parsed AS
  SELECT
    SPLIT(line, ';')[SAFE_OFFSET(0)] AS join_key,
    SPLIT(line, ';')[SAFE_OFFSET(1)] AS value_fax,
    line AS raw_line
  FROM fax_clean;

  ----------------------------------------------------------------------
  -- Reusable join logic
  ----------------------------------------------------------------------
  CREATE TEMP TABLE cibasisprodukt AS
  SELECT
    COALESCE(d24.join_key, d96.join_key, fx.join_key) AS join_key,
    d24.value_24,
    d96.value_96,
    fx.value_fax
  FROM data24_parsed d24
  FULL OUTER JOIN data96_parsed d96
    ON d24.join_key = d96.join_key
  FULL OUTER JOIN fax_parsed fx
    ON COALESCE(d24.join_key, d96.join_key) = fx.join_key;

  ----------------------------------------------------------------------
  -- Final output
  ----------------------------------------------------------------------
  SELECT *
  FROM cibasisprodukt
  ORDER BY join_key;
END;