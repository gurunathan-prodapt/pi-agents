-- Optional reusable utility pattern for parameter validation
-- File: bigquery/stored_procedures/sp_validate_ddmmyyyy.sql

CREATE OR REPLACE PROCEDURE `project_id.isbert_dataset.sp_validate_ddmmyyyy`(
  p_value STRING
)
BEGIN
  DECLARE v_date DATE;

  BEGIN
    SET v_date = PARSE_DATE('%d%m%Y', p_value);
  EXCEPTION WHEN ERROR THEN
    RAISE USING MESSAGE = CONCAT('Ungueltiges Datum: ', p_value);
  END;

  SELECT v_date AS parsed_date;
END;