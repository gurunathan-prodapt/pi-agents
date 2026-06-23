-- BigQuery Stored Procedure for core data transformation
-- This procedure encapsulates the logic from the original d_ausd_bp_ta_cntrct_dist.sql
-- Legacy source: d_ausd_bp_ta_cntrct_dist.sql (called by k_ausd_bp_ta_cntrct_dist.ksh)
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_cntrct_dist.ksh

CREATE OR REPLACE PROCEDURE `your_project_id.your_dataset_id.d_ausd_bp_ta_cntrct_dist_core`(
  IN p_EintragsNr STRING,
  IN p_JobKennung STRING,
  IN p_Stichtag DATE,
  IN p_datum_heute DATE,
  IN p_datum_gestern DATE
)
BEGIN
  -- Placeholder for the translated SQL content of d_ausd_bp_ta_cntrct_dist.sql.
  -- This procedure should contain the actual business logic to read from source tables
  -- and insert/update `your_project_id.your_dataset_id.PoolBasisprodukt`.

  -- Example: Insert dummy data into PoolBasisprodukt
  INSERT INTO `your_project_id.your_dataset_id.PoolBasisprodukt` (
    contract_id, product_type, start_date, end_date, value, stichtag, processing_timestamp
  )
  SELECT
    FORMAT('CONTRACT_%s', p_EintragsNr) AS contract_id,
    p_JobKennung AS product_type,
    p_datum_gestern AS start_date,
    p_datum_heute AS end_date,
    CAST(p_EintragsNr AS NUMERIC) * 100 AS value,
    p_Stichtag AS stichtag,
    CURRENT_TIMESTAMP() AS processing_timestamp
  WHERE NOT EXISTS (
    SELECT 1 FROM `your_project_id.your_dataset_id.PoolBasisprodukt`
    WHERE contract_id = FORMAT('CONTRACT_%s', p_EintragsNr) AND stichtag = p_Stichtag
  );

  -- You might need to add MERGE, UPDATE, or more complex SELECT/INSERT statements here
  -- based on the original d_ausd_bp_ta_cntrct_dist.sql content.

EXCEPTION WHEN ERROR THEN
  INSERT INTO `your_project_id.your_dataset_id.error_log` (
    job_id, entry_number, reference_date, error_message, component, severity
  ) VALUES (
    p_JobKennung, p_EintragsNr, p_Stichtag, ERROR_MESSAGE(), 'd_ausd_bp_ta_cntrct_dist_core', 'ERROR'
  );
  RAISE;
END;