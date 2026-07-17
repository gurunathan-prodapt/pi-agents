/*
  ==============================================================================
  SOURCE: NOT FOUND — sql/d_abgl_kunde_woech.sql (Placeholder & Blueprint)
  ==============================================================================
  Target System: BigQuery (Standard SQL Syntax)
  Job Parameters: 
    - Execution Date Variable: @lauf_woche (Format: YYYYMMDD)
    - Metadata Job Identifier: 'KUNDE_ABGL_WOECHENTLICH'

  Below is the optimized BigQuery-ready translation of the Oracle structural model:
*/

-- Step 1: Query discrepancies between Master Customer data and Reference systems
SELECT 
  PARSE_DATE('%Y%m%d', @lauf_woche) AS execution_date,
  'KUNDE_ABGL_WOECHENTLICH' AS job_kennung,
  m.kunde_id,
  m.name AS master_name,
  m.adresse AS master_adresse,
  r.adresse AS reference_adresse,
  CURRENT_TIMESTAMP() AS reconciliation_timestamp
FROM 
  `@GCP_PROJECT.@BQ_DATASET.kunde_master` AS m
INNER JOIN 
  `@GCP_PROJECT.@BQ_DATASET.kunde_reference` AS r
  ON m.kunde_id = r.kunde_id
WHERE 
  -- Identifies discrepancies in address records
  m.adresse != r.adresse;