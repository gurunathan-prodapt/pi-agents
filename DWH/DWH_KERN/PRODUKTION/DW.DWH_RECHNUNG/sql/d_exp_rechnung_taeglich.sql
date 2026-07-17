-- Purpose: Extract daily invoice data from the T_RECHNUNG table
-- Parameterized Stichtag is supplied dynamically at execution runtime.
SELECT 
  rechnungs_id,
  rechnungs_datum,
  kundennummer,
  betrag,
  waehrung,
  referenz_id
FROM 
  `@gcp_project.@bq_dataset.t_rechnung`
WHERE 
  rechnungs_datum = PARSE_DATE('%Y%m%d', '@l_stichtag');