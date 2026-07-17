-- =============================================================================
-- SQL-Script: d_exp_rechnung_taeglich.sql
-- Target Dialect: BigQuery Standard SQL
-- Description: Extracts daily billing records from T_RECHNUNG filtered by stichtag.
-- =============================================================================

SELECT
  r.RECHNUNGSNUMMER,
  r.VERTRAG,
  r.KUNDE,
  r.TARIF,
  r.ABRECHNUNGSZEITRAUM,
  CAST(r.RECHNUNGSBETRAG AS NUMERIC) AS RECHNUNGSBETRAG,
  r.WAEHRUNG,
  r.RECHNUNGSDATUM
FROM 
  `@gcp_project.@bq_dataset.T_RECHNUNG` AS r
WHERE 
  r.RECHNUNGSDATUM = PARSE_DATE('%Y%m%d', @p_Stichtag)
ORDER BY 
  r.RECHNUNGSNUMMER;