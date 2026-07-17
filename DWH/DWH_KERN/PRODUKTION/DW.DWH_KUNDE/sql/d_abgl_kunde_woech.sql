-- BigQuery Standard SQL conversion of d_abgl_kunde_woech.sql
-- Parameters:
--   @p_Stichtag (STRING) - Format 'YYYYMMDD' passed via execution context

SELECT
  'ABWEICHUNG' AS MARKER,
  k.KUNDE,
  k.NACHNAME,
  k.VORNAME,
  k.PLZ,
  k.ORT,
  k.STRASSE,
  r.PLZ       AS REF_PLZ,
  r.ORT       AS REF_ORT,
  r.STRASSE   AS REF_STRASSE
FROM `DWH_KERN.T_KUNDE` k
JOIN `STAMMDATEN.T_KUNDE_REFERENZ` r
  ON r.KUNDE = k.KUNDE
WHERE k.AKTUALISIERT_AM <= PARSE_DATE('%Y%m%d', @p_Stichtag)
  AND (
        COALESCE(k.PLZ, 'x')     != COALESCE(r.PLZ, 'x')
     OR COALESCE(k.ORT, 'x')     != COALESCE(r.ORT, 'x')
     OR COALESCE(k.STRASSE, 'x') != COALESCE(r.STRASSE, 'x')
      )
ORDER BY k.KUNDE;