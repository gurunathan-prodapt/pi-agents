-- BigQuery SQL compliant extraction query
-- Note: 'p_Stichtag' is defined as a standard query parameter (@p_Stichtag) to replace the legacy CLI variable.

SELECT
  r.RECHNUNGSNUMMER,
  r.VERTRAG,
  r.KUNDE,
  r.TARIF,
  r.ABRECHNUNGSZEITRAUM,
  r.RECHNUNGSBETRAG,
  r.WAEHRUNG,
  r.RECHNUNGSDATUM
FROM
  `DWH_KERN.T_RECHNUNG` AS r
WHERE
  r.RECHNUNGSDATUM = PARSE_DATE('%Y%m%d', @p_Stichtag)
ORDER BY
  r.RECHNUNGSNUMMER;