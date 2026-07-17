-- Target transformation mapping for consolidated sales revenue data.
-- TODO: No source found for umsatz_konsolidierung.mp. Confirm original schema targets.

INSERT INTO `{gcp_project}.{bq_dataset}.umsatz_konsolidiert` (
    verarbeitungsmonat,
    konzerngesellschaft,
    buchungsdatum,
    umsatz_betrag,
    waehrung,
    konsolidierungs_zeitpunkt
)
SELECT
    @verarbeitungsmonat AS verarbeitungsmonat,
    konzerngesellschaft,
    DATE(buchungsdatum) AS buchungsdatum,
    SUM(umsatz_betrag) AS umsatz_betrag,
    waehrung,
    CURRENT_TIMESTAMP() AS konsolidierungs_zeitpunkt
FROM `{gcp_project}.{bq_dataset}.umsatz_rohdaten`
WHERE 
    FORMAT_DATE('%Y%m', DATE(buchungsdatum)) = @verarbeitungsmonat
    AND (@konzerngesellschaft = 'ALL' OR konzerngesellschaft = @konzerngesellschaft)
GROUP BY 
    konzerngesellschaft, 
    buchungsdatum, 
    waehrung;