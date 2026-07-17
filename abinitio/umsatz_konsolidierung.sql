-- Target File: abinitio/umsatz_konsolidierung.sql
-- Description: Core consolidation logic extracted from umsatz_konsolidierung.mp

INSERT OVERWRITE `@BQ_DATASET.fact_umsatz_konsolidiert`
(
    verarbeitungsmonat,
    konzerngesellschaft,
    buchungsdatum,
    umsatz_betrag,
    waehrung,
    konsolidierungs_datum
)
SELECT
    @verarbeitungsmonat AS verarbeitungsmonat,
    konzerngesellschaft,
    buchungsdatum,
    SUM(umsatz_betrag) AS umsatz_betrag,
    waehrung,
    CURRENT_TIMESTAMP() AS konsolidierungs_datum
FROM
    `@BQ_DATASET.stg_umsatz`
WHERE
    verarbeitungsmonat = @verarbeitungsmonat
    AND (@konzerngesellschaft = 'ALL' OR konzerngesellschaft = @konzerngesellschaft)
GROUP BY
    konzerngesellschaft,
    buchungsdatum,
    waehrung;