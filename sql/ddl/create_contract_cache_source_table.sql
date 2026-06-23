-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_einzeln.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_einzeln.ksh
-- Description: DDL for the source contract cache data table, replacing DWH$TA_C_VERTRAG.

CREATE TABLE IF NOT EXISTS `project.dataset.contract_cache_source` (
    dwh_vertrag_id INT64 NOT NULL OPTIONS(description="Unique identifier for the contract, used for restart logic."),
    vertrag_nr STRING OPTIONS(description="Contract number."),
    kunde_id STRING OPTIONS(description="Customer ID associated with the contract."),
    gueltig_von DATE OPTIONS(description="Start date of contract validity."),
    gueltig_bis DATE OPTIONS(description="End date of contract validity."),
    ladedatum DATE OPTIONS(description="Load date of this record into the DWH."),
    produkt_typ STRING OPTIONS(description="Type of product (e.g., FAX, Data24)."),
    payload JSON OPTIONS(description="Generic field for additional contract data in JSON format.")
)
OPTIONS(
    description="BigQuery equivalent of DWH$TA_C_VERTRAG, storing contract cache data."
);