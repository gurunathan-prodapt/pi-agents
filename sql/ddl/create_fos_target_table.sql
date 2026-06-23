-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_einzeln.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_einzeln.ksh
-- Description: DDL for the FOS target table, where processed contract data is stored.

CREATE TABLE IF NOT EXISTS `project.dataset.fos_target_table` (
    dwh_vertrag_id INT64 NOT NULL OPTIONS(description="Unique identifier for the contract."),
    vertrag_nr STRING OPTIONS(description="Contract number."),
    kunde_id STRING OPTIONS(description="Customer ID associated with the contract."),
    gueltig_von DATE OPTIONS(description="Start date of contract validity."),
    gueltig_bis DATE OPTIONS(description="End date of contract validity."),
    ladedatum DATE OPTIONS(description="Load date of the record from source."),
    produkt_typ STRING OPTIONS(description="Type of product."),
    payload JSON OPTIONS(description="Generic field for additional contract data in JSON format."),
    processing_job_id STRING OPTIONS(description="Job ID that last processed and inserted this record."),
    processing_timestamp TIMESTAMP OPTIONS(description="Timestamp when this record was last processed.")
)
OPTIONS(
    description="Target table for the Forderungsscoring (FOS) system, receiving processed contract data."
);