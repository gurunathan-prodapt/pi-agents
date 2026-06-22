-- DDL for fos_table
-- Represents FOS-Tabelle from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_apn.ksh
-- This table receives the processed contract cache data.
CREATE TABLE IF NOT EXISTS `my_project.my_dataset.fos_table` (
    dwh_vertrag_id INT66 NOT NULL OPTIONS(description="Contract identifier"),
    gueltig_von DATE NOT NULL OPTIONS(description="Validity start date"),
    gueltig_bis DATE NOT NULL OPTIONS(description="Validity end date"),
    ladedatum DATE NOT NULL OPTIONS(description="Load date"),
    col_a STRING OPTIONS(description="Example column from source"),
    col_b STRING OPTIONS(description="Example column from source"),
    -- Add other columns derived from contract_cache as needed
    stichtag_lauf DATE NOT NULL OPTIONS(description="The 'Stichtag' used for this run"),
    created_ts TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the record was created"),
    PRIMARY KEY (dwh_vertrag_id, stichtag_lauf) NOT ENFORCED
);