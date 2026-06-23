-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_beschr.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_beschr.ksh
-- Description: Assumed schema for the source table 'ta_vertrag_cache'.
-- This schema is inferred from the design document and should be verified against the actual k_ausd_bp_ta_bpr_beschr.ksh content.
CREATE TABLE IF NOT EXISTS project.dataset.ta_vertrag_cache (
    DWH_VERTRAG_ID STRING NOT NULL OPTIONS(description="Contract identifier from DWH"),
    Gueltig_von DATE NOT NULL OPTIONS(description="Validity start date"),
    Gueltig_bis DATE NOT NULL OPTIONS(description="Validity end date"),
    LADEDATUM DATE NOT NULL OPTIONS(description="Load date for the contract cache entry"),
    -- Add other relevant columns from the source system as needed
    FOSHoleLadedatum DATE OPTIONS(description="FOS-specific load date, if applicable"),
    -- Placeholder for other contract cache data
    ATTRIBUTE_1 STRING,
    ATTRIBUTE_2 INT64,
    PROVISION_VALUE NUMERIC,
    PRIMARY KEY(DWH_VERTRAG_ID, LADEDATUM) NOT ENFORCED
);