-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_beschr.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_beschr.ksh
-- Description: Assumed schema for the target table 'fos_tabelle'.
-- This schema is inferred from the design document and should be verified against the actual k_ausd_bp_ta_bpr_beschr.ksh content.
CREATE TABLE IF NOT EXISTS project.dataset.fos_tabelle (
    DWH_VERTRAG_ID STRING NOT NULL OPTIONS(description="Contract identifier from DWH"),
    -- Add other relevant columns for FOS system as needed
    FOS_ATTRIBUTE_1 STRING,
    FOS_PROVISION_DATE DATE,
    FOS_PROVISION_VALUE NUMERIC,
    CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY(DWH_VERTRAG_ID) NOT ENFORCED
);