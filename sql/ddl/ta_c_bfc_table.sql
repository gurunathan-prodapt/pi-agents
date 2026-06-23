--
-- BigQuery DDL for the target table `ta_c_bfc`
-- Represents the target table for data processing from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_c_bfc.ksh
-- NOTE: Column definitions are inferred from usage in the SQL script and are placeholders.
--       Actual data types and constraints should be refined based on source system schema.
--

CREATE TABLE IF NOT EXISTS `your_project.your_dataset.ta_c_bfc` (
    cntrct_id STRING NOT NULL OPTIONS(description="Contract ID"),
    bindefrist DATE OPTIONS(description="Bindefrist (commitment period) date"),
    bfc_age DATE OPTIONS(description="Bindefrist age date"),
    bfc_count INT64 OPTIONS(description="Bindefrist count"),
    bfc_procedure DATE OPTIONS(description="Date of the Bindefrist procedure execution"),
    commitment_reference_date DATE OPTIONS(description="Commitment reference date"),
    cntrct_validity_id STRING OPTIONS(description="Contract validity ID")
)
OPTIONS(
    description="Target table for Bindefrist caching"
);