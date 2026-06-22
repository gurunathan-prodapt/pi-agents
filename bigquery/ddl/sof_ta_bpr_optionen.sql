-- DDL for project.dataset.sof_ta_bpr_optionen
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_optionen.ksh
-- This DDL is a best-effort inference from the design document.
-- Adjust data types and sizes based on actual source system schema.

CREATE TABLE `your-gcp-project-id.your-dataset.sof_ta_bpr_optionen`
(
    cntrct_id INT64 NOT NULL OPTIONS(description="Contract ID"),
    bpr_id    INT64 NOT NULL OPTIONS(description="Base Product ID")
    -- Add other columns that might exist in the target table based on full schema.
)
OPTIONS(
    description="Target table for base product tariff options."
);