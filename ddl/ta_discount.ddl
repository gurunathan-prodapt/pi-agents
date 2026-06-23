-- BigQuery DDL for the ta_discount table
-- Replaces legacy table updated by vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_discount.ksh
--
-- This DDL defines the target table. The actual data insertion/update
-- will be handled by the BigQuery Stored Procedure.
--
-- NOTE: The schema is inferred from the translated d_ausd_v_ta_discount.sql.
-- Please review and adjust data types and add any missing columns,
-- partitioning, or clustering based on full source analysis.

CREATE TABLE IF NOT EXISTS `project_id.dataset_id.ta_discount` (
    cntrct_id STRING,
    discount_id STRING,
    disc_vector_ty STRING,
    cntrct_obj_version STRING, -- Assuming this is a version string or numeric that can be cast to string
    rabatt STRING,
    rabatthoehe STRING
)
-- Optionally, add partitioning and clustering based on expected query patterns
-- PARTITION BY DATE(insert_date_column)
-- CLUSTER BY cntrct_id, discount_id
OPTIONS(
    description = "Migrated ta_discount table from legacy system."
);