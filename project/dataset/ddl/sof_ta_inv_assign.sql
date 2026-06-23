--
-- Target BigQuery DDL for table project.dataset.sof_ta_inv_assign
-- Replaces Oracle table sof$ta_inv_assign.
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_assign.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_assign.ksh
--
-- Note: The schema for this table is inferred to be identical to
-- project.dataset.cds_ta_inv_assignment, as per the migration design document
-- which states that data is inserted from cds_ta_inv_assignment into sof_ta_inv_assign.
-- Placeholder columns `assignment_id` and `some_value` are added as the full
-- source schema was not provided in the design document.
--
CREATE TABLE IF NOT EXISTS project.dataset.sof_ta_inv_assign (
    assignment_id STRING NOT NULL OPTIONS(description="Unique identifier for the assignment"),
    insert_at TIMESTAMP OPTIONS(description="Timestamp when the record was inserted"),
    modified_at TIMESTAMP OPTIONS(description="Timestamp when the record was last modified"),
    valid_from TIMESTAMP OPTIONS(description="Start date of validity"),
    valid_to TIMESTAMP OPTIONS(description="End date of validity"),
    is_production INT64 OPTIONS(description="Flag indicating if the assignment is for production (1 for production, 0 otherwise)"),
    some_value STRING OPTIONS(description="Placeholder for other relevant assignment data columns")
)
PARTITION BY
    DATE(insert_at)
CLUSTER BY
    assignment_id;