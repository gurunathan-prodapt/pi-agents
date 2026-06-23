-- Dataform SQL model for the main OUTPUT_SQL logic
-- Replaces: OUTPUT_SQL executed via sqlplus in vobs/dw_source/isdwh/exporter/is/bin/r_exis_v2
-- This file would be part of a Dataform project. It should be parameterized to accept
-- 'from_date', 'to_date', and other dynamic variables.

-- config {
--   type: "table",
--   schema: "`your_bigquery_dataset`",
--   name: "r_exis_v2_output_data",
--   description: "Main output data for r_exis_v2 exporter, processed and ready for export."
-- }

-- Declare variables for Dataform execution (example)
-- declare from_date DEFAULT "${FROM_DATE}";
-- declare to_date DEFAULT "${TO_DATE}";
-- declare job_name DEFAULT "${JOB_NAME}";

-- Example SQL: This should be replaced by the actual OUTPUT_SQL from the legacy config
SELECT
    current_timestamp() AS export_timestamp,
    PARSE_DATE('%Y-%m-%d', "${FROM_DATE}") AS data_from_date,
    PARSE_DATE('%Y-%m-%d', "${TO_DATE}") AS data_to_date,
    id,
    column1,
    column2,
    -- Apply BigQuery SQL transformations that replace simple nawk/sed operations
    REPLACE(description, 'old_value', 'new_value') AS transformed_description
FROM
    `your_gcp_project_id.your_bigquery_dataset.dwh_source_table` -- Replaced Oracle DWH$TA_K_MELDUNGEN or other source tables
WHERE
    event_date BETWEEN PARSE_DATE('%Y-%m-%d', "${FROM_DATE}") AND PARSE_DATE('%Y-%m-%d', "${TO_DATE}")
    AND some_filter_column = 'some_value'
;