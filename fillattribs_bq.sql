-- BigQuery SQL UDF/Stored Procedure for attribute filling/placeholder resolution
-- Replaces: 'fillattribs' function in vobs/dw_source/isdwh/exporter/is/bin/r_exis_v2
-- This UDF takes a template string with placeholders (e.g., ${MY_VAR}) and a JSON object of attributes,
-- and replaces the placeholders with corresponding attribute values. This is crucial for dynamic SQL generation.

CREATE OR REPLACE FUNCTION `your_gcp_project_id.your_bigquery_dataset.fillattribs_bq`(
    template_string STRING,
    attributes JSON
) RETURNS STRING AS (
    (
        SELECT
            REPLACE(
                template_string,
                '${' || attribute_key || '}',
                COALESCE(JSON_VALUE(attributes, '$[' || '"' || attribute_key || '"' || ']'), '')
            )
        FROM
            UNNEST(JSON_QUERY_KEYS(attributes)) AS attribute_key
    )
);

-- Note: The current BigQuery UDF limitation makes it challenging to implement iterative or recursive string replacement
-- within a single UDF. The above UDF is a simplified version and might need to be called multiple times
-- or integrated within a Stored Procedure for complex scenarios.
-- For a robust solution, dynamic SQL generation will likely be handled within Airflow Python tasks using string formatting
-- or Jinja templating, or in Dataform for more structured templating.