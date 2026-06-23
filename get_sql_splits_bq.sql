-- BigQuery SQL Stored Procedure for determining SQL-based partitioning logic
-- Replaces: 'getsqlsplits' function in vobs/dw_source/isdwh/exporter/is/bin/r_exis_v2
-- This Stored Procedure will calculate SQL split boundaries based on input parameters (e.g., partitioning column, table).
-- It can output a table of split details to be consumed by Airflow for parallel query execution.

CREATE OR REPLACE PROCEDURE `your_gcp_project_id.your_bigquery_dataset.get_sql_splits_bq`(
    IN p_table_name STRING,
    IN p_partition_column STRING,
    IN p_num_splits INT,
    OUT p_split_details ARRAY<STRUCT<split_id INT, min_value STRING, max_value STRING>>
)
BEGIN
    DECLARE min_col_val STRING;
    DECLARE max_col_val STRING;
    DECLARE query_string STRING;
    DECLARE interval_size FLOAT64;
    DECLARE current_min_val STRING;
    DECLARE current_max_val STRING;
    DECLARE splits ARRAY<STRUCT<split_id INT, min_value STRING, max_value STRING>>;
    DECLARE i INT64 DEFAULT 1;

    -- Get min and max values for the partition column
    SET query_string = FORMAT_BQM_QUERY(
        "SELECT CAST(MIN(%s) AS STRING), CAST(MAX(%s) AS STRING) FROM `%s.%s.%s`",
        p_partition_column, p_partition_column, GCP_PROJECT_ID, BIGQUERY_DATASET, p_table_name
    );
    EXECUTE IMMEDIATE query_string INTO min_col_val, max_col_val;

    IF min_col_val IS NULL OR max_col_val IS NULL OR p_num_splits <= 0 THEN
        SET p_split_details = ARRAY<STRUCT<split_id INT, min_value STRING, max_value STRING>>[];
        RETURN;
    END IF;

    -- Assume numeric or date-like partitioning for simplicity.
    -- More complex logic would be needed for string partitioning or specific data types.
    -- For numeric or date, we can cast to FLOAT64 for interval calculation.
    SET interval_size = (CAST(max_col_val AS FLOAT64) - CAST(min_col_val AS FLOAT64)) / p_num_splits;
    
    SET splits = ARRAY<STRUCT<split_id INT, min_value STRING, max_value STRING>>[];
    SET current_min_val = min_col_val;

    WHILE i <= p_num_splits DO
        SET current_max_val = CAST(CAST(min_col_val AS FLOAT64) + (interval_size * i) AS STRING);

        -- Adjust for the last split to ensure it covers the absolute max
        IF i = p_num_splits THEN
            SET current_max_val = max_col_val;
        END IF;

        SET splits = ARRAY_CONCAT(splits, [STRUCT(i AS split_id, current_min_val AS min_value, current_max_val AS max_value)]);
        SET current_min_val = current_max_val;
        SET i = i + 1;
    END WHILE;

    SET p_split_details = splits;
END;

-- Helper function for formatting BigQuery Management (BQM) query strings securely
CREATE OR REPLACE FUNCTION `your_gcp_project_id.your_bigquery_dataset.FORMAT_BQM_QUERY`(
    template_string STRING,
    arg1 STRING, arg2 STRING, arg3 STRING, arg4 STRING, arg5 STRING
) RETURNS STRING AS (
    REPLACE(
        REPLACE(
            REPLACE(
                REPLACE(
                    REPLACE(template_string, '%s1', arg1),
                '%s2', arg2),
            '%s3', arg3),
        '%s4', arg4),
    '%s5', arg5)
);

-- Example Usage:
-- DECLARE p_splits ARRAY<STRUCT<split_id INT, min_value STRING, max_value STRING>>;
-- CALL `your_gcp_project_id.your_bigquery_dataset.get_sql_splits_bq`('your_source_table', 'id_column', 4, p_splits);
-- SELECT * FROM UNNEST(p_splits);