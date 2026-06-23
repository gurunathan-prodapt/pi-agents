-- BigQuery User-Defined Function: format_timestamp_with_offset
-- Legacy Source: vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh (timestamp logic)
-- Job: vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh
-- Description: Formats a timestamp with a given offset (e.g., 'CURRENT_TIMESTAMP', 'CURRENT_DATE - 5 DAY').

CREATE OR REPLACE FUNCTION `<PROJECT_ID>.<DATASET_ID>.format_timestamp_with_offset`(
    p_format STRING,
    p_base_timestamp TIMESTAMP,
    p_offset_expression STRING -- e.g., '5 DAY', '-2 HOUR'
)
RETURNS STRING
AS ((
    SELECT
        FORMAT_TIMESTAMP(p_format,
            CASE
                WHEN p_offset_expression IS NULL OR p_offset_expression = '' THEN p_base_timestamp
                ELSE
                    -- This is a simplified example. For full dynamic offset parsing,
                    -- more complex UDFs or a Python function would be needed.
                    -- This assumes p_offset_expression is in the format 'X UNIT' or '-X UNIT'
                    TIMESTAMP_ADD(p_base_timestamp, INTERVAL CAST(SPLIT(p_offset_expression, ' ')[OFFSET(0)] AS INT64) (CASE WHEN ENDS_WITH(p_offset_expression, 'DAY') THEN DAY WHEN ENDS_WITH(p_offset_expression, 'HOUR') THEN HOUR WHEN ENDS_WITH(p_offset_expression, 'MINUTE') THEN MINUTE ELSE DAY END))
            END
        )
));