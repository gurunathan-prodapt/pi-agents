-- BigQuery SQL UDF to handle list expansion logic.
-- Replaces functionality from parser_fillist in vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh
-- This UDF iterates through elements and performs replacements for placeholders like {{LIST_NAME[i]}}.

CREATE OR REPLACE FUNCTION project.dataset.expand_list_block(
    block_text STRING,
    list_name STRING,
    elements ARRAY<STRING>
)
RETURNS STRING
AS
((
    SELECT
        IF(ARRAY_LENGTH(elements) = 0, block_text, -- No elements, return original block
            (
                SELECT
                    -- Iterate through elements and apply replacement
                    ARRAY_REDUCE(
                        GENERATE_ARRAY(0, ARRAY_LENGTH(elements) - 1),
                        block_text,
                        (accumulator, i) -> REGEXP_REPLACE(accumulator, CONCAT(r'\{\{', list_name, r'\[', CAST(i AS STRING), r'\]\}\}'), elements[i]),
                        (accumulator, i) -> accumulator
                    )
            )
        )
));