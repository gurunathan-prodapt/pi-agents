-- Helper function: expand_list_template
-- Legacy Source: vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh
-- Job: vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh

CREATE OR REPLACE TEMP FUNCTION `project.dataset.expand_list_template`(
    input_text STRING,
    list_values ARRAY<STRING>
)
RETURNS STRING
AS
((
    SELECT
        CASE
            WHEN ARRAY_LENGTH(list_values) = 1 THEN `project.dataset.replace_placeholder`(input_text, '<SINGLE>', list_values[OFFSET(0)])
            WHEN ARRAY_LENGTH(list_values) > 1 THEN
                `project.dataset.replace_placeholder`(
                    `project.dataset.replace_placeholder`(
                        `project.dataset.replace_placeholder`(
                            input_text,
                            '<FIRST>',
                            list_values[OFFSET(0)]
                        ),
                        '<END>',
                        list_values[OFFSET(ARRAY_LENGTH(list_values)-1)]
                    ),
                    '<MIDDLE>',
                    COALESCE((SELECT STRING_AGG(val, ', ') FROM UNNEST(list_values) AS val WITH OFFSET AS idx WHERE idx > 0 AND idx < ARRAY_LENGTH(list_values)-1), '')
                )
            ELSE input_text -- No list values, no replacements
        END
));