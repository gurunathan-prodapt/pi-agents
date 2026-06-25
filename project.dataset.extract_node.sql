-- Helper function: extract_node
-- Legacy Source: vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh
-- Job: vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh

CREATE OR REPLACE TEMP FUNCTION `project.dataset.extract_node`(
    input_text STRING,
    node_name STRING
)
RETURNS STRING
AS
((
    SELECT
        STRING_AGG(line, '\n' ORDER BY line_num)
    FROM
        UNNEST(SPLIT(input_text, '\n')) AS line WITH OFFSET AS line_num
    WHERE
        line_num BETWEEN
            (SELECT MIN(line_num) FROM UNNEST(SPLIT(input_text, '\n')) AS line WITH OFFSET AS line_num WHERE STARTS_WITH(TRIM(line), CONCAT('<', node_name, '>'))) + 1
            AND
            (SELECT MAX(line_num) FROM UNNEST(SPLIT(input_text, '\n')) AS line WITH OFFSET AS line_num WHERE STARTS_WITH(TRIM(line), CONCAT('</', node_name, '>'))) - 1
));