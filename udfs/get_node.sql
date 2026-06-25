-- BigQuery SQL UDF to extract content within named XML-like nodes.
-- Replaces functionality from parser_getnode in vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh
-- This UDF assumes nodes are well-formed and non-nested for direct extraction.
-- Complex nesting or includes within nodes will be handled by the main stored procedure.

CREATE OR REPLACE FUNCTION project.dataset.get_node(
    input_text STRING,
    node_name STRING
)
RETURNS STRING
AS
((
    SELECT
        -- Use regex to extract content between <node_name> and </node_name>
        -- DOTALL flag for '.' to match newlines
        REGEXP_EXTRACT(input_text, CONCAT(r'(?s)<', node_name, r'>(.*?)</', node_name, r'>'))
));