-- Helper function: replace_placeholder
-- Legacy Source: vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh
-- Job: vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh

CREATE OR REPLACE TEMP FUNCTION `project.dataset.replace_placeholder`(
    input_text STRING,
    placeholder STRING,
    replacement STRING
)
RETURNS STRING
AS
(
    REPLACE(input_text, placeholder, replacement)
);