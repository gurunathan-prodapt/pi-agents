-- DDL for attributes table
-- Legacy Source: vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh
-- Job: vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh
-- This table stores scalar attributes used for the parser_filattrib equivalent logic.

CREATE TABLE IF NOT EXISTS `<PROJECT_ID>.<DATASET_ID>.attributes` (
    attribute_name STRING OPTIONS(description="Name of the attribute (e.g., 'REPORT_DATE', 'SCHEMA_NAME')"),
    attribute_value STRING OPTIONS(description="The value of the attribute"),
    description STRING OPTIONS(description="Description of the attribute"),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);