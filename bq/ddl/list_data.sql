-- DDL for list_data table
-- Legacy Source: vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh
-- Job: vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh
-- This table stores list elements used for the parser_fillist equivalent logic.

CREATE TABLE IF NOT EXISTS `<PROJECT_ID>.<DATASET_ID>.list_data` (
    list_name STRING OPTIONS(description="Name of the list (e.g., 'table_list', 'column_list')"),
    element_order INT64 OPTIONS(description="Order of the element within the list"),
    element_value STRING OPTIONS(description="The value of the list element"),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);