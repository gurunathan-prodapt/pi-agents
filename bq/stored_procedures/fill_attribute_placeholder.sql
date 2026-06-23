-- BigQuery Stored Procedure: fill_attribute_placeholder
-- Legacy Source: vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh (parser_filattrib function)
-- Job: vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh
-- Description: Replaces a specific placeholder with a given value in a text block.

CREATE OR REPLACE PROCEDURE `<PROJECT_ID>.<DATASET_ID>.fill_attribute_placeholder`(
    INOUT io_template STRING,
    IN p_placeholder STRING,
    IN p_value STRING
)
BEGIN
    SET io_template = REPLACE(io_template, p_placeholder, p_value);
END;