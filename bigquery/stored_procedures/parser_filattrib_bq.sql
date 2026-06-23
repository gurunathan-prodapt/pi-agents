-- BigQuery Standard SQL
-- Legacy Source: vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh
-- Job: vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh

CREATE OR REPLACE PROCEDURE `dataset.parser_filattrib_bq`(
  IN placeholder STRING,
  IN value STRING,
  INOUT text_in STRING
)
BEGIN
  DECLARE token STRING DEFAULT CONCAT('<', placeholder, '>');
  SET text_in = REPLACE(text_in, token, IFNULL(value, ''));
END;