-- BigQuery Standard SQL
-- Legacy Source: vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh
-- Job: vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh

CREATE OR REPLACE PROCEDURE `dataset.parser_getnode_bq`(
  IN node_name STRING,
  IN input_text STRING,
  OUT node_text STRING
)
BEGIN
  DECLARE open_tag STRING DEFAULT CONCAT('<', node_name, '>');
  DECLARE close_tag STRING DEFAULT CONCAT('</', node_name, '>');
  DECLARE start_pos INT64;
  DECLARE end_pos INT64;

  SET start_pos = STRPOS(input_text, open_tag);
  SET end_pos = STRPOS(input_text, close_tag);

  IF start_pos = 0 OR end_pos = 0 OR end_pos <= start_pos THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = CONCAT('parser_getnode_bq: node not found: ', node_name);
  END IF;

  SET node_text = SUBSTR(
    input_text,
    start_pos + LENGTH(open_tag),
    end_pos - (start_pos + LENGTH(open_tag))
  );
END;