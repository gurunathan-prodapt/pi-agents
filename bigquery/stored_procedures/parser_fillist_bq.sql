-- BigQuery Standard SQL
-- Legacy Source: vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh
-- Job: vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh

CREATE OR REPLACE PROCEDURE `dataset.parser_fillist_bq`(
  IN list_name STRING,
  IN list_values ARRAY<STRING>,
  INOUT template_text STRING
)
BEGIN
  DECLARE list_count INT64 DEFAULT IFNULL(ARRAY_LENGTH(list_values), 0);
  DECLARE joined_values STRING DEFAULT '';
  DECLARE middle_values ARRAY<STRING> DEFAULT [];
  DECLARE first_value STRING DEFAULT '';
  DECLARE end_value STRING DEFAULT '';
  DECLARE single_value STRING DEFAULT '';

  IF list_count = 0 THEN
    -- Remove empty list block if present
    SET template_text = REGEXP_REPLACE(
      template_text,
      r'(?is)<LIST[^>]*>\s*</LIST>',
      ''
    );
    RETURN;
  END IF;

  SET first_value = list_values[OFFSET(0)];
  SET end_value = list_values[OFFSET(list_count - 1)];

  IF list_count = 1 THEN
    SET single_value = first_value;

    SET template_text = REGEXP_REPLACE(
      template_text,
      r'(?is)<LIST[^>]*>',
      ''
    );
    SET template_text = REGEXP_REPLACE(
      template_text,
      r'(?is)</LIST>',
      ''
    );
    SET template_text = REPLACE(template_text, '<FIRST>', single_value);
    SET template_text = REPLACE(template_text, '<MIDDLE>', '');
    SET template_text = REPLACE(template_text, '<END>', single_value);
    SET template_text = REPLACE(template_text, '<SINGLE>', single_value);

  ELSE
    IF list_count > 2 THEN
      SET middle_values = ARRAY(
        SELECT v
        FROM UNNEST(list_values) AS v WITH OFFSET pos
        WHERE pos > 0 AND pos < list_count - 1
      );
      SET joined_values = ARRAY_TO_STRING(middle_values, '\n');
    ELSE
      SET joined_values = '';
    END IF;

    SET template_text = REGEXP_REPLACE(
      template_text,
      r'(?is)<LIST[^>]*>',
      ''
    );
    SET template_text = REGEXP_REPLACE(
      template_text,
      r'(?is)</LIST>',
      ''
    );
    SET template_text = REPLACE(template_text, '<FIRST>', first_value);
    SET template_text = REPLACE(template_text, '<MIDDLE>', joined_values);
    SET template_text = REPLACE(template_text, '<END>', end_value);
    SET template_text = REPLACE(template_text, '<SINGLE>', '');
  END IF;
END;