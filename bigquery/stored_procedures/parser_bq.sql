-- BigQuery Standard SQL
-- Legacy Source: vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh
-- Job: vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh

CREATE OR REPLACE PROCEDURE `dataset.parser_bq`(
  IN input_text STRING,
  OUT output_text STRING
)
BEGIN
  DECLARE work_text STRING DEFAULT input_text;
  DECLARE repeat_flag BOOL DEFAULT TRUE;
  DECLARE iteration INT64 DEFAULT 0;
  DECLARE max_iterations INT64 DEFAULT 1000;

  -- Optional timestamp substitution example
  SET work_text = REPLACE(
    work_text,
    '<TIMESTAMP>',
    FORMAT_TIMESTAMP('%Y%m%d%H%M%S', CURRENT_TIMESTAMP())
  );

  -- Main iterative substitution loop
  WHILE repeat_flag AND iteration < max_iterations DO
    SET iteration = iteration + 1;
    SET repeat_flag = FALSE;

    -- Example: resolve a simple attribute placeholder if present
    -- In a real migration, placeholder/value pairs should come from a staging table
    -- or be passed as structured input. This is a safe no-op scaffold.
    -- For now, this just demonstrates the loop structure.
    -- The actual logic for calling parser_filattrib_bq and parser_fillist_bq will go here,
    -- reading from staging tables.

    -- This simplified example just checks if any placeholder pattern remains to keep looping
    IF REGEXP_CONTAINS(work_text, r'<[A-Z_]+>') THEN
      SET repeat_flag = TRUE; -- Keep looping if placeholders are found
      -- Add logic here to fetch attributes/lists from staging and call sub-procedures
      -- For example:
      -- CALL `dataset.parser_filattrib_bq`('SOME_ATTRIBUTE', 'some_value', work_text);
      -- CALL `dataset.parser_fillist_bq`('SOME_LIST', ['val1', 'val2'], work_text);
    END IF;

    -- Example: remove unresolved INCLUDE directives only if preprocessed externally
    -- This assumes external preprocessing has resolved actual includes.
    -- Any remaining INCLUDE tags would be a parsing error or unresolvable.
    SET work_text = REGEXP_REPLACE(
      work_text,
      r'(?is)<INCLUDE[^>]*>.*?</INCLUDE>',
      ''
    );
  END WHILE;

  IF iteration >= max_iterations THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'parser_bq: maximum iterations reached';
  END IF;

  SET output_text = work_text;
END;