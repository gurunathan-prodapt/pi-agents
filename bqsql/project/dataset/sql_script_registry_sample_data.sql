-- Target code for legacy job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.ksh
-- Sample data insertion for the `project.dataset.sql_script_registry` table.
-- Replace with actual migrated SQL*Plus script content.

INSERT INTO `project.dataset.sql_script_registry` (
  script_name,
  script_sql,
  is_readable,
  last_updated
)
VALUES
  ('path/to/first_migrated_script.sql', 'SELECT "This is the content of the first migrated SQL script. Parameters can be passed via EXECUTE IMMEDIATE variables if needed." AS message;', TRUE, CURRENT_TIMESTAMP()),
  ('path/to/second_migrated_script.sql', '''
  -- Example of a more complex script content, potentially calling other procedures or querying tables
  DECLARE var_param STRING DEFAULT "default_value"; -- If parameters were passed from p_Parameter
  BEGIN
    -- For demonstration, assuming some table 'my_table' exists in 'my_dataset'
    -- SELECT COUNT(1) FROM `project.dataset.my_table` WHERE some_column = var_param;
    SELECT "Executed second script with parameter: ", var_param AS debug_info;
  END;
  ''', TRUE, CURRENT_TIMESTAMP()),
  ('path/to/yet_another_script.sql', 'SELECT CURRENT_DATE() AS today, CURRENT_TIME() AS now;', FALSE, CURRENT_TIMESTAMP());