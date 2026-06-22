-- Target code for legacy job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.ksh
-- This BigQuery table stores metadata about the SQL scripts that starteSQLSkript will execute.

CREATE OR REPLACE TABLE `project.dataset.sql_script_registry` (
  script_name STRING NOT NULL OPTIONS(description="Name or path of the SQL script"),
  script_sql STRING NOT NULL OPTIONS(description="BigQuery-compatible SQL content of the script"),
  is_readable BOOL NOT NULL OPTIONS(description="Flag indicating if the script is ready for execution"),
  last_updated TIMESTAMP NOT NULL OPTIONS(description="Timestamp of the last update")
)
PARTITION BY
  DATE(last_updated)
CLUSTER BY
  script_name;