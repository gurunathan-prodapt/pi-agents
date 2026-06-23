-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh
CREATE TABLE IF NOT EXISTS data_operations_logs.message_table (
  eintrags_nr STRING,       -- Unique entry number
  job_kennung STRING,       -- Job identifier
  programmname STRING,      -- Program name
  logdatei STRING,          -- Log file path
  status STRING,            -- Status (e.g., 'OPEN', 'OK', 'ABBRUCH')
  fehler_typ STRING,        -- Error type (e.g., 'F', 'E', 'W')
  fehler_nr INT64,          -- Error code
  zusatz1 STRING,           -- Additional info field 1
  zusatz2 STRING,           -- Additional info field 2
  zusatzinfos STRING,       -- Generic additional info
  created_ts TIMESTAMP,     -- Record creation timestamp
  updated_ts TIMESTAMP      -- Record last updated timestamp
);