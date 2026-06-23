-- BigQuery UDF for DWMSG_ErmittleNr
-- Utility for generating a number, replacing logic from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vertrag_tmp.ksh
CREATE OR REPLACE FUNCTION `my_project.my_utils_dataset.DWMSG_ErmittleNr`() RETURNS INT64 AS (
  CAST(FORMAT_TIMESTAMP('%Y%m%d%H%M%S%f', CURRENT_TIMESTAMP()) AS INT64)
);