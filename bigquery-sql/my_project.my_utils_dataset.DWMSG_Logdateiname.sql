-- BigQuery UDF for DWMSG_Logdateiname
-- Utility for constructing a log filename, replacing logic from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vertrag_tmp.ksh
CREATE OR REPLACE FUNCTION `my_project.my_utils_dataset.DWMSG_Logdateiname`(
  IN p_job_name STRING,
  IN p_run_timestamp TIMESTAMP
) RETURNS STRING AS (
  FORMAT_FMT('%s_%s.log', p_job_name, FORMAT_TIMESTAMP('%Y%m%d%H%M%S', p_run_timestamp))
);