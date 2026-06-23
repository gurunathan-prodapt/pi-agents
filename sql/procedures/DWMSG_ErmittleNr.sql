-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount_rr.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount_rr.ksh

CREATE OR REPLACE PROCEDURE `my_gcp_project.my_bq_dataset.DWMSG_ErmittleNr`(
  OUT p_dw_eintrags_nr STRING
)
BEGIN
  -- This procedure mimics the legacy DWMSG_ErmittleNr by generating a unique job ID.
  -- In BigQuery, we can use a combination of timestamp and a random number for uniqueness,
  -- or rely on a sequence if one were available (not natively in BQ).
  -- For simplicity, we'll use a formatted timestamp.
  SET p_dw_eintrags_nr = FORMAT_TIMESTAMP('%Y%m%d%H%M%S', CURRENT_TIMESTAMP());
END;