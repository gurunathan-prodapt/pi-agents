CREATE OR REPLACE PROCEDURE dwh_bert_dataset.k_ausd_bp_ta_msisdn_controller(
  p_JobKennung STRING,
  p_Stichtag STRING,
  p_EintragsNr STRING, -- Changed to STRING to match GENERATE_UUID() output
  p_wiederanlaufWert STRING
)
BEGIN
  -- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_msisdn.ksh
  -- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_msisdn.ksh

  -- Validate p_Stichtag format (DDMMYYYY)
  IF SAFE.PARSE_DATE('%d%m%Y', p_Stichtag) IS NULL THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = CONCAT('Invalid p_Stichtag format (', p_Stichtag, '). Expected DDMMYYYY.');
  END IF;

  -- Call the core transformation procedure
  CALL dwh_bert_dataset.d_ausd_bp_ta_msisdn_transform();

  -- No specific logging within the controller for success.
  -- Error handling is done via SIGNAL above, which will be caught by the wrapper's EXCEPTION block.

END;