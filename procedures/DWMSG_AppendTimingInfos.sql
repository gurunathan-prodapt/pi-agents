CREATE OR REPLACE PROCEDURE `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.DWMSG_AppendTimingInfos`(
  IN p_EintragsNr INT64,
  IN p_InfoText STRING,
  IN p_DateFormat STRING
)
BEGIN
  DECLARE v_info STRING;

  IF p_EintragsNr IS NULL THEN
    RAISE USING MESSAGE = 'Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben';
  END IF;

  IF p_DateFormat IS NULL OR p_DateFormat = '' THEN
    RAISE USING MESSAGE = 'Argh!, Formatangabe erforderlich!';
  END IF;

  SET v_info = CONCAT(
    COALESCE(p_InfoText, ''),
    ' ',
    FORMAT_TIMESTAMP(p_DateFormat, CURRENT_TIMESTAMP()),
    ' '
  );

  CALL `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.sp_set_message_additional_info`(
    p_EintragsNr,
    NULL,
    v_info
  );
END;