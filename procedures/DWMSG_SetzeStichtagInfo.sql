CREATE OR REPLACE PROCEDURE `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.DWMSG_SetzeStichtagInfo`(
  IN p_EintragsNr INT64,
  IN p_Stichtag STRING,
  IN p_StichtagFmt STRING
)
BEGIN
  DECLARE v_ts TIMESTAMP;

  IF p_EintragsNr IS NULL THEN
    RAISE USING MESSAGE = 'Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben';
  END IF;

  IF p_Stichtag IS NULL OR p_Stichtag = '' THEN
    RAISE USING MESSAGE = 'Argh!, keinen Stichtag angegeben!';
  END IF;

  IF p_StichtagFmt IS NULL OR p_StichtagFmt = '' THEN
    RAISE USING MESSAGE = 'Argh!, Stichtagsangaben ohne Formatangaben können nicht verarbeitet werden!';
  END IF;

  SET v_ts = PARSE_TIMESTAMP(p_StichtagFmt, p_Stichtag);

  CALL `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.sp_set_message_additional_info`(
    p_EintragsNr,
    v_ts,
    NULL
  );
END;