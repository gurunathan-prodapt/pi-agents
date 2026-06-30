CREATE OR REPLACE PROCEDURE `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.DWMSG_SetzeStatusOK`(
  IN p_EintragsNr INT64
)
BEGIN
  IF p_EintragsNr IS NULL THEN
    RAISE USING MESSAGE = 'Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben';
  END IF;

  CALL `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.sp_update_message_status`(p_EintragsNr, 'OK');
END;