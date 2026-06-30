CREATE OR REPLACE PROCEDURE `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.sp_set_message_additional_info`(
  IN p_EintragsNr INT64,
  IN p_Stichtag TIMESTAMP,
  IN p_ZusatzInfos STRING
)
BEGIN
  UPDATE `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.message_table`
  SET stichtag = COALESCE(p_Stichtag, stichtag),
      zusatzinfos = COALESCE(p_ZusatzInfos, zusatzinfos),
      updated_at = CURRENT_TIMESTAMP()
  WHERE eintragsnr = p_EintragsNr;
END;