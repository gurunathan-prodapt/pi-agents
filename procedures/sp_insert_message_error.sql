CREATE OR REPLACE PROCEDURE `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.sp_insert_message_error`(
  IN p_EintragsNr INT64,
  IN p_Typ STRING,
  IN p_FehlerNr INT64,
  IN p_Zusatz1 STRING,
  IN p_Zusatz2 STRING
)
BEGIN
  INSERT INTO `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.message_errors` (
    eintragsnr, typ, fehlernr, zusatz1, zusatz2, created_at
  )
  VALUES (
    p_EintragsNr, p_Typ, p_FehlerNr, p_Zusatz1, p_Zusatz2, CURRENT_TIMESTAMP()
  );
END;