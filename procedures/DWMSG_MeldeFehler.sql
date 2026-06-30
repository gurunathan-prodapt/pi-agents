CREATE OR REPLACE PROCEDURE `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.DWMSG_MeldeFehler`(
  IN p_EintragsNr INT64,
  IN p_Typ STRING,
  IN p_FehlerNr INT64,
  IN p_Zusatz1 STRING,
  IN p_Zusatz2 STRING
)
BEGIN
  IF p_EintragsNr IS NULL THEN
    RAISE USING MESSAGE = 'Argh!, keine EintragsNummer bei Aufruf von MeldeFehler angegeben';
  END IF;

  CALL `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.sp_insert_message_error`(
    p_EintragsNr,
    p_Typ,
    p_FehlerNr,
    p_Zusatz1,
    p_Zusatz2
  );
END;