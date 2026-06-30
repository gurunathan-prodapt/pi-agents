CREATE OR REPLACE PROCEDURE `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.DWMSG_Fehlerbehandlung`(
  IN p_EintragsNr INT64,
  IN p_ErrorCode INT64
)
BEGIN
  DECLARE kUnerwFehler INT64 DEFAULT 10;

  IF p_EintragsNr IS NULL THEN
    RAISE USING MESSAGE = 'Argh!, keine EintragsNummer bei Aufruf von Fehlerbehandlung angegeben';
  END IF;

  CALL `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.DWMSG_MeldeFehler`(
    p_EintragsNr,
    'F',
    kUnerwFehler,
    CONCAT('ErrorCode ist: ', CAST(p_ErrorCode AS STRING)),
    NULL
  );

  CALL `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.DWMSG_SetzeStatusAbbruch`(p_EintragsNr);
END;