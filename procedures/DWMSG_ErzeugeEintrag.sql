CREATE OR REPLACE PROCEDURE `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.DWMSG_ErzeugeEintrag`(
  IN p_EintragsNr INT64,
  IN p_JobKennung STRING,
  IN p_Programmname STRING,
  IN p_LogDatei STRING
)
BEGIN
  IF p_EintragsNr IS NULL THEN
    RAISE USING MESSAGE = 'Argh!, keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben';
  END IF;

  INSERT INTO `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.message_table` (
    eintragsnr, jobkennung, programmname, logdatei, status, created_at, updated_at
  )
  VALUES (
    p_EintragsNr, p_JobKennung, p_Programmname, p_LogDatei, 'OPEN',
    CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
  );
END;