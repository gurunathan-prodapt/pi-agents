CREATE OR REPLACE PROCEDURE `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.DWMSG_Logdateiname`(
  IN p_JobKennung STRING,
  IN p_EintragsNr INT64,
  OUT p_Dateiname STRING
)
BEGIN
  SET p_Dateiname = CONCAT(
    'gs://${GCP_PROJECT_ID}-prot/',
    p_JobKennung,
    '_',
    FORMAT_TIMESTAMP('%Y%m%d_%H%M', CURRENT_TIMESTAMP()),
    '_',
    CAST(p_EintragsNr AS STRING),
    '.log'
  );
END;