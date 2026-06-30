CREATE TABLE IF NOT EXISTS `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.message_table` (
  eintragsnr INT64 NOT NULL,
  jobkennung STRING,
  programmname STRING,
  logdatei STRING,
  status STRING NOT NULL,
  stichtag TIMESTAMP,
  zusatzinfos STRING,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(created_at)
CLUSTER BY eintragsnr, status;