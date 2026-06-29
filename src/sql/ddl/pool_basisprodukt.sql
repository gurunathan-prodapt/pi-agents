-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_cntrct_dist.ksh
-- Target: BigQuery Tables for PoolBasisprodukt

CREATE TABLE IF NOT EXISTS `project.dataset.PoolBasisprodukt` (
  stichtag DATE,
  datum_heute DATE,
  datum_gestern DATE,
  job_kennung STRING,
  eintrags_nr STRING,
  contract_id STRING,
  distribution_channel STRING,
  account_balance NUMERIC,
  load_timestamp TIMESTAMP
)
PARTITION BY stichtag
CLUSTER BY contract_id, distribution_channel;

CREATE TABLE IF NOT EXISTS `project.dataset.PoolBasisprodukt_Staging` (
  stichtag DATE,
  contract_id STRING,
  distribution_channel STRING,
  account_balance NUMERIC
);