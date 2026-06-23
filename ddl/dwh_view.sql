-- BigQuery DDL for dwh_view dataset
-- Replaces parts of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_rechempf.ksh

CREATE SCHEMA IF NOT EXISTS `dwh_view`;

CREATE TABLE IF NOT EXISTS `dwh_view.vi_s_ibasisprodukt`
(
  vertrags_id INT64,
  vpn_id INT64,
  basisprodukt_id INT64
);