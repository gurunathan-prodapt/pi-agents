-- BigQuery DDL for creating the dwh_exporter dataset
-- Legacy source: vobs/dw_source/isdwh/exporter/is/bin/r_exis_v2
CREATE SCHEMA IF NOT EXISTS dwh_exporter
OPTIONS(
    location="US"
);