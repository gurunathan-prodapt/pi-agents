-- DDL for RKopfStan
-- Target table for data processed by d_aurd_rechstan.sql, orchestrated by vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_aurd_rechstan.ksh
-- NOTE: The original schema for RKopfStan was not provided. This is a placeholder schema.
CREATE TABLE IF NOT EXISTS `my_project.my_dataset.RKopfStan`
(
    rkopf_id STRING NOT NULL OPTIONS(description="Placeholder unique identifier for RKopfStan records"),
    stichtag_date DATE NOT NULL OPTIONS(description="Reference date for the record"),
    attribute_1 STRING OPTIONS(description="Placeholder attribute"),
    attribute_2 INT64 OPTIONS(description="Placeholder attribute"),
    creation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Record creation timestamp")
)
PARTITION BY
    stichtag_date
PRIMARY KEY (rkopf_id)
OPTIONS(
    description="Target table for the r_aurd_rechstan process. Placeholder schema as original not defined."
);