-- Legacy Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh
-- Description: Creates the BigQuery dataset for date utility functions.
-- This dataset will house UDFs and Stored Procedures migrated from the legacy KornShell script.

CREATE SCHEMA IF NOT EXISTS dw_utils
OPTIONS(
    description = "Dataset for migrated date utility functions from h_alis_date.ksh"
);