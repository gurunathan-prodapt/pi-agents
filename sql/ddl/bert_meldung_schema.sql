-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh
-- Target Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh

-- This script defines the BigQuery DDL for the BERT_MELDUNG and BERT_MELDUNG_FEHLER tables.
-- Replace `my_project.my_dataset` with your actual BigQuery project and dataset IDs.

CREATE SCHEMA IF NOT EXISTS `my_project.my_dataset`;

CREATE TABLE IF NOT EXISTS `my_project.my_dataset.bert_meldung`
(
    eintrags_nr STRING NOT NULL OPTIONS(description="Unique entry number for the message/log entry"),
    job_kennung STRING OPTIONS(description="Identifier for the job"),
    programm_name STRING OPTIONS(description="Name of the program/script"),
    log_datei STRING OPTIONS(description="Path or name of the log file"),
    typ STRING OPTIONS(description="Type of message (e.g., 'INFO', 'WARNING', 'ERROR')"),
    status STRING OPTIONS(description="Status of the entry (e.g., 'OK', 'ABORTED', 'IN_PROGRESS', 'PENDING')"),
    zusatz_infos STRING OPTIONS(description="Additional information, potentially JSON or concatenated string"),
    creation_timestamp TIMESTAMP OPTIONS(description="Timestamp when the entry was created"),
    last_update_timestamp TIMESTAMP OPTIONS(description="Timestamp when the entry was last updated")
)
PARTITION BY DATE(creation_timestamp)
CLUSTER BY eintrags_nr;

CREATE TABLE IF NOT EXISTS `my_project.my_dataset.bert_meldung_fehler`
(
    error_id STRING NOT NULL OPTIONS(description="Unique identifier for the error entry"),
    eintrags_nr STRING NOT NULL OPTIONS(description="Foreign key to bert_meldung.eintrags_nr"),
    fehler_nr STRING OPTIONS(description="Error code or number"),
    fehler_text STRING OPTIONS(description="Detailed error message"),
    zusatz_info_1 STRING OPTIONS(description="Additional error detail 1"),
    zusatz_info_2 STRING OPTIONS(description="Additional error detail 2"),
    variable_name STRING OPTIONS(description="Name of the variable associated with the error"),
    error_timestamp TIMESTAMP OPTIONS(description="Timestamp when the error occurred")
)
PARTITION BY DATE(error_timestamp)
CLUSTER BY eintrags_nr;