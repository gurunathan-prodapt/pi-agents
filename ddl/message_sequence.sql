-- BigQuery DDL for message_sequence
-- Replaces Oracle sequence functionality used by vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh
-- This table is used to generate unique 'eintragsnr' values.

CREATE TABLE IF NOT EXISTS dw_is_error_management.message_sequence (
    sequence_name STRING NOT NULL OPTIONS(description="Name of the sequence, e.g., 'eintragsnr_seq'"),
    next_val INT64 NOT NULL OPTIONS(description="Next available sequence value")
)
OPTIONS(
    description="Table to simulate Oracle sequences for generating unique IDs."
);

-- Initialize the sequence if it doesn't exist
MERGE INTO dw_is_error_management.message_sequence T
USING (SELECT 'eintragsnr_seq' AS sequence_name, 1 AS initial_val) S
ON T.sequence_name = S.sequence_name
WHEN NOT MATCHED THEN
    INSERT (sequence_name, next_val) VALUES (S.sequence_name, S.initial_val);