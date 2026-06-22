-- DDL for project.isbert_schema.dwtk_meldungen
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_optionen.ksh
-- This DDL is a best-effort inference from the design document.
-- Adjust data types and sizes based on actual source system schema.

CREATE TABLE `your-gcp-project-id.your-isbert-schema-dataset.dwtk_meldungen`
(
    job_kennung STRING NOT NULL OPTIONS(description="Identifier for the job, e.g., 'BERT_DROP_TEMP_TABLE'"),
    timecreated TIMESTAMP OPTIONS(description="Timestamp when the record was created")
    -- Add other columns from the source table as needed.
)
OPTIONS(
    description="Table to store DWTK messages, used for date derivation in the legacy system."
);