--
-- BigQuery DDL for the ta_disc_zusgf table.
-- Replaces legacy table definition related to job BERT_V_TA_DISC_ZUSGF.
--
-- IMPORTANT: This schema is a placeholder. The actual schema must be
-- derived from a detailed analysis of the source system's `ta_disc_zusgf`
-- table definition and usage.
--

CREATE TABLE IF NOT EXISTS `<PROJECT_ID>.<DATASET_ID>.ta_disc_zusgf`
(
    -- Placeholder columns. Replace with actual schema from source.
    id                  STRING OPTIONS(description="Unique identifier"),
    description         STRING OPTIONS(description="Description of the discount or reconciliation item"),
    amount              NUMERIC OPTIONS(description="Amount associated with the item"),
    currency_code       STRING OPTIONS(description="Currency of the amount (e.g., 'EUR', 'USD')"),
    transaction_date    DATE OPTIONS(description="Date of the transaction or reconciliation"),
    status              STRING OPTIONS(description="Status of the item (e.g., 'ACTIVE', 'RECONCILED')"),
    load_timestamp      TIMESTAMP OPTIONS(description="Timestamp when the record was loaded into BigQuery")
)
PARTITION BY transaction_date
CLUSTER BY status, currency_code
OPTIONS(
    description="This table stores reconciled discount data, replacing the legacy ta_disc_zusgf table.",
    labels=[('source_system', 'isbert'), ('job_name', 'bert_v_ta_disc_zusgf')]
);