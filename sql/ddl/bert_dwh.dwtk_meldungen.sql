-- Header: BigQuery DDL for dwtk_meldungen table
-- Legacy Source: isbert_schema.dwtk_meldungen
-- Job: BERT_V_TA_DISC_ZUSGF

CREATE TABLE IF NOT EXISTS `bert_dwh.dwtk_meldungen` (
    -- Assuming a minimal schema based on usage (MAX(m.timecreated))
    -- Additional columns can be added as needed based on full source schema.
    timecreated TIMESTAMP,
    -- Placeholder for other columns from original table
    original_data JSON
);