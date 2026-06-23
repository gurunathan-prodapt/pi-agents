-- DDL for sof_ta_discount_rr, replacing Oracle SOF$TA_DISCOUNT_RR from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_discount_rr.ksh
CREATE TABLE IF NOT EXISTS `your_gcp_project.isrpt_isbert_stage.sof_ta_discount_rr` (
    cntrct_id STRING,
    discount_id STRING,
    disc_vector_ty STRING,
    cntrct_obj_version INT64,
    cntrct_template_id STRING,
    disc_invoice_item_id STRING,
    rabatt STRING,
    rabatthoehe FLOAT64,
    rabattierte_rech_pos STRING
)
PARTITION BY
    -- Assuming a date column would be used for partitioning.
    -- If no suitable date column exists, consider a different partitioning strategy
    -- or omit partitioning if the table is small.
    -- For now, let's assume `created_at` or a similar timestamp is available in the source.
    -- As it's not explicitly in the target columns, we'll omit partitioning for now
    -- to avoid making assumptions, but it's a best practice for large BigQuery tables.
    -- If data from Oracle SOF$TA_DISCOUNT_RR has a timestamp, use it.
    -- For this example, let's add a `processing_timestamp` for partitioning.
    DATE(processing_timestamp)
CLUSTER BY
    discount_id, cntrct_id;

-- Add a new column `processing_timestamp` for partitioning purposes.
ALTER TABLE `your_gcp_project.isrpt_isbert_stage.sof_ta_discount_rr`
ADD COLUMN IF NOT EXISTS processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP();