-- BigQuery DDL for temporary table sof_ta_barrier
-- Replaces legacy Oracle table sof$ta_barrier for job DW.BERT_AUSD_V_TA_P_VERTRAG
-- Schema not provided in design document, using generic column as placeholder.
CREATE TABLE IF NOT EXISTS `project_id.dataset_id.sof_ta_barrier`
(
    -- Add actual column definitions based on source system schema
    placeholder_col STRING
)
;