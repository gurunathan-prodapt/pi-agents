-- Legacy Source: vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/RELEASEWECHSEL/RELEASEWECHSEL172/INSTALL_NACH_172_APT_DWHM/DW.DWH_APT_EXPORT_MONATLICH_JP/DW.DWH_EXIS_SD_APT_NNA_VOIC.xml
-- Job: DW.DWH_APT_EXPORT_MONATLICH_JP
-- Purpose: BigQuery SQL for preparing DW.DWH_EXIS_SD_APT_NNA_VOIC export.

-- This SQL script replicates the data extraction and transformation logic
-- previously handled by the 'r_exis_v2' binary and its associated '.var' configuration files.
-- The specific logic needs to be reverse-engineered and implemented here based on the original data sources
-- and required CSV output format.

-- Parameters:
--   export_month_yyyymm: The month for which to export data, in YYYYMM format (e.g., '202301').
--                        This parameter is passed from the Airflow DAG.

-- TODO: Replace the placeholder query below with the actual BigQuery SQL logic
-- to select, filter, transform, and format the data for the NNA_VOIC export.
-- Ensure that the output columns and their order match the expected CSV schema.
SELECT
    CAST(CURRENT_DATE() AS STRING) AS export_date,
    @export_month_yyyymm AS export_month_identifier,
    'VOIC_RECORD_TYPE' AS record_type,
    -- Add all actual columns required for the DW.DWH_EXIS_SD_APT_NNA_VOIC export.
    -- Example placeholder columns:
    v.voice_id,
    v.caller_number,
    v.callee_number,
    v.duration_seconds
FROM
    `{{ GCP_PROJECT_ID }}.{{ BIGQUERY_DATASET }}.your_source_table_for_nna_voic` AS v -- TODO: Replace with your actual source table
WHERE
    FORMAT_DATE('%Y%m', v.call_start_date_column) = @export_month_yyyymm -- TODO: Adjust date column and filtering logic
    AND v.call_type = 'OUTBOUND' -- Example filter
    -- Add any additional filtering, joining, or transformation logic as per original specification
;