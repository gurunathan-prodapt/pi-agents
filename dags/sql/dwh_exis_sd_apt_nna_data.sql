-- Legacy Source: vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/RELEASEWECHSEL/RELEASEWECHSEL172/INSTALL_NACH_172_APT_DWHM/DW.DWH_APT_EXPORT_MONATLICH_JP/DW.DWH_EXIS_SD_APT_NNA_DATA.xml
-- Job: DW.DWH_APT_EXPORT_MONATLICH_JP
-- Purpose: BigQuery SQL for preparing DW.DWH_EXIS_SD_APT_NNA_DATA export.

-- This SQL script replicates the data extraction and transformation logic
-- previously handled by the 'r_exis_v2' binary and its associated '.var' configuration files.
-- The specific logic needs to be reverse-engineered and implemented here based on the original data sources
-- and required CSV output format.

-- Parameters:
--   export_month_yyyymm: The month for which to export data, in YYYYMM format (e.g., '202301').
--                        This parameter is passed from the Airflow DAG.

-- TODO: Replace the placeholder query below with the actual BigQuery SQL logic
-- to select, filter, transform, and format the data for the NNA_DATA export.
-- Ensure that the output columns and their order match the expected CSV schema.
SELECT
    CAST(CURRENT_DATE() AS STRING) AS export_date,
    @export_month_yyyymm AS export_month_identifier,
    'DATA_RECORD_TYPE' AS record_type,
    -- Add all actual columns required for the DW.DWH_EXIS_SD_APT_NNA_DATA export.
    -- Example placeholder columns:
    t.column_a,
    t.column_b,
    t.column_c
FROM
    `{{ GCP_PROJECT_ID }}.{{ BIGQUERY_DATASET }}.your_source_table_for_nna_data` AS t -- TODO: Replace with your actual source table
WHERE
    FORMAT_DATE('%Y%m', t.transaction_date_column) = @export_month_yyyymm -- TODO: Adjust date column and filtering logic
    AND t.status = 'ACTIVE' -- Example filter
    -- Add any additional filtering, joining, or transformation logic as per original specification
;