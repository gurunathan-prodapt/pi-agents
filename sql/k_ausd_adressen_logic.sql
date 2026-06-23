-- Legacy Source: k_ausd_adressen.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_adressen.ksh

-- This SQL represents the migrated core logic of 'k_ausd_adressen.ksh'.
-- It is designed to be executed via an Airflow BigQueryOperator,
-- receiving templated parameters from the orchestrating DAG.

-- Parameters expected from Airflow:
--   job_kennung        (STRING) : E.g., 'BERT_P_ADRESSEN'
--   stichtag           (STRING) : Reference date in 'DDMMYYYY' format
--   entry_nr           (INT64)  : Unique job entry number
--   wiederanlaufwert   (INT64)  : Restart value, typically 0 or a positive integer
--   start_date         (STRING) : Derived start date in 'DDMMYYYY' format (from dwh_util.utils.get_zeitraum_dates)
--   end_date           (STRING) : Derived end date in 'DDMMYYYY' format (from dwh_util.utils.get_zeitraum_dates)


-- IMPORTANT: Replace 'your-gcp-project', 'your_source_dataset', 'your_target_dataset'
-- with your actual Google Cloud Project ID and BigQuery Dataset names.

-- Step 1: Optional - Handle 'wiederanlaufwert' for restart logic.
-- The original `k_ausd_adressen.ksh` script might include deletion logic
-- or specific processing based on `wiederanlaufwert`.
-- This is a placeholder; actual logic needs to be translated from the source.
/*
-- Example: If wiederanlaufwert > 0 implies a full refresh for the stichtag
-- or processing of specific entities (e.g., DWH_VERTRAG_ID).
-- This DELETE statement might be conditional or part of a MERGE statement.
DELETE FROM `your-gcp-project.your_target_dataset.dwh_target_addresses`
WHERE
    extraction_stichtag = PARSE_DATE('%d%m%Y', '{{ stichtag }}')
    -- AND ({{ wiederanlaufwert }} > 0 AND DWH_VERTRAG_ID IN (SELECT contract_id FROM `your-gcp-project.your_source_dataset.restart_contracts` WHERE restart_flag = true))
;
*/

-- Step 2: Main data extraction and transformation logic.
-- This part selects data from the source (e.g., CRS system tables, assumed to be in BigQuery)
-- and transforms it before inserting into the target address table.

INSERT INTO `your-gcp-project.your_target_dataset.dwh_target_addresses` (
    address_key,                  -- Unique identifier for the address record
    business_partner_id,          -- Identifier for the business partner
    invoice_recipient_id,         -- Identifier for the invoice recipient
    address_line_1,
    address_line_2,
    city,
    postal_code,
    country_code,
    valid_from_date,              -- Validity start date of the address
    valid_to_date,                -- Validity end date of the address
    effective_start_date,         -- Start date for which this record is effective in DWH
    effective_end_date,           -- End date for which this record is effective in DWH
    extraction_stichtag,          -- The reference date used for this extraction
    load_timestamp,               -- Timestamp when this record was loaded
    job_identifier,               -- Identifier of the job that processed this record
    job_entry_number              -- Unique entry number for the job run
)
SELECT
    -- Example transformations: Generate a hash key for address_key
    FARM_FINGERPRINT(
        CONCAT(
            IFNULL(src.address_id, ''),
            IFNULL(src.business_partner_id, ''),
            IFNULL(src.valid_from_date_str, '')
        )
    ) AS address_key,
    src.business_partner_id,
    src.invoice_recipient_id,
    src.street AS address_line_1,
    src.house_number AS address_line_2, -- Assuming house_number is part of address_line_2
    src.city,
    src.zip_code AS postal_code,
    src.country_code,
    PARSE_DATE('%Y%m%d', src.valid_from_date_str) AS valid_from_date,
    PARSE_DATE('%Y%m%d', src.valid_to_date_str) AS valid_to_date,
    -- Assuming effective dates are derived from stichtag or source validity
    PARSE_DATE('%d%m%Y', '{{ stichtag }}') AS effective_start_date,
    DATE('9999-12-31') AS effective_end_date, -- Default to open-ended
    PARSE_DATE('%d%m%Y', '{{ stichtag }}') AS extraction_stichtag,
    CURRENT_TIMESTAMP() AS load_timestamp,
    '{{ job_kennung }}' AS job_identifier,
    {{ entry_nr }} AS job_entry_number
FROM
    `your-gcp-project.your_source_dataset.crs_source_addresses` AS src
WHERE
    -- Filter data based on the stichtag.
    -- Assuming `valid_from_date_str` and `valid_to_date_str` are in 'YYYYMMDD' format in source.
    PARSE_DATE('%Y%m%d', src.valid_from_date_str) <= PARSE_DATE('%d%m%Y', '{{ stichtag }}')
    AND PARSE_DATE('%Y%m%d', src.valid_to_date_str) >= PARSE_DATE('%d%m%Y', '{{ stichtag }}')
    -- Additional filtering based on wiederanlaufwert if applicable in k_ausd_adressen.ksh.
    -- For instance, if wiederanlaufwert identifies specific DWH_VERTRAG_ID (contract IDs) to re-process.
    -- AND ({{ wiederanlaufwert }} = 0 OR src.dwh_vertrag_id IN (SELECT contract_id FROM `your-gcp-project.your_source_dataset.contract_reprocess_list` WHERE restart_id = {{ wiederanlaufwert }}))
;

-- Optional: Return a simple status or count of processed rows for logging/monitoring.
-- This SELECT statement will be executed by the BigQueryOperator and its result
-- can be captured in Airflow task logs or pushed to XComs if configured.
SELECT
    'k_ausd_adressen_bq_logic_executed_successfully' AS status,
    COUNT(1) AS processed_rows
FROM
    `your-gcp-project.your_target_dataset.dwh_target_addresses`
WHERE
    extraction_stichtag = PARSE_DATE('%d%m%Y', '{{ stichtag }}')
    AND job_entry_number = {{ entry_nr }};