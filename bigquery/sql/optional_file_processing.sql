-- Optional BigQuery SQL for commented-out file processing (sed, sort, join)
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_basis_his.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_basis_his.ksh

-- IMPORTANT: This file contains placeholder SQL for logic that was commented out
-- in the original KornShell script. A decision must be made whether to reactivate
-- this functionality. If reactivated, ensure the raw data files (.dat, .csv)
-- are ingested into BigQuery tables (e.g., `project.dataset.cibasis_data24_raw`).

-- Example SQL for `sed s/\\ //g` and `sort -u -k 1 -t ';'` operations
-- This assumes raw data is in tables like `cibasis_data24_raw` with a single STRING column.
-- Adapt column names and parsing based on actual file structure (e.g., CSV).

-- Create cleaned and sorted views/tables for cibasis_data24
CREATE OR REPLACE TABLE `project.dataset.cibasis_data24_processed` AS
SELECT
  TRIM(REPLACE(column_0, ' ', '')) AS processed_data,
  SPLIT(TRIM(REPLACE(column_0, ' ', '')), ';')[OFFSET(0)] AS key_column -- Assuming key is first part of semicolon-separated string
FROM
  `project.dataset.cibasis_data24_raw` -- Replace with actual source table
QUALIFY ROW_NUMBER() OVER (PARTITION BY SPLIT(TRIM(REPLACE(column_0, ' ', '')), ';')[OFFSET(0)] ORDER BY 1) = 1; -- Equivalent to -u for unique keys

-- Create cleaned and sorted views/tables for cibasis_data96
CREATE OR REPLACE TABLE `project.dataset.cibasis_data96_processed` AS
SELECT
  TRIM(REPLACE(column_0, ' ', '')) AS processed_data,
  SPLIT(TRIM(REPLACE(column_0, ' ', '')), ';')[OFFSET(0)] AS key_column
FROM
  `project.dataset.cibasis_data96_raw`
QUALIFY ROW_NUMBER() OVER (PARTITION BY SPLIT(TRIM(REPLACE(column_0, ' ', '')), ';')[OFFSET(0)] ORDER BY 1) = 1;

-- Create cleaned and sorted views/tables for cibasis_fax
CREATE OR REPLACE TABLE `project.dataset.cibasis_fax_processed` AS
SELECT
  TRIM(REPLACE(column_0, ' ', '')) AS processed_data,
  SPLIT(TRIM(REPLACE(column_0, ' ', '')), ';')[OFFSET(0)] AS key_column
FROM
  `project.dataset.cibasis_fax_raw`
QUALIFY ROW_NUMBER() OVER (PARTITION BY SPLIT(TRIM(REPLACE(column_0, ' ', '')), ';')[OFFSET(0)] ORDER BY 1) = 1;

-- Example for `join` operations
-- This would produce `cibasisprodukt.csv` output
CREATE OR REPLACE TABLE `project.dataset.cibasisprodukt` AS
SELECT
    COALESCE(d24.key_column, d96.key_column, cfax.key_column) AS join_key,
    d24.processed_data AS data24_info,
    d96.processed_data AS data96_info,
    cfax.processed_data AS fax_info
FROM
    `project.dataset.cibasis_data24_processed` d24
FULL OUTER JOIN
    `project.dataset.cibasis_data96_processed` d96
ON
    d24.key_column = d96.key_column
LEFT JOIN -- Assuming this join corresponds to the original `join -a 1` or specific requirements
    `project.dataset.cibasis_fax_processed` cfax
ON
    COALESCE(d24.key_column, d96.key_column) = cfax.key_column
;

-- Notes:
-- 1. The exact structure of `column_0` (e.g., if it's already structured CSV, or raw lines)
--    needs to be understood to correctly apply SPLIT and extraction logic.
-- 2. `REGEXP_REPLACE` was used in pseudocode; `REPLACE` is sufficient if only single spaces are targeted.
-- 3. The `join` operations and output columns (`-o 2.1,1.2,2.2`) require careful translation
--    based on the exact desired output format and column mapping from the source files.
-- 4. These are examples. The final implementation requires concrete definitions of the input file formats
--    and the exact output desired from the join operations.