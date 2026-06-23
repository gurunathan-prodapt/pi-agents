-- BigQuery Stored Procedure for post-processing cibasis data
-- Replaces commented-out sed, sort, join operations in k_ausd_bp_ta_bpr_basis_his.ksh
-- Original job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_basis_his.ksh
--
-- This procedure assumes the following external tables (or staging tables) exist:
--   - `isbert_dataset.cibasis_data24_ext` (representing cibasis_data24.dat)
--   - `isbert_dataset.cibasis_data96_ext` (representing cibasis_data96.dat)
--   - `isbert_dataset.cibasis_fax_ext` (representing cibasis_fax.dat)
-- Each external table is assumed to have a single `line` column for the raw data.
-- The delimiter is assumed to be semicolon (`;`) based on `sort -t ';'`.

CREATE OR REPLACE PROCEDURE `your-gcp-project.isbert_dataset.post_process_cibasis_data`(
  IN p_process_date DATE
)
BEGIN
  -- Step 1: Simulate `sed s/\\ //g` (remove spaces) and initial load
  -- Create staging tables after removing spaces and parsing
  CREATE OR REPLACE TEMPORARY TABLE `tmp_cibasis_data24_sed` AS
  SELECT
    TRIM(REPLACE(line, ' ', '')) AS processed_line,
    SPLIT(TRIM(REPLACE(line, ' ', '')), ';')[OFFSET(0)] AS key_column -- Assuming key is first column
  FROM `your-gcp-project.isbert_dataset.cibasis_data24_ext`
  WHERE line IS NOT NULL AND line != '';

  CREATE OR REPLACE TEMPORARY TABLE `tmp_cibasis_data96_sed` AS
  SELECT
    TRIM(REPLACE(line, ' ', '')) AS processed_line,
    SPLIT(TRIM(REPLACE(line, ' ', '')), ';')[OFFSET(0)] AS key_column
  FROM `your-gcp-project.isbert_dataset.cibasis_data96_ext`
  WHERE line IS NOT NULL AND line != '';

  CREATE OR REPLACE TEMPORARY TABLE `tmp_cibasis_fax_sed` AS
  SELECT
    TRIM(REPLACE(line, ' ', '')) AS processed_line,
    SPLIT(TRIM(REPLACE(line, ' ', '')), ';')[OFFSET(0)] AS key_column
  FROM `your-gcp-project.isbert_dataset.cibasis_fax_ext`
  WHERE line IS NOT NULL AND line != '';

  -- Step 2: Simulate `sort -u -k 1 -t ';'` (unique sort by first column)
  -- Use ROW_NUMBER to get unique rows based on key_column
  CREATE OR REPLACE TEMPORARY TABLE `tmp_cibasis_data24_sorted` AS
  SELECT
    processed_line,
    key_column
  FROM (
    SELECT
      processed_line,
      key_column,
      ROW_NUMBER() OVER (PARTITION BY key_column ORDER BY processed_line) as rn
    FROM `tmp_cibasis_data24_sed`
  )
  WHERE rn = 1;

  CREATE OR REPLACE TEMPORARY TABLE `tmp_cibasis_data96_sorted` AS
  SELECT
    processed_line,
    key_column
  FROM (
    SELECT
      processed_line,
      key_column,
      ROW_NUMBER() OVER (PARTITION BY key_column ORDER BY processed_line) as rn
    FROM `tmp_cibasis_data96_sed`
  )
  WHERE rn = 1;

  CREATE OR REPLACE TEMPORARY TABLE `tmp_cibasis_fax_sorted` AS
  SELECT
    processed_line,
    key_column
  FROM (
    SELECT
      processed_line,
      key_column,
      ROW_NUMBER() OVER (PARTITION BY key_column ORDER BY processed_line) as rn
    FROM `tmp_cibasis_fax_sed`
  )
  WHERE rn = 1;

  -- Step 3: Simulate `join` operations
  -- `join -j1 1 -j2 1 -o 2.1,1.2,2.2 -a 2 -t ';'` with cibasis_data24 and cibasis_data96
  -- This join is complex with `-o` and `-a` options.
  -- `-o 2.1,1.2,2.2` means (field 1 from file 2, field 2 from file 1, field 2 from file 2)
  -- `-a 2` means print unpairable lines from file 2. This implies a RIGHT JOIN (or FULL OUTER if `-a 1` was also present).
  -- Given the output format, we need to extract specific fields.
  -- Assuming processed_line contains semicolon-separated fields.
  -- Let's extract fields dynamically for clarity, assuming enough fields exist.

  CREATE OR REPLACE TEMPORARY TABLE `tmp_cibasis_24_96` AS
  SELECT
    -- From `cibasis_data96` (file 2 in join)
    SPLIT(t96.processed_line, ';')[OFFSET(0)] AS f2_f1_key, -- 2.1
    -- From `cibasis_data24` (file 1 in join)
    IF(t24.processed_line IS NOT NULL, SPLIT(t24.processed_line, ';')[OFFSET(1)], NULL) AS f1_f2, -- 1.2
    -- From `cibasis_data96` (file 2 in join)
    IF(t96.processed_line IS NOT NULL, SPLIT(t96.processed_line, ';')[OFFSET(1)], NULL) AS f2_f2, -- 2.2
    t96.processed_line AS raw_line_96 -- For debugging/completeness
  FROM `tmp_cibasis_data24_sorted` t24
  RIGHT JOIN `tmp_cibasis_data96_sorted` t96
    ON t24.key_column = t96.key_column;

  -- Second join: `join -j1 1 -j2 1 -o 1.1,1.2,1.3,2.2 -a 1 -t ';'` with tmp_cibasis_24_96 and cibasis_fax
  -- Assuming the previous output `tmp_cibasis_24_96` acts as file 1, and `cibasis_fax` acts as file 2.
  -- `-o 1.1,1.2,1.3,2.2` means (field 1 from file 1, field 2 from file 1, field 3 from file 1, field 2 from file 2)
  -- `-a 1` means print unpairable lines from file 1. This implies a LEFT JOIN (or FULL OUTER if `-a 2` was also present).

  -- Assuming `tmp_cibasis_24_96` fields are now f2_f1_key, f1_f2, f2_f2
  -- The original join output `2.1,1.2,2.2` are string fields.
  -- We need to reconstruct the "fields" from the combined string.
  -- This part is highly dependent on the exact format of 'processed_line'
  -- and how many fields each original file has.
  -- For this example, let's assume `tmp_cibasis_24_96` has its own 'key_column' or that f2_f1_key acts as one.

  CREATE OR REPLACE TABLE `your-gcp-project.isbert_dataset.cibasisprodukt_csv` AS
  SELECT
    t2496.f2_f1_key, -- 1.1 (key from first join output)
    t2496.f1_f2,     -- 1.2
    t2496.f2_f2,     -- 1.3
    IF(tfax.processed_line IS NOT NULL, SPLIT(tfax.processed_line, ';')[OFFSET(1)], NULL) AS f2_f2_from_fax -- 2.2
  FROM `tmp_cibasis_24_96` t2496
  LEFT JOIN `tmp_cibasis_fax_sorted` tfax
    ON t2496.f2_f1_key = tfax.key_column;

  -- Optionally, you can log completion or row counts.
  SELECT 'Post-processing complete' AS status;

END;