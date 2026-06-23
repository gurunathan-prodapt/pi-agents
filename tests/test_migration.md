As a senior data-migration QA engineer, I have analyzed the provided migration design and the legacy KornShell script `k_ausd_bp_ta_bpr_beschr.ksh` along with its associated SQL script `d_ausd_bp_ta_bpr_beschr.sql`. The migration targets BigQuery Stored Procedures for orchestration and data processing, replacing Oracle tables with BigQuery tables.

The following test cases are designed to validate the behavioral equivalence of the migrated BigQuery solution to the legacy system, covering output parity, transformation correctness, external system replacements, and data quality assertions.

---

## Test Setup Prerequisites

Before running any tests, ensure the following BigQuery resources are created and accessible:

*   **Project and Datasets:**
    *   `your_gcp_project.dw_source.isrpt` (for source tables)
    *   `your_gcp_project.dw_target.isrpt` (for target tables)
    *   `your_gcp_project.your_audit_dataset` (for `job_table`)
*   **Tables:**
    *   `your_gcp_project.dw_source.isrpt.dwtk_meldungen`
        *   Schema: `job_kennung` STRING, `timecreated` TIMESTAMP
    *   `your_gcp_project.dw_source.isrpt.pds_ta_bpr`
        *   Schema: `bpr_id` STRING, `pds_description_id` STRING, `modified_at` TIMESTAMP, `is_production` INT64
    *   `your_gcp_project.dw_source.isrpt.pds_ta_care_description`
        *   Schema: `pds_description_id` STRING, `pds_description` STRING
    *   `your_gcp_project.dw_target.isrpt.sof_ta_bpr_beschr`
        *   Schema: `BPR_ID` STRING, `PDS_DESCRIPTION` STRING
    *   `your_gcp_project.your_audit_dataset.job_table`
        *   Schema: `tab_name` STRING, `status_a` STRING, `status_i` STRING, `stichtag_from` DATE, `stichtag_to` DATE, `job_type` STRING, `restart_flag` STRING, `record_count` INT64, `description` STRING, `insert_datetime` TIMESTAMP
*   **BigQuery Stored Procedures:**
    *   `your_gcp_project.your_dataset.k_ausd_bp_ta_bpr_beschr` (migrated orchestration script)
    *   `your_gcp_project.your_dataset.d_ausd_bp_ta_bpr_beschr` (migrated SQL script)

For all test cases, replace `your_gcp_project`, `your_dataset`, `your_audit_dataset`, `dw_source.isrpt`, and `dw_target.isrpt` with the actual BigQuery project and dataset names.

---

### Test Case 1: Successful Execution - Happy Path

**Purpose:** Verify that the migrated job executes successfully with valid parameters, processes data correctly, and logs the correct record count. This covers output parity, transformation correctness, and basic data quality.

**Setup:**
1.  Clear target and audit tables.
2.  Populate source tables with sample data that should be processed.

```sql
-- Clear target and audit tables
TRUNCATE TABLE `your_gcp_project.dw_target.isrpt.sof_ta_bpr_beschr`;
TRUNCATE TABLE `your_gcp_project.your_audit_dataset.job_table`;

-- Populate dw_source.isrpt.pds_ta_bpr
INSERT INTO `your_gcp_project.dw_source.isrpt.pds_ta_bpr` (bpr_id, pds_description_id, modified_at, is_production) VALUES
('BPR001', 'DESC001', NULL, 1),
('BPR002', 'DESC002', NULL, 1),
('BPR003', 'DESC001', '2023-01-01 10:00:00 UTC', 1), -- Should be filtered out (modified_at IS NOT NULL)
('BPR004', 'DESC003', NULL, 0),                   -- Should be filtered out (is_production = 0)
('BPR005', 'DESC004', NULL, 1),
('BPR006', 'DESC_NULL', NULL, 1);                 -- Will not join if DESC_NULL is not in pds_ta_care_description

-- Populate dw_source.isrpt.pds_ta_care_description
INSERT INTO `your_gcp_project.dw_source.isrpt.pds_ta_care_description` (pds_description_id, pds_description) VALUES
('DESC001', 'Description A'),
('DESC002', 'Description B'),
('DESC004', 'Description D');

-- Populate dw_source.isrpt.dwtk_meldungen (for v_datum_from_meldungen, though not directly used in INSERT)
INSERT INTO `your_gcp_project.dw_source.isrpt.dwtk_meldungen` (job_kennung, timecreated) VALUES
('BERT_DROP_TEMP_TABLE', '2023-03-15 12:00:00 UTC');
```

**Action:**
Execute the migrated orchestration stored procedure with valid parameters.

```sql
CALL `your_gcp_project.your_dataset.k_ausd_bp_ta_bpr_beschr`(
  'JOB_ABC',
  'ENTRY_123',
  '20010507', -- DDMMYYYY format
  '0'
);
```

**Pass/Fail Criterion:**
1.  The procedure completes without error.
2.  The `dw_target.isrpt.sof_ta_bpr_beschr` table contains the expected rows.
3.  The `your_gcp_project.your_audit_dataset.job_table` contains one entry with the correct metadata and record count.

```sql
-- Assertion 1: Check target table content
SELECT BPR_ID, PDS_DESCRIPTION FROM `your_gcp_project.dw_target.isrpt.sof_ta_bpr_beschr` ORDER BY BPR_ID;
-- Expected Result:
-- BPR_ID | PDS_DESCRIPTION
-- -------|-----------------
-- BPR001 | Description A
-- BPR002 | Description B
-- BPR005 | Description D

-- Assertion 2: Check job_table entry
SELECT
  tab_name,
  status_a,
  status_i,
  FORMAT_DATE('%Y-%m-%d', stichtag_from) AS stichtag_from,
  FORMAT_DATE('%Y-%m-%d', stichtag_to) AS stichtag_to,
  job_type,
  restart_flag,
  record_count,
  description
FROM `your_gcp_project.your_audit_dataset.job_table`;
-- Expected Result:
-- tab_name        | status_a | status_i | stichtag_from | stichtag_to | job_type | restart_flag | record_count | description
-- ----------------|----------|----------|---------------|-------------|----------|--------------|--------------|------------------
-- PoolBasisprodukt| A        | I        | 2001-05-07    | 2001-05-07  | J        | N            | 3            | Initialbefuellung
```

---

### Test Case 2: Parameter Validation - Missing Jobkennung

**Purpose:** Verify that the migrated job correctly identifies and raises an error for a missing `Jobkennung` parameter, mirroring the legacy script's `pruefeParameterGesetzt Jobkennung p_JobKennung` and `DWMSG_MeldeFehler` behavior.

**Setup:**
1.  Clear target and audit tables (optional, as no data modification is expected).
2.  Ensure source tables are populated (not strictly necessary for this error case, but good practice).

```sql
TRUNCATE TABLE `your_gcp_project.dw_target.isrpt.sof_ta_bpr_beschr`;
TRUNCATE TABLE `your_gcp_project.your_audit_dataset.job_table`;
-- Source tables can be empty or populated, it won't affect this test.
```

**Action:**
Execute the migrated orchestration stored procedure with `p_JobKennung` as `NULL` or an empty string.

```sql
-- Attempt 1: p_JobKennung is NULL
CALL `your_gcp_project.your_dataset.k_ausd_bp_ta_bpr_beschr`(
  NULL,
  'ENTRY_123',
  '20010507',
  '0'
);

-- Attempt 2: p_JobKennung is an empty string
-- CALL `your_gcp_project.your_dataset.k_ausd_bp_ta_bpr_beschr`(
--   '',
--   'ENTRY_123',
--   '20010507',
--   '0'
-- );
```

**Pass/Fail Criterion:**
1.  The procedure execution fails with an error message containing "Jobkennung fehlt".
2.  No data is inserted into `dw_target.isrpt.sof_ta_bpr_beschr`.
3.  No entry is created in `your_gcp_project.your_audit_dataset.job_table`.

```sql
-- Assertion 1: Check for error message (manual observation of BigQuery job logs or client output)
-- Expected: Error message similar to "Jobkennung fehlt"

-- Assertion 2: Check target table is empty
SELECT COUNT(*) FROM `your_gcp_project.dw_target.isrpt.sof_ta_bpr_beschr`;
-- Expected Result: 0

-- Assertion 3: Check job_table is empty
SELECT COUNT(*) FROM `your_gcp_project.your_audit_dataset.job_table`;
-- Expected Result: 0
```

---

### Test Case 3: Parameter Validation - Invalid Stichtag Format

**Purpose:** Verify that the migrated job correctly identifies and raises an error for an invalid `Stichtag` format, mirroring the legacy script's `DWDate_Datum_Check` behavior.

**Setup:**
1.  Clear target and audit tables.
2.  Ensure source tables are populated.

```sql
TRUNCATE TABLE `your_gcp_project.dw_target.isrpt.sof_ta_bpr_beschr`;
TRUNCATE TABLE `your_gcp_project.your_audit_dataset.job_table`;
-- Source tables can be empty or populated.
```

**Action:**
Execute the migrated orchestration stored procedure with an invalid `Stichtag` parameter (e.g., `YYYY-MM-DD` instead of `DDMMYYYY`).

```sql
CALL `your_gcp_project.your_dataset.k_ausd_bp_ta_bpr_beschr`(
  'JOB_ABC',
  'ENTRY_123',
  '2001-05-07', -- Invalid format
  '0'
);
```

**Pass/Fail Criterion:**
1.  The procedure execution fails with an error message indicating an invalid date format.
2.  No data is inserted into `dw_target.isrpt.sof_ta_bpr_beschr`.
3.  No entry is created in `your_gcp_project.your_audit_dataset.job_table`.

```sql
-- Assertion 1: Check for error message (manual observation of BigQuery job logs or client output)
-- Expected: Error message similar to "Ungueltiges Datum: 2001-05-07. Erwartetes Format: DDMMYYYY"

-- Assertion 2: Check target table is empty
SELECT COUNT(*) FROM `your_gcp_project.dw_target.isrpt.sof_ta_bpr_beschr`;
-- Expected Result: 0

-- Assertion 3: Check job_table is empty
SELECT COUNT(*) FROM `your_gcp_project.your_audit_dataset.job_table`;
-- Expected Result: 0
```

---

### Test Case 4: Transformation Correctness - Filtering Logic

**Purpose:** Verify that the `WHERE` clause (`bp.modified_at IS NULL AND bp.is_production = 1`) in the `d_ausd_bp_ta_bpr_beschr` logic is correctly applied, ensuring only relevant records are processed. This directly tests transformation correctness.

**Setup:**
1.  Clear target and audit tables.
2.  Populate source tables with data specifically designed to test the filter conditions.

```sql
-- Clear target and audit tables
TRUNCATE TABLE `your_gcp_project.dw_target.isrpt.sof_ta_bpr_beschr`;
TRUNCATE TABLE `your_gcp_project.your_audit_dataset.job_table`;

-- Populate dw_source.isrpt.pds_ta_bpr with various filter conditions
INSERT INTO `your_gcp_project.dw_source.isrpt.pds_ta_bpr` (bpr_id, pds_description_id, modified_at, is_production) VALUES
('BPR_OK_1', 'DESC_A', NULL, 1),             -- Should be included
('BPR_OK_2', 'DESC_B', NULL, 1),             -- Should be included
('BPR_MODIFIED', 'DESC_A', '2023-01-01 10:00:00 UTC', 1), -- Should be excluded (modified_at IS NOT NULL)
('BPR_NOT_PROD', 'DESC_B', NULL, 0),         -- Should be excluded (is_production = 0)
('BPR_BOTH_BAD', 'DESC_C', '2023-01-01 10:00:00 UTC', 0), -- Should be excluded
('BPR_NULL_DESC_ID', NULL, NULL, 1);         -- Should be excluded (JOIN condition fails)

-- Populate dw_source.isrpt.pds_ta_care_description
INSERT INTO `your_gcp_project.dw_source.isrpt.pds_ta_care_description` (pds_description_id, pds_description) VALUES
('DESC_A', 'Description for A'),
('DESC_B', 'Description for B'),
('DESC_C', 'Description for C');
```

**Action:**
Execute the migrated orchestration stored procedure.

```sql
CALL `your_gcp_project.your_dataset.k_ausd_bp_ta_bpr_beschr`(
  'JOB_FILTER',
  'ENTRY_FILTER',
  '20230301',
  '0'
);
```

**Pass/Fail Criterion:**
1.  The procedure completes without error.
2.  The `dw_target.isrpt.sof_ta_bpr_beschr` table contains only the records that satisfy `modified_at IS NULL AND is_production = 1` and successfully joined.
3.  The `record_count` in `job_table` reflects the number of successfully inserted rows.

```sql
-- Assertion 1: Check target table content
SELECT BPR_ID, PDS_DESCRIPTION FROM `your_gcp_project.dw_target.isrpt.sof_ta_bpr_beschr` ORDER BY BPR_ID;
-- Expected Result:
-- BPR_ID   | PDS_DESCRIPTION
-- ---------|-----------------
-- BPR_OK_1 | Description for A
-- BPR_OK_2 | Description for B

-- Assertion 2: Check job_table record_count
SELECT record_count FROM `your_gcp_project.your_audit_dataset.job_table` WHERE tab_name = 'PoolBasisprodukt';
-- Expected Result: 2
```

---

### Test Case 5: External System Replacement - Logging to `job_table`

**Purpose:** Verify that the record count and job metadata, which were originally handled by a temporary file (`tmpFile`) and a commented-out `FOSJobErzeugeEintrag` call, are correctly captured and inserted into the `project.dataset.job_table` in BigQuery. This validates the replacement of file system operations with BigQuery table inserts.

**Setup:**
1.  Clear target and audit tables.
2.  Populate source tables with data that will result in a known number of output rows.

```sql
-- Clear target and audit tables
TRUNCATE TABLE `your_gcp_project.dw_target.isrpt.sof_ta_bpr_beschr`;
TRUNCATE TABLE `your_gcp_project.your_audit_dataset.job_table`;

-- Populate dw_source.isrpt.pds_ta_bpr (3 records will pass filters)
INSERT INTO `your_gcp_project.dw_source.isrpt.pds_ta_bpr` (bpr_id, pds_description_id, modified_at, is_production) VALUES
('L_BPR001', 'L_DESC001', NULL, 1),
('L_BPR002', 'L_DESC002', NULL, 1),
('L_BPR003', 'L_DESC001', NULL, 1),
('L_BPR004', 'L_DESC003', '2023-01-01 10:00:00 UTC', 1); -- Filtered out

-- Populate dw_source.isrpt.pds_ta_care_description
INSERT INTO `your_gcp_project.dw_source.isrpt.pds_ta_care_description` (pds_description_id, pds_description) VALUES
('L_DESC001', 'Log Desc A'),
('L_DESC002', 'Log Desc B');
```

**Action:**
Execute the migrated orchestration stored procedure.

```sql
CALL `your_gcp_project.your_dataset.k_ausd_bp_ta_bpr_beschr`(
  'JOB_LOG',
  'ENTRY_LOG',
  '20230401',
  '1' -- Test with a non-default restart value
);
```

**Pass/Fail Criterion:**
1.  The procedure completes without error.
2.  Exactly one row is inserted into `your_gcp_project.your_audit_dataset.job_table`.
3.  The `record_count` in `job_table` matches the actual number of rows inserted into `dw_target.isrpt.sof_ta_bpr_beschr`.
4.  Other metadata fields (`tab_name`, `stichtag_from`, `restart_flag`, `description`) are correctly populated.

```sql
-- Assertion 1: Check job_table content
SELECT
  tab_name,
  status_a,
  status_i,
  FORMAT_DATE('%Y-%m-%d', stichtag_from) AS stichtag_from,
  FORMAT_DATE('%Y-%m-%d', stichtag_to) AS stichtag_to,
  job_type,
  restart_flag,
  record_count,
  description
FROM `your_gcp_project.your_audit_dataset.job_table`;
-- Expected Result:
-- tab_name        | status_a | status_i | stichtag_from | stichtag_to | job_type | restart_flag | record_count | description
-- ----------------|----------|----------|---------------|-------------|----------|--------------|--------------|------------------
-- PoolBasisprodukt| A        | I        | 2023-04-01    | 2023-04-01  | J        | Y            | 3            | Initialbefuellung

-- Assertion 2: Verify record_count against actual target table count
SELECT COUNT(*) FROM `your_gcp_project.dw_target.isrpt.sof_ta_bpr_beschr`;
-- Expected Result: 3 (should match record_count from job_table)
```

---

### Test Case 6: Data Quality - Empty Source Tables

**Purpose:** Verify that the job handles empty source tables gracefully, resulting in an empty target table and a `record_count` of 0 in the audit log. This tests edge cases and data quality.

**Setup:**
1.  Clear all source, target, and audit tables.

```sql
-- Clear all tables
TRUNCATE TABLE `your_gcp_project.dw_source.isrpt.pds_ta_bpr`;
TRUNCATE TABLE `your_gcp_project.dw_source.isrpt.pds_ta_care_description`;
TRUNCATE TABLE `your_gcp_project.dw_source.isrpt.dwtk_meldungen`;
TRUNCATE TABLE `your_gcp_project.dw_target.isrpt.sof_ta_bpr_beschr`;
TRUNCATE TABLE `your_gcp_project.your_audit_dataset.job_table`;
```

**Action:**
Execute the migrated orchestration stored procedure.

```sql
CALL `your_gcp_project.your_dataset.k_ausd_bp_ta_bpr_beschr`(
  'JOB_EMPTY',
  'ENTRY_EMPTY',
  '20230501',
  '0'
);
```

**Pass/Fail Criterion:**
1.  The procedure completes without error.
2.  The `dw_target.isrpt.sof_ta_bpr_beschr` table remains empty.
3.  The `your_gcp_project.your_audit_dataset.job_table` contains one entry with `record_count = 0`.

```sql
-- Assertion 1: Check target table is empty
SELECT COUNT(*) FROM `your_gcp_project.dw_target.isrpt.sof_ta_bpr_beschr`;
-- Expected Result: 0

-- Assertion 2: Check job_table record_count
SELECT record_count FROM `your_gcp_project.your_audit_dataset.job_table` WHERE tab_name = 'PoolBasisprodukt';
-- Expected Result: 0
```

---

### Test Case 7: Data Quality - No Matching Joins

**Purpose:** Verify that if source data exists but no records satisfy the `JOIN` condition (e.g., `pds_description_id` mismatch), the target table remains empty and the `record_count` is 0. This tests transformation correctness and data quality.

**Setup:**
1.  Clear target and audit tables.
2.  Populate source tables such that `pds_ta_bpr.pds_description_id` values do not match any `pds_ta_care_description.pds_description_id`.

```sql
-- Clear target and audit tables
TRUNCATE TABLE `your_gcp_project.dw_target.isrpt.sof_ta_bpr_beschr`;
TRUNCATE TABLE `your_gcp_project.your_audit_dataset.job_table`;

-- Populate dw_source.isrpt.pds_ta_bpr with valid filter conditions but no matching join keys
INSERT INTO `your_gcp_project.dw_source.isrpt.pds_ta_bpr` (bpr_id, pds_description_id, modified_at, is_production) VALUES
('BPR_NOJOIN_1', 'NON_EXISTENT_DESC_1', NULL, 1),
('BPR_NOJOIN_2', 'NON_EXISTENT_DESC_2', NULL, 1);

-- Populate dw_source.isrpt.pds_ta_care_description with different IDs
INSERT INTO `your_gcp_project.dw_source.isrpt.pds_ta_care_description` (pds_description_id, pds_description) VALUES
('UNIQUE_DESC_A', 'Unique Description A'),
('UNIQUE_DESC_B', 'Unique Description B');
```

**Action:**
Execute the migrated orchestration stored procedure.

```sql
CALL `your_gcp_project.your_dataset.k_ausd_bp_ta_bpr_beschr`(
  'JOB_NOJOIN',
  'ENTRY_NOJOIN',
  '20230601',
  '0'
);
```

**Pass/Fail Criterion:**
1.  The procedure completes without error.
2.  The `dw_target.isrpt.sof_ta_bpr_beschr` table remains empty.
3.  The `your_gcp_project.your_audit_dataset.job_table` contains one entry with `record_count = 0`.

```sql
-- Assertion 1: Check target table is empty
SELECT COUNT(*) FROM `your_gcp_project.dw_target.isrpt.sof_ta_bpr_beschr`;
-- Expected Result: 0

-- Assertion 2: Check job_table record_count
SELECT record_count FROM `your_gcp_project.your_audit_dataset.job_table` WHERE tab_name = 'PoolBasisprodukt';
-- Expected Result: 0
```

---

### Test Case 8: Schema and Data Type Handling

**Purpose:** Verify that the migrated job correctly handles data types and schema, ensuring that `BPR_ID` and `PDS_DESCRIPTION` are inserted into the target table with the expected types and values without truncation or conversion errors. This covers transformation correctness and schema assertions.

**Setup:**
1.  Clear target and audit tables.
2.  Populate source tables with data that includes various string lengths and characters to test type handling.

```sql
-- Clear target and audit tables
TRUNCATE TABLE `your_gcp_project.dw_target.isrpt.sof_ta_bpr_beschr`;
TRUNCATE TABLE `your_gcp_project.your_audit_dataset.job_table`;

-- Populate dw_source.isrpt.pds_ta_bpr
INSERT INTO `your_gcp_project.dw_source.isrpt.pds_ta_bpr` (bpr_id, pds_description_id, modified_at, is_production) VALUES
('BPR_SHORT', 'DESC_SHORT', NULL, 1),
('BPR_LONG_ID_12345678901234567890', 'DESC_LONG', NULL, 1),
('BPR_SPECIAL_CHARS_ÄÖÜß', 'DESC_SPECIAL', NULL, 1);

-- Populate dw_source.isrpt.pds_ta_care_description
INSERT INTO `your_gcp_project.dw_source.isrpt.pds_ta_care_description` (pds_description_id, pds_description) VALUES
('DESC_SHORT', 'Short Desc'),
('DESC_LONG', 'This is a very long description that should be fully preserved without any truncation or data loss during the migration process.'),
('DESC_SPECIAL', 'Beschreibung mit Sonderzeichen: ÄÖÜß');
```

**Action:**
Execute the migrated orchestration stored procedure.

```sql
CALL `your_gcp_project.your_dataset.k_ausd_bp_ta_bpr_beschr`(
  'JOB_SCHEMA',
  'ENTRY_SCHEMA',
  '20230701',
  '0'
);
```

**Pass/Fail Criterion:**
1.  The procedure completes without error.
2.  The `dw_target.isrpt.sof_ta_bpr_beschr` table contains all expected rows.
3.  The `BPR_ID` and `PDS_DESCRIPTION` values in the target table exactly match the source values, including length and special characters, confirming correct data type handling.

```sql
-- Assertion 1: Check target table content for exact value match
SELECT BPR_ID, PDS_DESCRIPTION FROM `your_gcp_project.dw_target.isrpt.sof_ta_bpr_beschr` ORDER BY BPR_ID;
-- Expected Result:
-- BPR_ID                           | PDS_DESCRIPTION
-- ---------------------------------|----------------------------------------------------------------------------------------------------
-- BPR_LONG_ID_12345678901234567890 | This is a very long description that should be fully preserved without any truncation or data loss during the migration process.
-- BPR_SHORT                        | Short Desc
-- BPR_SPECIAL_CHARS_ÄÖÜß           | Beschreibung mit Sonderzeichen: ÄÖÜß

-- Assertion 2: Check row count
SELECT COUNT(*) FROM `your_gcp_project.dw_target.isrpt.sof_ta_bpr_beschr`;
-- Expected Result: 3
```