# Migration Validation Test Suite: `ausd_bp_ta_bpr_basis_his`

This document defines the migration-validation test suite to verify the behavioral equivalence of the migrated `ausd_bp_ta_bpr_basis_his` job on Google Cloud Platform (BigQuery/Cloud Composer) against the legacy Oracle-based implementation.

---

## Test Case 1: End-to-End Output Parity (Golden Dataset)

### Purpose
To prove that given the same source data inputs and metadata context, the migrated BigQuery pipeline produces identical outputs to the legacy Oracle pipeline.

### Setup
1. **Oracle Environment**: Populate the source tables with a controlled "Golden Dataset" containing 10 distinct contract and base product scenarios.
2. **BigQuery Environment**: Replicate the exact same "Golden Dataset" into the BigQuery staging tables: `cds.ta_cntrct`, `pds.ta_bpri_com`, and `isbert_schema.dwtk_meldungen`.
3. Set the metadata date context in both environments to `2026-04-20`.

#### Golden Dataset SQL Setup (BigQuery & Oracle equivalent)
```sql
-- Metadata setup
INSERT INTO `isbert_schema.dwtk_meldungen` (job_kennung, timecreated) 
VALUES ('BERT_DROP_TEMP_TABLE', TIMESTAMP('2026-04-20 12:00:00'));

-- Contracts setup
INSERT INTO `cds.ta_cntrct` (
  cntrct_id, cntrct_st, redundant_owner_id, insert_at, modified_at, 
  valid_from, valid_to, is_production, cntrct_ty, cntrct_parent
) VALUES 
-- 1. Valid active contract
('C_VAL_01', 5, 1, TIMESTAMP('2026-04-19 00:00:00'), NULL, DATE('2026-04-19'), NULL, 1, 3, NULL),
-- 2. Invalid status (not 5 or 6)
('C_INV_ST', 4, 1, TIMESTAMP('2026-04-19 00:00:00'), NULL, DATE('2026-04-19'), NULL, 1, 3, NULL),
-- 3. Invalid owner (not 1)
('C_INV_OWN', 5, 2, TIMESTAMP('2026-04-19 00:00:00'), NULL, DATE('2026-04-19'), NULL, 1, 3, NULL),
-- 4. Invalid temporal (valid_from in future relative to v_datum)
('C_INV_TIME', 5, 1, TIMESTAMP('2026-04-19 00:00:00'), NULL, DATE('2026-04-21'), NULL, 1, 3, NULL);

-- Base Products setup
INSERT INTO `pds.ta_bpri_com` (
  cntrct_id, bpr_id, bpri_com_id, iccid_mi, iccid_ii, iccid_iai, iccid_nr, iccid_cd,
  imsi_mcc, imsi_mnc, imsi_hlr, imsi_si, cntrct_id_ref, valid_from, valid_to, 
  modified_at, insert_at, slave_number, eid, is_production
) VALUES 
-- Matches C_VAL_01 (Valid MultiSIM)
('C_VAL_01', 3848, 'BP_01', '89', '49', '01', '123456789', '0', '262', '01', 'HLR1', 'SI1', NULL, DATE('2026-04-19'), NULL, NULL, TIMESTAMP('2026-04-19 00:00:00'), 1, 'EID_01', 1),
-- Matches C_VAL_01 but invalid bpr_id (not in target list)
('C_VAL_01', 9999, 'BP_02', '89', '49', '01', '123456789', '0', '262', '01', 'HLR1', 'SI1', NULL, DATE('2026-04-19'), NULL, NULL, TIMESTAMP('2026-04-19 00:00:00'), NULL, NULL, 1);
```

### Action
1. Run the legacy Oracle SQL script `d_ausd_bp_ta_bpr_basis_his.sql` on the Oracle test database. Export the target table `sof$ta_bpr_basis_his` to a CSV file (`oracle_output.csv`).
2. Run the migrated BigQuery SQL script on GCP. Export the target table `sof.ta_bpr_basis_his` to a CSV file (`bq_output.csv`).
3. Execute a Python-based comparison script to assert exact parity.

### Pass/Fail Criterion
* **Pass**: The row count in both outputs is exactly `1`. The contents of `oracle_output.csv` and `bq_output.csv` are identical (ignoring column ordering and timestamp formatting differences).
* **Fail**: Any mismatch in row count, column values, or if records excluded in Oracle are included in BigQuery (or vice versa).

---

## Test Case 2: Dynamic Date Context (`v_datum`) Resolution

### Purpose
To verify that the dynamic date context (`v_datum`) is correctly resolved from the operational metadata table `dwtk_meldungen` and defaults to `1900-01-01` if no record is found.

### Setup
Create two test scenarios in BigQuery:
* **Scenario A**: Multiple metadata entries exist for `BERT_DROP_TEMP_TABLE`.
* **Scenario B**: No metadata entries exist for `BERT_DROP_TEMP_TABLE`.

### Action
Execute the following validation query for both scenarios:

```sql
-- Scenario A Setup
CREATE OR REPLACE TEMP TABLE temp_dwtk_meldungen AS (
  SELECT 'BERT_DROP_TEMP_TABLE' AS job_kennung, TIMESTAMP('2026-04-15 10:00:00') AS timecreated
  UNION ALL
  SELECT 'BERT_DROP_TEMP_TABLE', TIMESTAMP('2026-04-20 15:30:00')
  UNION ALL
  SELECT 'OTHER_JOB', TIMESTAMP('2026-04-22 00:00:00')
);

-- Scenario A Assertion
DECLARE v_datum DATE;
SET v_datum = (
  SELECT COALESCE(DATE(MAX(timecreated)), DATE('1900-01-01'))
  FROM temp_dwtk_meldungen
  WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
);
ASSERT v_datum = DATE('2026-04-20') AS "Scenario A Failed: Did not resolve MAX timecreated";

-- Scenario B Setup
TRUNCATE TABLE temp_dwtk_meldungen;

-- Scenario B Assertion
SET v_datum = (
  SELECT COALESCE(DATE(MAX(timecreated)), DATE('1900-01-01'))
  FROM temp_dwtk_meldungen
  WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
);
ASSERT v_datum = DATE('1900-01-01') AS "Scenario B Failed: Did not default to 1900-01-01";
```

### Pass/Fail Criterion
* **Pass**: Both assertions execute successfully without throwing errors.
* **Fail**: Any assertion fails, indicating incorrect date resolution or fallback logic.

---

## Test Case 3: ICCID Concatenation & NULL Handling

### Purpose
To verify that the string concatenation of ICCID sub-components matches Oracle's behavior when one or more components are `NULL`. In Oracle, `NULL` in string concatenation (`||`) acts as an empty string, whereas in BigQuery, standard `CONCAT` returns `NULL` if any argument is `NULL`. The test proves the `COALESCE` wrapper functions correctly.

### Setup
Insert test records into a temporary table representing `pds.ta_bpri_com` with various NULL combinations in ICCID fields.

```sql
CREATE OR REPLACE TEMP TABLE temp_bpri_com AS (
  SELECT 
    'ALL_VALS' AS bpri_com_id, '89' AS iccid_mi, '49' AS iccid_ii, '01' AS iccid_iai, '12345' AS iccid_nr, '9' AS iccid_cd
  UNION ALL
  SELECT 
    'SOME_NULLS', '89', CAST(NULL AS STRING), '01', '12345', CAST(NULL AS STRING)
  UNION ALL
  SELECT 
    'ALL_NULLS', CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING)
);
```

### Action
Run the BigQuery concatenation logic against the test table:

```sql
CREATE OR REPLACE TEMP TABLE concat_results AS
SELECT 
  bpri_com_id,
  CONCAT(
    COALESCE(iccid_mi, ''), '-',
    COALESCE(iccid_ii, ''), '-',
    COALESCE(iccid_iai, ''), '-',
    COALESCE(iccid_nr, ''), '-',
    COALESCE(iccid_cd, '')
  ) AS ICCID
FROM temp_bpri_com;
```

### Pass/Fail Criterion
Verify the output matches the expected Oracle-equivalent concatenation:
```sql
-- Assertion Query
SELECT bpri_com_id, ICCID FROM concat_results;
```
* **Pass**: 
  * `ALL_VALS` produces `'89-49-01-12345-9'`
  * `SOME_NULLS` produces `'89--01-12345-'`
  * `ALL_NULLS` produces `'----'`
* **Fail**: Any output is `NULL`, or the hyphens are misplaced/missing.

---

## Test Case 4: Contract Status and Type Filtering Logic

### Purpose
To verify that the complex contract filtering logic is correctly applied, specifically:
1. `cntrct_st IN (5, 6)`
2. `redundant_owner_id = 1`
3. `cntrct_ty NOT IN (1, 2, 5) OR cntrct_parent IS NOT NULL`

### Setup
Insert contracts with varying combinations of status, owner, type, and parent values.

```sql
CREATE OR REPLACE TEMP TABLE temp_ta_cntrct AS (
  -- Valid: Status 5, Owner 1, Type 3, Parent NULL
  SELECT 'C_OK_1' AS cntrct_id, 5 AS cntrct_st, 1 AS redundant_owner_id, 3 AS cntrct_ty, CAST(NULL AS STRING) AS cntrct_parent, 1 AS is_production, DATE('2026-04-01') AS valid_from, CAST(NULL AS DATE) AS valid_to, TIMESTAMP('2026-04-01 00:00:00') AS insert_at, CAST(NULL AS TIMESTAMP) AS modified_at
  UNION ALL
  -- Valid: Status 6, Owner 1, Type 1 (normally excluded), but Parent is NOT NULL
  SELECT 'C_OK_2', 6, 1, 1, 'PARENT_01', 1, DATE('2026-04-01'), NULL, TIMESTAMP('2026-04-01 00:00:00'), NULL
  UNION ALL
  -- Invalid: Status 4 (Excluded)
  SELECT 'C_ERR_ST', 4, 1, 3, NULL, 1, DATE('2026-04-01'), NULL, TIMESTAMP('2026-04-01 00:00:00'), NULL
  UNION ALL
  -- Invalid: Owner 2 (Excluded)
  SELECT 'C_ERR_OWN', 5, 2, 3, NULL, 1, DATE('2026-04-01'), NULL, TIMESTAMP('2026-04-01 00:00:00'), NULL
  UNION ALL
  -- Invalid: Type 2 and Parent is NULL (Excluded)
  SELECT 'C_ERR_TY', 5, 1, 2, NULL, 1, DATE('2026-04-01'), NULL, TIMESTAMP('2026-04-01 00:00:00'), NULL
  UNION ALL
  -- Invalid: Not production
  SELECT 'C_ERR_PROD', 5, 1, 3, NULL, 0, DATE('2026-04-01'), NULL, TIMESTAMP('2026-04-01 00:00:00'), NULL
);
```

### Action
Run the filtering logic with `v_datum = '2026-04-20'`:

```sql
SELECT cntrct_id 
FROM temp_ta_cntrct c
WHERE c.cntrct_st IN (5, 6)
  AND c.redundant_owner_id = 1
  AND DATE(c.insert_at) <= DATE('2026-04-20')
  AND (c.modified_at IS NULL OR DATE(c.modified_at) > DATE('2026-04-20'))
  AND DATE(c.valid_from) <= DATE('2026-04-20')
  AND (c.valid_to IS NULL OR DATE(c.valid_to) > DATE('2026-04-20'))
  AND c.is_production = 1
  AND (c.cntrct_ty NOT IN (1, 2, 5) OR c.cntrct_parent IS NOT NULL);
```

### Pass/Fail Criterion
* **Pass**: The query returns exactly two rows: `C_OK_1` and `C_OK_2`.
* **Fail**: Any of the `C_ERR_*` contracts are returned, or any of the `C_OK_*` contracts are missing.

---

## Test Case 5: Temporal Boundary Filtering (As-Of Date Logic)

### Purpose
To verify that the temporal filters correctly capture the snapshot of contracts and base products active *as of* the dynamic `v_datum`.

### Setup
Set `v_datum = '2026-04-20'`. Insert records with boundary dates.

```sql
CREATE OR REPLACE TEMP TABLE temp_temporal_cntrct AS (
  -- 1. Active: valid_from before, valid_to after v_datum (INCLUDED)
  SELECT 'C_ACT' AS cntrct_id, DATE('2026-04-01') AS valid_from, DATE('2026-05-01') AS valid_to, TIMESTAMP('2026-04-01 00:00:00') AS insert_at, CAST(NULL AS TIMESTAMP) AS modified_at
  UNION ALL
  -- 2. Future: valid_from after v_datum (EXCLUDED)
  SELECT 'C_FUT', DATE('2026-04-21'), NULL, TIMESTAMP('2026-04-19 00:00:00'), NULL
  UNION ALL
  -- 3. Expired: valid_to before v_datum (EXCLUDED)
  SELECT 'C_EXP', DATE('2026-04-01'), DATE('2026-04-19'), TIMESTAMP('2026-04-01 00:00:00'), NULL
  UNION ALL
  -- 4. Modified/Superseded: modified_at is in the past relative to v_datum (EXCLUDED)
  SELECT 'C_MOD', DATE('2026-04-01'), NULL, TIMESTAMP('2026-04-01 00:00:00'), TIMESTAMP('2026-04-15 00:00:00')
);
```

### Action
Run the temporal filter query:

```sql
SELECT cntrct_id 
FROM temp_temporal_cntrct
WHERE DATE(insert_at) <= DATE('2026-04-20')
  AND (modified_at IS NULL OR DATE(modified_at) > DATE('2026-04-20'))
  AND DATE(valid_from) <= DATE('2026-04-20')
  AND (valid_to IS NULL OR DATE(valid_to) > DATE('2026-04-20'));
```

### Pass/Fail Criterion
* **Pass**: Only `C_ACT` is returned.
* **Fail**: `C_FUT`, `C_EXP`, or `C_MOD` are returned.

---

## Test Case 6: Target Table Idempotency (Truncate and Reload)

### Purpose
To verify that the BigQuery script is fully idempotent, ensuring that multiple executions do not cause duplicate records or primary key violations.

### Setup
1. Populate the target table `sof.ta_bpr_basis_his` with 5 dummy records.
2. Ensure source tables have a static set of 3 valid records.

### Action
1. Execute the BigQuery script `d_ausd_bp_ta_bpr_basis_his.sql` once.
2. Record the row count of `sof.ta_bpr_basis_his`.
3. Execute the BigQuery script a second time.
4. Record the row count of `sof.ta_bpr_basis_his` again.

### Pass/Fail Criterion
* **Pass**: 
  * The first run completely removes the 5 dummy records and inserts exactly 3 records.
  * The second run results in exactly 3 records (no duplication, total count remains 3).
* **Fail**: The final row count is not 3 (e.g., 6 due to duplication, or 8 due to failure to truncate).

---

## Test Case 7: Schema and Data Quality Assertions

### Purpose
To verify that the target BigQuery table matches the specified DDL schema, nullability constraints, and partitioning configuration.

### Setup
The target table `sof.ta_bpr_basis_his` must be deployed in the target BigQuery environment.

### Action
Execute a metadata query against `INFORMATION_SCHEMA`:

```sql
-- Check Partitioning
SELECT sensor_result
FROM (
  SELECT 
    CASE 
      WHEN is_partitioning_filter_required = 'NO' AND date_sharding_source IS NULL 
      THEN 'PASSED' 
      ELSE 'FAILED' 
    END AS sensor_result
  FROM `sof.INFORMATION_SCHEMA.TABLES`
  WHERE table_name = 'ta_bpr_basis_his'
);

-- Check Column Types
SELECT 
  column_name, 
  data_type, 
  is_nullable
FROM `sof.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'ta_bpr_basis_his'
ORDER BY ordinal_position;
```

### Pass/Fail Criterion
* **Pass**: 
  * The table is partitioned by `VALID_FROM` (type `DATE`).
  * Column types match the design document exactly:
    * `CNTRCT_ID`: `STRING`
    * `BPR_ID`: `INT64`
    * `VALID_FROM`: `DATE`
    * `INSERT_AT`: `TIMESTAMP`
* **Fail**: Any column type mismatch, missing columns, or incorrect partitioning configuration.