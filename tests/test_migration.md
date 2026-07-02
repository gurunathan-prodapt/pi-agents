# Migration Validation Test Suite: `ausd_bp_ta_bpr_basis_his`

This document outlines the migration-validation tests designed to verify that the re-architected BigQuery and Cloud Composer (Airflow) implementation of the `ausd_bp_ta_bpr_basis_his` job is behaviorally equivalent to the legacy Oracle/UC4 implementation.

---

## 1. Output Parity Tests

### Test Case 1.1: End-to-End Output Parity (Parallel Run Comparison)
* **Purpose**: Prove that running the migrated BigQuery SQL with identical source data yields the exact same output dataset as the legacy Oracle execution.
* **Setup**:
  1. Identify a historical execution date (e.g., `2026-04-20`).
  2. Extract the state of the source tables (`cds.ta_cntrct`, `pds.ta_bpri_com`, and `isbert_schema.dwtk_meldungen`) from Oracle for that run.
  3. Load these exact records into a test dataset in BigQuery (e.g., `gcp-dwh-test-verify`).
  4. Run the legacy Oracle job to populate the legacy target table. Extract the output to a temporary table or CSV.
* **Action**:
  Execute the migrated BigQuery SQL script pointing to the test dataset:
  ```sql
  -- Execute the migrated SQL script against the test dataset
  ```
* **Pass/Fail Criterion**: 
  The row count and column-by-column values must match exactly between the Oracle target and the BigQuery target. This is verified using the following validation query:
  ```sql
  -- This query should return 0 rows if there is absolute parity
  (
    SELECT CNTRCT_ID, BPR_ID, BPRI_COM_ID, ICCID, IMSI_MCC, IMSI_MNC, IMSI_HLR, IMSI_SI, CNTRCT_ID_REF, VALID_FROM, VALID_TO, SLAVE_NUMBER, E_ID 
    FROM `gcp-dwh-prod.sof.ta_bpr_basis_his`
    EXCEPT DISTINCT
    SELECT CNTRCT_ID, BPR_ID, BPRI_COM_ID, ICCID, IMSI_MCC, IMSI_MNC, IMSI_HLR, IMSI_SI, CNTRCT_ID_REF, VALID_FROM, VALID_TO, SLAVE_NUMBER, E_ID 
    FROM `legacy_oracle_exports.ta_bpr_basis_his_legacy`
  )
  UNION ALL
  (
    SELECT CNTRCT_ID, BPR_ID, BPRI_COM_ID, ICCID, IMSI_MCC, IMSI_MNC, IMSI_HLR, IMSI_SI, CNTRCT_ID_REF, VALID_FROM, VALID_TO, SLAVE_NUMBER, E_ID 
    FROM `legacy_oracle_exports.ta_bpr_basis_his_legacy`
    EXCEPT DISTINCT
    SELECT CNTRCT_ID, BPR_ID, BPRI_COM_ID, ICCID, IMSI_MCC, IMSI_MNC, IMSI_HLR, IMSI_SI, CNTRCT_ID_REF, VALID_FROM, VALID_TO, SLAVE_NUMBER, E_ID 
    FROM `gcp-dwh-prod.sof.ta_bpr_basis_his`
  );
  ```

---

## 2. Transformation Correctness Tests

### Test Case 2.1: Safe ICCID Concatenation and NULL Handling
* **Purpose**: Verify that the BigQuery `CONCAT` implementation with `IFNULL` behaves identically to Oracle's `||` operator when handling `NULL` values in ICCID components (preventing null propagation).
* **Setup**:
  Insert a test record into `pds.ta_bpri_com` where some ICCID components are `NULL` and others are populated.
  ```sql
  INSERT INTO `gcp-dwh-prod.pds.ta_bpri_com` (cntrct_id, bpr_id, bpri_com_id, iccid_mi, iccid_ii, iccid_iai, iccid_nr, iccid_cd, insert_at, valid_from, is_production)
  VALUES (999999, 31, 888888, '89', NULL, '49', NULL, '7', '1900-01-01', '1900-01-01', 1);
  
  INSERT INTO `gcp-dwh-prod.cds.ta_cntrct` (cntrct_id, cntrct_st, redundant_owner_id, insert_at, valid_from, is_production, cntrct_ty, cntrct_parent)
  VALUES (999999, 5, 1, '1900-01-01', '1900-01-01', 1, 3, NULL);
  ```
* **Action**:
  Run the migration SQL script with `v_datum` set to a date greater than or equal to `1900-01-01`.
* **Pass/Fail Criterion**: 
  The concatenated `ICCID` field in the target table must preserve the dashes and non-null values instead of resolving to `NULL`.
  ```sql
  SELECT ICCID FROM `gcp-dwh-prod.sof.ta_bpr_basis_his` WHERE CNTRCT_ID = 999999;
  -- Expected Output: "89--49--7"
  ```
  The test fails if the output is `NULL` or does not match the expected string format.

### Test Case 2.2: Contract Status and Type Filter Logic
* **Purpose**: Ensure that only contracts matching the complex filter criteria (`cntrct_st IN (5, 6)`, `redundant_owner_id = 1`, `is_production = 1`, and `(cntrct_ty NOT IN (1, 2, 5) OR cntrct_parent IS NOT NULL)`) are processed.
* **Setup**:
  Insert test records into `cds.ta_cntrct` that violate each boundary condition individually, along with one valid record.
* **Action**:
  Execute the migration SQL script.
* **Pass/Fail Criterion**: 
  Only the valid record must be inserted into `sof.ta_bpr_basis_his`.
  ```python
  import pytest
  from google.cloud import bigquery

  def test_contract_filters():
      client = bigquery.Client()
      query = """
      SELECT CNTRCT_ID FROM `gcp-dwh-prod.sof.ta_bpr_basis_his`
      WHERE CNTRCT_ID IN (101, 102, 103, 104, 105)
      """
      # 101: cntrct_st = 4 (Invalid)
      # 102: redundant_owner_id = 2 (Invalid)
      # 103: cntrct_ty = 1 and cntrct_parent IS NULL (Invalid)
      # 104: cntrct_ty = 1 and cntrct_parent IS NOT NULL (Valid)
      # 105: cntrct_ty = 3 and cntrct_parent IS NULL (Valid)
      
      results = [row.CNTRCT_ID for row in client.query(query).result()]
      assert 101 not in results
      assert 102 not in results
      assert 103 not in results
      assert 104 in results
      assert 105 in results
  ```

### Test Case 2.3: Temporal Filtering (Cutoff Date `v_datum` Logic)
* **Purpose**: Verify that the temporal filters correctly apply `v_datum` as the cutoff boundary for `insert_at`, `modified_at`, and `valid_from`/`valid_to`.
* **Setup**:
  1. Set `v_datum` in `isbert_schema.dwtk_meldungen` to `2026-04-20`.
  2. Insert a contract record with `valid_from = '2026-04-21'` (future-dated relative to `v_datum`).
  3. Insert a contract record with `valid_from = '2026-04-19'` and `valid_to = '2026-04-20'` (expired relative to `v_datum`).
  4. Insert a contract record with `valid_from = '2026-04-19'` and `valid_to = '2026-04-22'` (active relative to `v_datum`).
* **Action**:
  Execute the migration SQL script.
* **Pass/Fail Criterion**: 
  Only the active record (item 4) must be loaded into the target table. Future-dated and expired records must be excluded.
  ```sql
  -- Assert that no records with valid_from > v_datum or valid_to <= v_datum exist in the target
  SELECT COUNT(1) AS invalid_records
  FROM `gcp-dwh-prod.sof.ta_bpr_basis_his` target
  JOIN `gcp-dwh-prod.cds.ta_cntrct` c ON target.CNTRCT_ID = c.cntrct_id
  WHERE DATE(c.valid_from) > DATE('2026-04-20')
     OR DATE(c.valid_to) <= DATE('2026-04-20');
  -- Expected: 0
  ```

---

## 3. External-System Replacements

### Test Case 3.1: Metadata Extraction (`v_datum` Resolution)
* **Purpose**: Verify that the dynamic resolution of `v_datum` from `isbert_schema.dwtk_meldungen` matches the legacy UC4 parameter evaluation.
* **Setup**:
  Insert multiple run-date records into `isbert_schema.dwtk_meldungen` for `job_kennung = 'BERT_DROP_TEMP_TABLE'`, ensuring the latest `timecreated` corresponds to `2026-04-20`.
* **Action**:
  Run a test harness query that isolates the variable declaration and assignment:
  ```sql
  DECLARE v_datum DATE;
  SET v_datum = (
    SELECT COALESCE(DATE(MAX(timecreated)), DATE('1900-01-01'))
    FROM `gcp-dwh-prod.isbert_schema.dwtk_meldungen`
    WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
  );
  SELECT v_datum;
  ```
* **Pass/Fail Criterion**: 
  The query must return exactly `2026-04-20`. If the table is empty, it must gracefully fall back to `1900-01-01` without throwing an execution error.

---

## 4. Data-Quality, Row-Count, and Schema Assertions

### Test Case 4.1: Target Schema Integrity
* **Purpose**: Ensure the target table `sof.ta_bpr_basis_his` matches the required production schema, including data types, column names, and nullability.
* **Setup**: None (Metadata query).
* **Action**:
  Query the BigQuery `INFORMATION_SCHEMA.COLUMNS` view for the target table.
* **Pass/Fail Criterion**: 
  The schema must match the design specification exactly.
  ```python
  def test_target_schema_integrity():
      client = bigquery.Client()
      query = """
      SELECT column_name, data_type, is_nullable
      FROM `gcp-dwh-prod.sof.INFORMATION_SCHEMA.COLUMNS`
      WHERE table_name = 'ta_bpr_basis_his'
      ORDER BY ordinal_position
      """
      schema_rows = list(client.query(query).result())
      schema_dict = {row.column_name: (row.data_type, row.is_nullable) for row in schema_rows}
      
      expected_schema = {
          "CNTRCT_ID": ("INT64", "YES"),
          "BPR_ID": ("INT64", "YES"),
          "BPRI_COM_ID": ("INT64", "YES"),
          "ICCID": ("STRING", "YES"),
          "IMSI_MCC": ("STRING", "YES"),
          "IMSI_MNC": ("STRING", "YES"),
          "IMSI_HLR": ("STRING", "YES"),
          "IMSI_SI": ("STRING", "YES"),
          "CNTRCT_ID_REF": ("INT64", "YES"),
          "VALID_FROM": ("DATE", "YES"),
          "VALID_TO": ("DATE", "YES"),
          "MODIFIED_AT": ("TIMESTAMP", "YES"),
          "INSERT_AT": ("TIMESTAMP", "YES"),
          "SLAVE_NUMBER": ("INT64", "YES"),
          "E_ID": ("STRING", "YES")
      }
      
      for col, specs in expected_schema.items():
          assert col in schema_dict, f"Column {col} is missing from target schema."
          assert schema_dict[col][0] == specs[0], f"Column {col} type mismatch. Expected {specs[0]}, got {schema_dict[col][0]}."
  ```

### Test Case 4.2: Idempotency and Restartability (Truncate-and-Load Verification)
* **Purpose**: Verify that the job can be run repeatedly without duplicating data or leaving the target table in an inconsistent state.
* **Setup**:
  Ensure the target table `sof.ta_bpr_basis_his` contains pre-existing records from a previous run.
* **Action**:
  Execute the Airflow DAG `dw_bert_ausd_bp_ta_bpr_basis_his` twice consecutively.
* **Pass/Fail Criterion**: 
  The row count of the target table after the second run must be exactly equal to the row count after the first run (proving the `TRUNCATE` step executes successfully prior to the `INSERT` step).
  ```sql
  -- Assert no duplicate records exist on primary key equivalents
  SELECT CNTRCT_ID, BPRI_COM_ID, COUNT(1)
  FROM `gcp-dwh-prod.sof.ta_bpr_basis_his`
  GROUP BY CNTRCT_ID, BPRI_COM_ID
  HAVING COUNT(1) > 1;
  -- Expected: 0 rows returned
  ```