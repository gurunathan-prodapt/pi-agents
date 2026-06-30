Here is a comprehensive suite of migration-validation tests for the `ausd_bp_ta_bpr_instance` job. These tests are designed to prove behavioral equivalence between the legacy Oracle system and the migrated Google Cloud Platform (BigQuery/Airflow) implementation.

---

# Migration Validation Test Suite: `ausd_bp_ta_bpr_instance`

## Section 1: Output Parity Tests

### Test Case 1.1: End-to-End Dual-Run Parity (Oracle vs. BigQuery)
* **Purpose**: Prove that running the migrated BigQuery SQL script on a replicated snapshot of legacy source data produces identical row counts, keys, and column values as the legacy Oracle execution.
* **Setup**:
  1. Identify a static historical business date (e.g., `2025-01-15`).
  2. Extract the source tables from Oracle (`cds$ta_cntrct`, `pds$ta_bpri_com`, `dwtk_meldungen`) for that date and load them into a isolated BigQuery test dataset (`test_cds`, `test_pds`, `test_isbert`).
  3. Run the legacy Oracle job to populate the legacy target table `sof$ta_bpr_instance`. Extract this target table to a BigQuery table named `test_oracle_results.ta_bpr_instance`.
* **Action**:
  Execute the migrated BigQuery SQL script targeting a clean table `test_bq_results.ta_bpr_instance` using the same source data.
* **Pass/Fail Criterion**:
  The test passes if a full outer join between the Oracle-produced target and the BigQuery-produced target yields zero mismatched rows.
* **Validation Code (SQL)**:
  ```sql
  -- Assert absolute parity between Oracle legacy output and BigQuery migrated output
  WITH bq_target AS (
    SELECT 
      CNTRCT_ID, BPR_ID, BPR_INSTANCE_ID, ICCID, 
      IMSI_MCC, IMSI_MNC, IMSI_HLR, IMSI_SI, CNTRCT_ID_REF
    FROM `your-project-id.test_bq_results.ta_bpr_instance`
  ),
  oracle_target AS (
    SELECT 
      CNTRCT_ID, BPR_ID, BPR_INSTANCE_ID, ICCID, 
      IMSI_MCC, IMSI_MNC, IMSI_HLR, IMSI_SI, CNTRCT_ID_REF
    FROM `your-project-id.test_oracle_results.ta_bpr_instance`
  ),
  mismatches AS (
    SELECT 
      COALESCE(a.CNTRCT_ID, b.CNTRCT_ID) AS CNTRCT_ID,
      'BQ_ONLY' AS mismatch_type
    FROM bq_target a 
    LEFT JOIN oracle_target b ON a.CNTRCT_ID = b.CNTRCT_ID AND a.BPR_INSTANCE_ID = b.BPR_INSTANCE_ID
    WHERE b.CNTRCT_ID IS NULL

    UNION ALL

    SELECT 
      COALESCE(a.CNTRCT_ID, b.CNTRCT_ID) AS CNTRCT_ID,
      'ORACLE_ONLY' AS mismatch_type
    FROM oracle_target a 
    LEFT JOIN bq_target b ON a.CNTRCT_ID = b.CNTRCT_ID AND a.BPR_INSTANCE_ID = b.BPR_INSTANCE_ID
    WHERE b.CNTRCT_ID IS NULL

    UNION ALL

    SELECT 
      a.CNTRCT_ID,
      'VALUE_MISMATCH' AS mismatch_type
    FROM bq_target a
    JOIN oracle_target b ON a.CNTRCT_ID = b.CNTRCT_ID AND a.BPR_INSTANCE_ID = b.BPR_INSTANCE_ID
    WHERE 
      a.BPR_ID != b.BPR_ID
      OR a.ICCID != b.ICCID
      OR COALESCE(a.IMSI_MCC, '') != COALESCE(b.IMSI_MCC, '')
      OR COALESCE(a.IMSI_MNC, '') != COALESCE(b.IMSI_MNC, '')
      OR COALESCE(a.IMSI_HLR, '') != COALESCE(b.IMSI_HLR, '')
      OR COALESCE(a.IMSI_SI, '') != COALESCE(b.IMSI_SI, '')
      OR COALESCE(a.CNTRCT_ID_REF, -1) != COALESCE(b.CNTRCT_ID_REF, -1)
  )
  SELECT 
    mismatch_type, 
    COUNT(1) AS mismatch_count 
  FROM mismatches 
  GROUP BY mismatch_type;
  -- PASS: Query returns 0 rows.
  -- FAIL: Query returns rows indicating BQ_ONLY, ORACLE_ONLY, or VALUE_MISMATCH.
  ```

---

## Section 2: Transformation Correctness Tests

### Test Case 2.1: Watermark (`v_datum`) Resolution & Fallback
* **Purpose**: Verify that the dynamic watermark `v_datum` is correctly resolved from `dwtk_meldungen` and falls back to `1900-01-01` if no matching log entry exists.
* **Setup**:
  Create a mock `dwtk_meldungen` table.
* **Action**:
  Run two test scenarios:
  * **Scenario A**: `dwtk_meldungen` contains a record for `BERT_DROP_TEMP_TABLE` with `timecreated = '2025-02-10 14:30:00 UTC'`.
  * **Scenario B**: `dwtk_meldungen` is empty.
* **Pass/Fail Criterion**:
  * In **Scenario A**, `v_datum` must resolve to `2025-02-10`.
  * In **Scenario B**, `v_datum` must resolve to `1900-01-01`.
* **Validation Code (pytest)**:
  ```python
  import pytest
  from google.cloud import bigquery

  @pytest.fixture
  def bq_client():
      return bigquery.Client()

  def test_watermark_resolution(bq_client):
      # Scenario A: Valid entry exists
      query_a = """
      DECLARE v_datum DATE;
      CREATE TEMP TABLE mock_meldungen (job_kennung STRING, timecreated TIMESTAMP);
      INSERT INTO mock_meldungen VALUES ('BERT_DROP_TEMP_TABLE', TIMESTAMP('2025-02-10 14:30:00 UTC'));
      
      SET v_datum = (
        SELECT COALESCE(DATE(MAX(timecreated)), DATE '1900-01-01')
        FROM mock_meldungen
        WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
      );
      SELECT v_datum;
      """
      result_a = list(bq_client.query(query_a).result())[0][0]
      assert str(result_a) == "2025-02-10"

      # Scenario B: No entry exists
      query_b = """
      DECLARE v_datum DATE;
      CREATE TEMP TABLE mock_meldungen (job_kennung STRING, timecreated TIMESTAMP);
      
      SET v_datum = (
        SELECT COALESCE(DATE(MAX(timecreated)), DATE '1900-01-01')
        FROM mock_meldungen
        WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
      );
      SELECT v_datum;
      """
      result_b = list(bq_client.query(query_b).result())[0][0]
      assert str(result_b) == "1900-01-01"
  ```

### Test Case 2.2: ICCID Concatenation & NULL Handling
* **Purpose**: Verify that the `build_iccid` temporary function correctly concatenates the five ICCID components and handles `NULL` values without propagating `NULL` to the entire output string (matching Oracle's string concatenation behavior).
* **Setup**:
  Define a set of test inputs with varying `NULL` placements.
* **Action**:
  Evaluate the `build_iccid` function on these inputs.
* **Pass/Fail Criterion**:
  The function must return the exact expected strings below. If any component is `NULL`, it must be treated as an empty string, but the hyphens (`-`) must remain in place.
* **Validation Code (SQL)**:
  ```sql
  CREATE TEMP FUNCTION build_iccid(
    iccid_mi STRING, iccid_ii STRING, iccid_iai STRING, iccid_nr STRING, iccid_cd STRING
  ) AS (
    CONCAT(
      COALESCE(iccid_mi, ''), '-',
      COALESCE(iccid_ii, ''), '-',
      COALESCE(iccid_iai, ''), '-',
      COALESCE(iccid_nr, ''), '-',
      COALESCE(iccid_cd, '')
    )
  );

  WITH test_cases AS (
    SELECT '89' AS mi, '49' AS ii, '11' AS iai, '123456' AS nr, '7' AS cd, '89-49-11-123456-7' AS expected UNION ALL
    SELECT NULL, '49', '11', '123456', '7', '-49-11-123456-7' AS expected UNION ALL
    SELECT '89', NULL, NULL, '123456', NULL, '89---123456-' AS expected UNION ALL
    SELECT NULL, NULL, NULL, NULL, NULL, '----' AS expected
  )
  SELECT 
    mi, ii, iai, nr, cd, expected,
    build_iccid(mi, ii, iai, nr, cd) AS actual,
    build_iccid(mi, ii, iai, nr, cd) = expected AS is_correct
  FROM test_cases
  -- ASSERTION: Every row returned must have is_correct = TRUE
  ```

### Test Case 2.3: Contract Type and Parent Filtering Logic
* **Purpose**: Verify the complex conditional filter: `(cntrct_ty NOT IN (1, 2, 5) OR cntrct_parent IS NOT NULL)`.
* **Setup**:
  Insert mock contracts with varying combinations of `cntrct_ty` and `cntrct_parent`.
* **Action**:
  Run the filtering logic against these mock contracts.
* **Pass/Fail Criterion**:
  * Contracts with type `1`, `2`, or `5` must be **excluded** unless they have a non-null `cntrct_parent`.
  * Contracts with other types (e.g., `3`, `4`, `6`) must be **included** regardless of `cntrct_parent`.
* **Validation Code (SQL)**:
  ```sql
  WITH mock_contracts AS (
    SELECT 101 AS cntrct_id, 1 AS cntrct_ty, CAST(NULL AS INT64) AS cntrct_parent, 'EXCLUDE' AS expected UNION ALL
    SELECT 102 AS cntrct_id, 1 AS cntrct_ty, 999 AS cntrct_parent, 'INCLUDE' AS expected UNION ALL
    SELECT 103 AS cntrct_id, 3 AS cntrct_ty, CAST(NULL AS INT64) AS cntrct_parent, 'INCLUDE' AS expected UNION ALL
    SELECT 104 AS cntrct_id, 5 AS cntrct_ty, CAST(NULL AS INT64) AS cntrct_parent, 'EXCLUDE' AS expected UNION ALL
    SELECT 105 AS cntrct_id, 5 AS cntrct_ty, 888 AS cntrct_parent, 'INCLUDE' AS expected
  )
  SELECT 
    cntrct_id, cntrct_ty, cntrct_parent, expected,
    CASE 
      WHEN (cntrct_ty NOT IN (1, 2, 5) OR cntrct_parent IS NOT NULL) THEN 'INCLUDE'
      ELSE 'EXCLUDE'
    END AS actual
  FROM mock_contracts
  -- ASSERTION: actual must equal expected for all rows.
  ```

### Test Case 2.4: Temporal / SCD2 Filtering Boundaries
* **Purpose**: Verify that records are correctly filtered based on the dynamic watermark `v_datum` across all temporal fields (`insert_at`, `modified_at`, `valid_from`, `valid_to`).
* **Setup**:
  Set `v_datum = '2025-01-15'`. Create mock contracts with different temporal boundaries.
* **Action**:
  Apply the temporal filter:
  `DATE(insert_at) <= v_datum AND (modified_at IS NULL OR DATE(modified_at) > v_datum) AND DATE(valid_from) <= v_datum AND (valid_to IS NULL OR DATE(valid_to) > v_datum)`
* **Pass/Fail Criterion**:
  Only records active on or before `v_datum` and not modified/expired on or before `v_datum` must be included.
* **Validation Code (SQL)**:
  ```sql
  DECLARE v_datum DATE DEFAULT '2025-01-15';

  WITH mock_temporal_contracts AS (
    -- Case 1: Active and valid (Should Include)
    SELECT 201 AS id, TIMESTAMP('2025-01-01') AS insert_at, CAST(NULL AS TIMESTAMP) AS modified_at, TIMESTAMP('2025-01-01') AS valid_from, CAST(NULL AS TIMESTAMP) AS valid_to, 'INCLUDE' AS expected UNION ALL
    -- Case 2: Inserted after watermark (Should Exclude)
    SELECT 202 AS id, TIMESTAMP('2025-01-16') AS insert_at, CAST(NULL AS TIMESTAMP) AS modified_at, TIMESTAMP('2025-01-01') AS valid_from, CAST(NULL AS TIMESTAMP) AS valid_to, 'EXCLUDE' AS expected UNION ALL
    -- Case 3: Modified before watermark (Should Exclude - superseded)
    SELECT 203 AS id, TIMESTAMP('2025-01-01') AS insert_at, TIMESTAMP('2025-01-14') AS modified_at, TIMESTAMP('2025-01-01') AS valid_from, CAST(NULL AS TIMESTAMP) AS valid_to, 'EXCLUDE' AS expected UNION ALL
    -- Case 4: Valid to date is in the past (Should Exclude - expired)
    SELECT 204 AS id, TIMESTAMP('2025-01-01') AS insert_at, CAST(NULL AS TIMESTAMP) AS modified_at, TIMESTAMP('2025-01-01') AS valid_from, TIMESTAMP('2025-01-10') AS valid_to, 'EXCLUDE' AS expected UNION ALL
    -- Case 5: Valid from is in the future (Should Exclude - not yet active)
    SELECT 205 AS id, TIMESTAMP('2025-01-01') AS insert_at, CAST(NULL AS TIMESTAMP) AS modified_at, TIMESTAMP('2025-01-16') AS valid_from, CAST(NULL AS TIMESTAMP) AS valid_to, 'EXCLUDE' AS expected
  )
  SELECT 
    id, expected,
    CASE 
      WHEN DATE(insert_at) <= v_datum
       AND (modified_at IS NULL OR DATE(modified_at) > v_datum)
       AND DATE(valid_from) <= v_datum
       AND (valid_to IS NULL OR DATE(valid_to) > v_datum) THEN 'INCLUDE'
      ELSE 'EXCLUDE'
    END AS actual
  FROM mock_temporal_contracts;
  -- ASSERTION: actual must equal expected for all rows.
  ```

---

## Section 3: External-System Replacements & Orchestration

### Test Case 3.1: Airflow DAG Compilation & Variable Substitution
* **Purpose**: Ensure the Airflow DAG compiles without syntax errors and correctly resolves environment-specific variables (e.g., `gcp_project_id`, `isbert_dataset`, `sof_dataset`) within the SQL template.
* **Setup**:
  An Airflow testing environment (or local `pytest` with `apache-airflow` installed).
* **Action**:
  Import the DAG and render the SQL template for the task `run_d_ausd_bp_ta_bpr_instance`.
* **Pass/Fail Criterion**:
  The DAG must load without errors, and the rendered SQL must contain the substituted project and dataset names instead of Jinja placeholders.
* **Validation Code (pytest)**:
  ```python
  import os
  from airflow.models import DagBag, Variable
  from airflow.models.taskinstance import TaskInstance
  from datetime import datetime

  def test_dag_compilation_and_template_rendering(monkeypatch):
      # Mock Airflow Variables
      mock_vars = {
          "gcp_project_id": "gcp-dwh-test-project",
          "isbert_dataset": "test_isbert",
          "sof_dataset": "test_sof",
          "cds_dataset": "test_cds",
          "pds_dataset": "test_pds"
      }
      monkeypatch.setattr(Variable, "get", lambda key, default_var=None: mock_vars.get(key, default_var))

      # Load DagBag
      dag_dir = os.path.join(os.path.dirname(__file__), "../dags")
      dagbag = DagBag(dag_folder=dag_dir, include_examples=False)
      
      assert dagbag.import_errors == {}
      dag = dagbag.get_dag(dag_id="dw_bert_ausd_bp_ta_bpr_instance")
      assert dag is not None

      # Retrieve task and render template
      task = dag.get_task("run_d_ausd_bp_ta_bpr_instance")
      
      # Create a dummy DagRun and TaskInstance to resolve templates
      execution_date = datetime(2025, 1, 1)
      ti = TaskInstance(task=task, execution_date=execution_date)
      
      # Render templates
      rendered_sql = task.render_template(task.sql, ti.get_template_context())
      
      # Assertions to verify variable substitution
      assert "gcp-dwh-test-project.test_isbert.dwtk_meldungen" in rendered_sql
      assert "gcp-dwh-test-project.test_sof.ta_bpr_instance" in rendered_sql
      assert "gcp-dwh-test-project.test_cds.ta_cntrct" in rendered_sql
      assert "gcp-dwh-test-project.test_pds.ta_bpri_com" in rendered_sql
      assert "{{" not in rendered_sql  # Ensure no unrendered Jinja remains
  ```

---

## Section 4: Data-Quality, Schema, and Row-Count Assertions

### Test Case 4.1: Target Schema & Nullability Validation
* **Purpose**: Verify that the target table `ta_bpr_instance` in BigQuery matches the required schema structure, data types, and nullability constraints.
* **Setup**:
  The target table `ta_bpr_instance` must be deployed in the target dataset.
* **Action**:
  Query the BigQuery `INFORMATION_SCHEMA.COLUMNS` view for the target table.
* **Pass/Fail Criterion**:
  The columns, data types, and nullability must match the design specification exactly.
* **Validation Code (SQL)**:
  ```sql
  WITH expected_schema AS (
    SELECT 'CNTRCT_ID' AS column_name, 'INT64' AS data_type, 'YES' AS is_nullable UNION ALL
    SELECT 'BPR_ID', 'INT64', 'YES' UNION ALL
    SELECT 'BPR_INSTANCE_ID', 'INT64', 'YES' UNION ALL
    SELECT 'ICCID', 'STRING', 'YES' UNION ALL
    SELECT 'IMSI_MCC', 'STRING', 'YES' UNION ALL
    SELECT 'IMSI_MNC', 'STRING', 'YES' UNION ALL
    SELECT 'IMSI_HLR', 'STRING', 'YES' UNION ALL
    SELECT 'IMSI_SI', 'STRING', 'YES' UNION ALL
    SELECT 'CNTRCT_ID_REF', 'INT64', 'YES'
  ),
  actual_schema AS (
    SELECT 
      column_name, 
      data_type, 
      is_nullable
    FROM `{{ var.value.gcp_project_id }}.{{ var.value.sof_dataset }}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'ta_bpr_instance'
  )
  SELECT 
    COALESCE(e.column_name, a.column_name) AS col_name,
    e.data_type AS exp_type, a.data_type AS act_type,
    e.is_nullable AS exp_null, a.is_nullable AS act_null,
    CASE 
      WHEN a.column_name IS NULL THEN 'MISSING IN TARGET'
      WHEN e.column_name IS NULL THEN 'UNEXPECTED IN TARGET'
      WHEN e.data_type != a.data_type THEN 'TYPE MISMATCH'
      WHEN e.is_nullable != a.is_nullable THEN 'NULLABILITY MISMATCH'
      ELSE 'OK'
    END AS status
  FROM expected_schema e
  FULL OUTER JOIN actual_schema a ON e.column_name = a.column_name
  WHERE e.column_name IS NULL 
     OR a.column_name IS NULL 
     OR e.data_type != a.data_type 
     OR e.is_nullable != a.is_nullable;
  -- PASS: Query returns 0 rows.
  -- FAIL: Query returns rows indicating schema mismatches.
  ```

### Test Case 4.2: Post-Load Data Quality Assertions
* **Purpose**: Run post-execution data quality checks on the target table to ensure no logical corruption occurred (e.g., orphaned records, malformed ICCIDs).
* **Setup**:
  The BigQuery migration job has completed execution.
* **Action**:
  Execute a series of data quality checks on the target table.
* **Pass/Fail Criterion**:
  * **Check A**: No duplicate `BPR_INSTANCE_ID` values exist (Primary Key constraint).
  * **Check B**: All `ICCID` values contain exactly 4 hyphens (verifying the `CONCAT` logic worked correctly).
  * **Check C**: No `CNTRCT_ID` is null.
* **Validation Code (SQL)**:
  ```sql
  -- Check A: Duplicate Key Check
  SELECT 'DUPLICATE_KEYS' AS check_name, COUNT(1) AS failure_count
  FROM (
    SELECT BPR_INSTANCE_ID, COUNT(1)
    FROM `{{ var.value.gcp_project_id }}.{{ var.value.sof_dataset }}.ta_bpr_instance`
    WHERE BPR_INSTANCE_ID IS NOT NULL
    GROUP BY BPR_INSTANCE_ID
    HAVING COUNT(1) > 1
  )

  UNION ALL

  -- Check B: Malformed ICCID Check (Must have exactly 4 hyphens)
  SELECT 'MALFORMED_ICCID' AS check_name, COUNT(1) AS failure_count
  FROM `{{ var.value.gcp_project_id }}.{{ var.value.sof_dataset }}.ta_bpr_instance`
  WHERE LENGTH(ICCID) - LENGTH(REPLACE(ICCID, '-', '')) != 4

  UNION ALL

  -- Check C: Null Contract ID Check
  SELECT 'NULL_CONTRACT_ID' AS check_name, COUNT(1) AS failure_count
  FROM `{{ var.value.gcp_project_id }}.{{ var.value.sof_dataset }}.ta_bpr_instance`
  WHERE CNTRCT_ID IS NULL;

  -- PASS: All checks return a failure_count of 0.
  -- FAIL: Any check returns a failure_count > 0.
  ```