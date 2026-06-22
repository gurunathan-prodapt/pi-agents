As a senior data-migration QA engineer, I have reviewed the migration design document and the generated Airflow DAG for `DW.BERT_AUSD_BP_TA_BCP_ICCID`. The following test plan is designed to ensure the migrated solution is behaviourally equivalent to the legacy system, covering output parity, transformation correctness, external system replacements, and data quality assertions.

---

## Migration Validation Test Plan: DW.BERT_AUSD_BP_TA_BCP_ICCID

**Assumptions:**
*   The BigQuery source tables (`PROJECT_ID.DATASET_ID.TA_BPR_BCP`, `PROJECT_ID.DATASET_ID.TA_ICCID_VERTRAG`, `PROJECT_ID.DATASET_ID.DWTK_MELDUNGEN`) are populated with data that accurately reflects the Oracle legacy system for each test scenario.
*   The Airflow DAG `dw_bert_ausd_bp_ta_bcp_iccid` is deployed and accessible in the target environment.
*   `PROJECT_ID` and `DATASET_ID` placeholders in the DAG are replaced with actual values.
*   The `google_cloud_default` Airflow connection is correctly configured for BigQuery access.
*   The target table `PROJECT_ID.DATASET_ID.TA_BCP_ICCID` has been created with the appropriate schema as a prerequisite to running the job.

---

### 1. Output Parity Tests

These tests ensure that for the same inputs, the migrated job produces the exact same output as the legacy Oracle job.

#### Test 1.1: Full Data Parity (Happy Path)

*   **Purpose:** Verify that the migrated job produces an identical dataset in the target BigQuery table (`TA_BCP_ICCID`) compared to the legacy Oracle job's output (`sof$ta_bcp_iccid`) under normal operating conditions. This is the most critical end-to-end test.
*   **Setup:**
    1.  Identify a representative, non-trivial dataset in the legacy Oracle environment for `sof$ta_bpr_bcp`, `sof$ta_iccid_vertrag`, and `isbert_schema.dwtk_meldungen`.
    2.  Execute the legacy Oracle job with this dataset and capture the final state of `sof$ta_bcp_iccid`. Export this data to a format suitable for BigQuery (e.g., CSV, JSON).
    3.  Load this exact dataset into the corresponding BigQuery source tables: `PROJECT_ID.DATASET_ID.TA_BPR_BCP`, `PROJECT_ID.DATASET_ID.TA_ICCID_VERTRAG`, `PROJECT_ID.DATASET_ID.DWTK_MELDUNGEN`.
    4.  Load the captured legacy output into a temporary BigQuery table, e.g., `PROJECT_ID.DATASET_ID.LEGACY_TA_BCP_ICCID_OUTPUT`, ensuring schema compatibility.
    5.  Ensure the BigQuery target table `PROJECT_ID.DATASET_ID.TA_BCP_ICCID` is empty before running the migrated job.
*   **Action:** Trigger the Airflow DAG `dw_bert_ausd_bp_ta_bcp_iccid`.
*   **Pass/Fail Criterion:**
    *   The Airflow DAG completes successfully.
    *   The data in `PROJECT_ID.DATASET_ID.TA_BCP_ICCID` is identical to the data in `PROJECT_ID.DATASET_ID.LEGACY_TA_BCP_ICCID_OUTPUT` (same number of rows, same values for all columns, order-independent comparison).

    ```sql
    -- SQL assertion to compare row counts and content
    SELECT
        (SELECT COUNT(*) FROM `PROJECT_ID.DATASET_ID.TA_BCP_ICCID`) = (SELECT COUNT(*) FROM `PROJECT_ID.DATASET_ID.LEGACY_TA_BCP_ICCID_OUTPUT`) AS row_count_match,
        (SELECT COUNT(*) FROM (
            SELECT * FROM `PROJECT_ID.DATASET_ID.TA_BCP_ICCID`
            EXCEPT DISTINCT
            SELECT * FROM `PROJECT_ID.DATASET_ID.LEGACY_TA_BCP_ICCID_OUTPUT`
        )) = 0 AS no_extra_rows_in_migrated,
        (SELECT COUNT(*) FROM (
            SELECT * FROM `PROJECT_ID.DATASET_ID.LEGACY_TA_BCP_ICCID_OUTPUT`
            EXCEPT DISTINCT
            SELECT * FROM `PROJECT_ID.DATASET_ID.TA_BCP_ICCID`
        )) = 0 AS no_missing_rows_in_migrated;
    -- Expected: All three boolean columns should be TRUE.
    ```

#### Test 1.2: Row Count Parity (Happy Path)

*   **Purpose:** Verify that the migrated job produces the same number of rows in the target table as the legacy job. This serves as a quick sanity check for output parity.
*   **Setup:** Same as Test 1.1, but only the row count from the legacy output is strictly needed.
*   **Action:** Trigger the Airflow DAG `dw_bert_ausd_bp_ta_bcp_iccid`.
*   **Pass/Fail Criterion:**
    *   The Airflow DAG completes successfully.
    *   The row count of `PROJECT_ID.DATASET_ID.TA_BCP_ICCID` matches the row count of the legacy `sof$ta_bcp_iccid` for the same input data.

    ```sql
    -- SQL assertion
    SELECT COUNT(*) FROM `PROJECT_ID.DATASET_ID.TA_BCP_ICCID`;
    -- Expected: <count from legacy output, e.g., 12345>
    ```

---

### 2. Transformation Correctness Tests

These tests focus on the specific logic within the SQL transformation, including joins, distinct operations, and NULL handling.

#### Test 2.1: Join Logic Correctness

*   **Purpose:** Verify that the `INNER JOIN` condition `bp.cntrct_id_ref = ic.cntrct_id` behaves identically to the Oracle version, correctly matching and excluding records.
*   **Setup:**
    1.  Populate `TA_BPR_BCP` with:
        *   Rows where `cntrct_id_ref` has a unique match in `TA_ICCID_VERTRAG`.
        *   Rows where `cntrct_id_ref` has no match in `TA_ICCID_VERTRAG`.
        *   Rows where `cntrct_id_ref` has multiple matches in `TA_ICCID_VERTRAG` (e.g., `TA_ICCID_VERTRAG` has multiple entries for the same `CNTRCT_ID`).
    2.  Populate `TA_ICCID_VERTRAG` with:
        *   Rows where `cntrct_id` has a unique match in `TA_BPR_BCP`.
        *   Rows where `cntrct_id` has no match in `TA_BPR_BCP`.
    3.  Ensure `TA_BCP_ICCID` is empty.
*   **Action:** Trigger the Airflow DAG.
*   **Pass/Fail Criterion:**
    *   The DAG completes successfully.
    *   Only records where `bp.cntrct_id_ref` exactly matches `ic.cntrct_id` are present in the target table.
    *   If `bp.cntrct_id_ref` matches multiple `ic.cntrct_id` values, all resulting combinations (before `DISTINCT`) are correctly processed, and the final distinct output reflects this.

    ```sql
    -- Example: Verify a specific join scenario.
    -- Assume TA_BPR_BCP has (1, 'BPR1', 100) and TA_ICCID_VERTRAG has (100, 'ICC1', 'IMSI1') and (100, 'ICC2', 'IMSI2').
    -- If (1, 'BPR1', 100, 'ICC1', 'IMSI1') and (1, 'BPR1', 100, 'ICC2', 'IMSI2') are distinct, both should be present.
    SELECT COUNT(*) FROM `PROJECT_ID.DATASET_ID.TA_BCP_ICCID` WHERE CNTRCT_ID_REF = 100;
    -- Expected: 2 (if the other columns make them distinct) or 1 (if other columns are identical, due to DISTINCT).
    ```

#### Test 2.2: DISTINCT Clause Correctness

*   **Purpose:** Verify that the `DISTINCT` keyword correctly removes duplicate rows based on the combination of all selected columns (`CNTRCT_ID`, `BPR_ID`, `CNTRCT_ID_REF`, `TN_ICCID`, `TN_IMSI_HLR`).
*   **Setup:**
    1.  Populate `TA_BPR_BCP` and `TA_ICCID_VERTRAG` such that the join operation would produce multiple identical rows for the combination of selected output columns.
        *   Example: `TA_BPR_BCP` has `(1, 'BPR1', 100)` and `TA_ICCID_VERTRAG` has `(100, 'ICC1', 'IMSI1')`. If `TA_BPR_BCP` also has another row `(1, 'BPR1', 100)` (a duplicate source row), the join would produce `(1, 'BPR1', 100, 'ICC1', 'IMSI1')` twice.
    2.  Ensure `TA_BCP_ICCID` is empty.
*   **Action:** Trigger the Airflow DAG.
*   **Pass/Fail Criterion:**
    *   The DAG completes successfully.
    *   The target table `TA_BCP_ICCID` contains only unique combinations of (`CNTRCT_ID`, `BPR_ID`, `CNTRCT_ID_REF`, `TN_ICCID`, `TN_IMSI_HLR`).
    *   The row count in `TA_BCP_ICCID` is less than the row count that would result from the join *without* the `DISTINCT` clause, if duplicates were introduced in the setup.

    ```sql
    -- SQL assertion
    SELECT COUNT(*) AS total_rows,
           COUNT(DISTINCT CONCAT(CNTRCT_ID, '|', BPR_ID, '|', CNTRCT_ID_REF, '|', TN_ICCID, '|', TN_IMSI_HLR)) AS distinct_rows
    FROM `PROJECT_ID.DATASET_ID.TA_BCP_ICCID`;
    -- Expected: total_rows should be equal to distinct_rows.
    ```

#### Test 2.3: NULL Handling in Join Keys

*   **Purpose:** Verify that rows with `NULL` values in the join columns (`bp.cntrct_id_ref` or `ic.cntrct_id`) are correctly excluded from the result, as `INNER JOIN` typically does not match `NULL` with `NULL`.
*   **Setup:**
    1.  Populate `TA_BPR_BCP` with rows where `cntrct_id_ref` is `NULL`.
    2.  Populate `TA_ICCID_VERTRAG` with rows where `cntrct_id` is `NULL`.
    3.  Include some valid joinable rows to ensure the job still processes.
    4.  Ensure `TA_BCP_ICCID` is empty.
*   **Action:** Trigger the Airflow DAG.
*   **Pass/Fail Criterion:**
    *   The DAG completes successfully.
    *   No rows in `TA_BCP_ICCID` originate from records where `bp.cntrct_id_ref` was `NULL` or `ic.cntrct_id` was `NULL`. Specifically, the `CNTRCT_ID_REF` column in the output should never be `NULL`.

    ```sql
    -- SQL assertion: Verify that the join key in the output is never NULL
    SELECT COUNT(*) FROM `PROJECT_ID.DATASET_ID.TA_BCP_ICCID`
    WHERE CNTRCT_ID_REF IS NULL;
    -- Expected: 0
    ```

#### Test 2.4: NULL Handling in Selected Columns (Non-Join Keys)

*   **Purpose:** Verify that `NULL` values in non-join key columns (`bp.cntrct_id`, `bp.bpr_id`, `ic.tn_iccid`, `ic.tn_imsi_hlr`) are correctly preserved and transferred to the target table.
*   **Setup:**
    1.  Populate `TA_BPR_BCP` and `TA_ICCID_VERTRAG` with rows that successfully join, but where some of the selected columns (e.g., `bp.cntrct_id`, `bp.bpr_id`, `ic.tn_iccid`, `ic.tn_imsi_hlr`) contain `NULL` values.
    2.  Ensure `TA_BCP_ICCID` is empty.
*   **Action:** Trigger the Airflow DAG.
*   **Pass/Fail Criterion:**
    *   The DAG completes successfully.
    *   The `NULL` values in the specified columns are present in the corresponding output columns of `TA_BCP_ICCID` for the joined rows.

    ```sql
    -- SQL assertion (example for TN_ICCID)
    -- Assume a specific CNTRCT_ID_REF (e.g., 200) that joins, and for which TN_ICCID was NULL in source.
    SELECT TN_ICCID FROM `PROJECT_ID.DATASET_ID.TA_BCP_ICCID` WHERE CNTRCT_ID_REF = 200;
    -- Expected: NULL (if the corresponding source value was NULL)
    ```

#### Test 2.5: Empty Source Tables

*   **Purpose:** Verify the job handles scenarios where both main source tables (`TA_BPR_BCP` and `TA_ICCID_VERTRAG`) are empty gracefully.
*   **Setup:**
    1.  Empty `PROJECT_ID.DATASET_ID.TA_BPR_BCP`.
    2.  Empty `PROJECT_ID.DATASET_ID.TA_ICCID_VERTRAG`.
    3.  `PROJECT_ID.DATASET_ID.DWTK_MELDUNGEN` can be empty or populated (it won't affect the main insert logic).
    4.  Ensure `TA_BCP_ICCID` is empty.
*   **Action:** Trigger the Airflow DAG.
*   **Pass/Fail Criterion:**
    *   The DAG completes successfully (no errors due to empty tables).
    *   `TA_BCP_ICCID` remains empty (row count is 0).

    ```sql
    -- SQL assertion
    SELECT COUNT(*) FROM `PROJECT_ID.DATASET_ID.TA_BCP_ICCID`;
    -- Expected: 0
    ```

#### Test 2.6: One Source Table Empty

*   **Purpose:** Verify the job handles scenarios where only one of the two main source tables (`TA_BPR_BCP` or `TA_ICCID_VERTRAG`) is empty.
*   **Setup:**
    1.  **Scenario A:** `PROJECT_ID.DATASET_ID.TA_BPR_BCP` is empty, `PROJECT_ID.DATASET_ID.TA_ICCID_VERTRAG` is populated.
    2.  **Scenario B:** `PROJECT_ID.DATASET_ID.TA_BPR_BCP` is populated, `PROJECT_ID.DATASET_ID.TA_ICCID_VERTRAG` is empty.
    3.  `PROJECT_ID.DATASET_ID.DWTK_MELDUNGEN` can be empty or populated.
    4.  Ensure `TA_BCP_ICCID` is empty for each scenario before running.
*   **Action:** Trigger the Airflow DAG for each scenario (A then B, or vice-versa).
*   **Pass/Fail Criterion:**
    *   The DAG completes successfully for both scenarios.
    *   `TA_BCP_ICCID` remains empty (row count is 0) for both scenarios, as an `INNER JOIN` with an empty table will always result in an empty set.

    ```sql
    -- SQL assertion (after running for each scenario)
    SELECT COUNT(*) FROM `PROJECT_ID.DATASET_ID.TA_BCP_ICCID`;
    -- Expected: 0
    ```

---

### 3. External System Replacements & Orchestration

These tests verify the correct functioning of the Airflow orchestration and the interaction with BigQuery, including the `v_datum` retrieval logic.

#### Test 3.1: `v_datum` Retrieval - Happy Path

*   **Purpose:** Verify that the `fetch_stichtag_task` correctly retrieves the maximum `timecreated` for `job_kennung = 'BERT_DROP_TEMP_TABLE'` and formats it as 'YYYYMMDD', pushing it to XCom.
*   **Setup:**
    1.  Populate `PROJECT_ID.DATASET_ID.DWTK_MELDUNGEN` with multiple rows, including:
        *   Several rows with `job_kennung = 'BERT_DROP_TEMP_TABLE'` and varying `timecreated` values (e.g., '2023-01-15 10:00:00', '2023-01-20 12:30:00').
        *   Rows with other `job_kennung` values.
    2.  Identify the expected `MAX(timecreated)` for `BERT_DROP_TEMP_TABLE` and its 'YYYYMMDD' format (e.g., '20230120').
*   **Action:** Trigger the Airflow DAG.
*   **Pass/Fail Criterion:**
    *   The `fetch_stichtag_task` completes successfully.
    *   The `stichtag_date` XCom value (retrieved from the task instance) matches the expected 'YYYYMMDD' string derived from the `MAX(timecreated)` in the setup.

    ```python
    # Conceptual pytest-like assertion (requires Airflow testing framework)
    from airflow.models import DagRun
    from airflow.utils.session import provide_session
    from airflow.utils.state import State

    @provide_session
    def test_stichtag_happy_path(session=None):
        # Assuming a DAG run has just completed
        dag_run = session.query(DagRun).filter(DagRun.dag_id == 'dw_bert_ausd_bp_ta_bcp_iccid').order_by(DagRun.execution_date.desc()).first()
        assert dag_run.state == State.SUCCESS

        ti = dag_run.get_task_instance(task_id='fetch_stichtag_task', session=session)
        stichtag = ti.xcom_pull(key='stichtag_date', task_ids='fetch_stichtag_task')
        assert stichtag == '20230120' # Example expected date based on setup
    ```

#### Test 3.2: `v_datum` Retrieval - No Matching `job_kennung`

*   **Purpose:** Verify that if no rows match `job_kennung = 'BERT_DROP_TEMP_TABLE'`, the `v_datum` defaults to '19000101'.
*   **Setup:**
    1.  Populate `PROJECT_ID.DATASET_ID.DWTK_MELDUNGEN` with rows, but *none* with `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
*   **Action:** Trigger the Airflow DAG.
*   **Pass/Fail Criterion:**
    *   The `fetch_stichtag_task` completes successfully.
    *   The `stichtag_date` XCom value is '19000101'.

    ```python
    # Conceptual pytest-like assertion
    @provide_session
    def test_stichtag_no_matching_job_kennung(session=None):
        dag_run = session.query(DagRun).filter(DagRun.dag_id == 'dw_bert_ausd_bp_ta_bcp_iccid').order_by(DagRun.execution_date.desc()).first()
        ti = dag_run.get_task_instance(task_id='fetch_stichtag_task', session=session)
        stichtag = ti.xcom_pull(key='stichtag_date', task_ids='fetch_stichtag_task')
        assert stichtag == '19000101'
    ```

#### Test 3.3: `v_datum` Retrieval - `timecreated` is NULL

*   **Purpose:** Verify that if the `MAX(timecreated)` for the relevant `job_kennung` is `NULL`, the `v_datum` defaults to '19000101'.
*   **Setup:**
    1.  Populate `PROJECT_ID.DATASET_ID.DWTK_MELDUNGEN` with at least one row where `job_kennung = 'BERT_DROP_TEMP_TABLE'` and `timecreated` is `NULL`. Ensure this is the *only* or *maximum* `timecreated` for that `job_kennung`.
*   **Action:** Trigger the Airflow DAG.
*   **Pass/Fail Criterion:**
    *   The `fetch_stichtag_task` completes successfully.
    *   The `stichtag_date` XCom value is '19000101'.

    ```python
    # Conceptual pytest-like assertion
    @provide_session
    def test_stichtag_timecreated_null(session=None):
        dag_run = session.query(DagRun).filter(DagRun.dag_id == 'dw_bert_ausd_bp_ta_bcp_iccid').order_by(DagRun.execution_date.desc()).first()
        ti = dag_run.get_task_instance(task_id='fetch_stichtag_task', session=session)
        stichtag = ti.xcom_pull(key='stichtag_date', task_ids='fetch_stichtag_task')
        assert stichtag == '19000101'
    ```

#### Test 3.4: `v_datum` Retrieval - Empty `DWTK_MELDUNGEN` table

*   **Purpose:** Verify that if `PROJECT_ID.DATASET_ID.DWTK_MELDUNGEN` is entirely empty, the `v_datum` defaults to '19000101'.
*   **Setup:**
    1.  Empty `PROJECT_ID.DATASET_ID.DWTK_MELDUNGEN` table.
*   **Action:** Trigger the Airflow DAG.
*   **Pass/Fail Criterion:**
    *   The `fetch_stichtag_task` completes successfully.
    *   The `stichtag_date` XCom value is '19000101'.

    ```python
    # Conceptual pytest-like assertion
    @provide_session
    def test_stichtag_empty_dwtk_meldungen(session=None):
        dag_run = session.query(DagRun).filter(DagRun.dag_id == 'dw_bert_ausd_bp_ta_bcp_iccid').order_by(DagRun.execution_date.desc()).first()
        ti = dag_run.get_task_instance(task_id='fetch_stichtag_task', session=session)
        stichtag = ti.xcom_pull(key='stichtag_date', task_ids='fetch_stichtag_task')
        assert stichtag == '19000101'
    ```

#### Test 3.5: Airflow Task Execution Order

*   **Purpose:** Verify that the Airflow tasks execute in the correct sequence as defined in the DAG: `fetch_stichtag_task` -> `truncate_target_table_task` -> `insert_data_task`.
*   **Setup:**
    1.  Deploy the Airflow DAG.
    2.  Ensure BigQuery source tables are populated with some data to allow tasks to run.
*   **Action:** Trigger the Airflow DAG.
*   **Pass/Fail Criterion:**
    *   The Airflow UI (or logs) shows the tasks executing in the order: `fetch_stichtag_task` completes, then `truncate_target_table_task` starts and completes, then `insert_data_task` starts and completes. No tasks run out of order or concurrently if not intended.

    ```python
    # Conceptual pytest-like assertion (requires Airflow testing framework to inspect DAG run state)
    @provide_session
    def test_task_execution_order(session=None):
        dag_run = session.query(DagRun).filter(DagRun.dag_id == 'dw_bert_ausd_bp_ta_bcp_iccid').order_by(DagRun.execution_date.desc()).first()
        assert dag_run.state == State.SUCCESS

        ti_fetch = dag_run.get_task_instance(task_id='fetch_stichtag_task', session=session)
        ti_truncate = dag_run.get_task_instance(task_id='truncate_target_table_task', session=session)
        ti_insert = dag_run.get_task_instance(task_id='insert_data_task', session=session)

        # Check that tasks completed successfully
        assert ti_fetch.state == State.SUCCESS
        assert ti_truncate.state == State.SUCCESS
        assert ti_insert.state == State.SUCCESS

        # Check start/end times for sequential execution
        assert ti_fetch.end_date < ti_truncate.start_date
        assert ti_truncate.end_date < ti_insert.start_date
    ```

#### Test 3.6: BigQuery Connection and Permissions

*   **Purpose:** Verify that the Airflow DAG has the necessary permissions and connectivity to interact with BigQuery (read from source tables, truncate and insert into target table).
*   **Setup:**
    1.  Ensure the Airflow service account has `bigquery.dataEditor` or equivalent roles on the `PROJECT_ID.DATASET_ID` dataset.
    2.  Ensure `google_cloud_default` connection is correctly configured in Airflow.
    3.  Populate source tables with minimal data.
*   **Action:** Trigger the Airflow DAG.
*   **Pass/Fail Criterion:**
    *   The entire DAG completes successfully without any BigQuery permission errors (e.g., `Access Denied`, `Not Found`, `Permission Denied`).
    *   Data is successfully inserted into `TA_BCP_ICCID`.
*   **Note:** This test is implicitly covered by other tests passing without permission errors. If any other test fails with a permission error, this test implicitly fails.

---

### 4. Data Quality & Schema Assertions

These tests ensure the integrity and correctness of the data and schema in the target BigQuery table.

#### Test 4.1: Target Table Schema Validation

*   **Purpose:** Verify that the schema of the target BigQuery table `TA_BCP_ICCID` matches the expected schema (column names, data types, nullability) derived from the legacy Oracle table `sof$ta_bcp_iccid`.
*   **Setup:**
    1.  Ensure `TA_BCP_ICCID` exists (it should be created as part of the migration prerequisite).
    2.  Obtain the schema definition of the legacy `sof$ta_bcp_iccid` table.
*   **Action:** Inspect the schema of `PROJECT_ID.DATASET_ID.TA_BCP_ICCID` in BigQuery using `INFORMATION_SCHEMA`.
*   **Pass/Fail Criterion:**
    *   The column names (`CNTRCT_ID`, `BPR_ID`, `CNTRCT_ID_REF`, `TN_ICCID`, `TN_IMSI_HLR`) are correct.
    *   The data types (e.g., `INT64`, `STRING`) for each column are appropriate and match the Oracle types (or their BigQuery equivalents).
    *   Nullability constraints (e.g., `CNTRCT_ID_REF` should likely be `NOT NULL` if it's a join key, but this depends on the target table DDL).

    ```sql
    -- SQL assertion (BigQuery metadata query)
    SELECT column_name, data_type, is_nullable
    FROM `PROJECT_ID.DATASET_ID.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'TA_BCP_ICCID'
    ORDER BY ordinal_position;
    /* Expected Output (example, adjust based on actual Oracle types and BigQuery mapping):
    column_name   | data_type | is_nullable
    --------------|-----------|------------
    CNTRCT_ID     | INT64     | YES
    BPR_ID        | STRING    | YES
    CNTRCT_ID_REF | INT64     | NO  (or YES, depending on DDL)
    TN_ICCID      | STRING    | YES
    TN_IMSI_HLR   | STRING    | YES
    */
    ```

#### Test 4.2: Data Type Integrity

*   **Purpose:** Verify that data types are correctly handled during the migration and transformation, preventing truncation, conversion errors, or unexpected behavior.
*   **Setup:**
    1.  Populate source tables with data that tests type boundaries (e.g., max length strings, large numbers).
    2.  Include values that might cause implicit type conversions or errors if not handled correctly (e.g., non-numeric strings in columns expected to be numeric, if the source allowed it).
    3.  Ensure `TA_BCP_ICCID` is empty.
*   **Action:** Trigger the Airflow DAG.
*   **Pass/Fail Criterion:**
    *   The DAG completes successfully.
    *   All data in `TA_BCP_ICCID` is correctly represented according to its BigQuery data type, without loss of precision or unexpected conversions.

    ```sql
    -- SQL assertion (example for a string column, checking length)
    SELECT MAX(LENGTH(TN_ICCID)) FROM `PROJECT_ID.DATASET_ID.TA_BCP_ICCID`;
    -- Expected: Should not exceed the defined length for the column, and match source.

    -- Example for a numeric column (CNTRCT_ID_REF) to ensure it's valid if stored as STRING in source
    -- This assumes CNTRCT_ID_REF is INT64 in target.
    SELECT COUNT(*) FROM `PROJECT_ID.DATASET_ID.TA_BCP_ICCID`
    WHERE CNTRCT_ID_REF IS NOT NULL AND NOT SAFE.PARSE_INT(CAST(CNTRCT_ID_REF AS STRING)) IS NOT NULL;
    -- Expected: COUNT(*) should be equal to the total number of non-NULL rows for CNTRCT_ID_REF.
    ```

---