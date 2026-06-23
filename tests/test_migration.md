As a senior data-migration QA engineer, I have analyzed the migration design for `k_ausd_bp_ta_rn_da_vda_tk.ksh` to BigQuery and Airflow. The core logic involves a `TRUNCATE` and `INSERT ... SELECT` operation with a specific `WHERE` clause. The KornShell script's orchestration, parameter handling, and utility script sourcing are largely replaced by Airflow's native capabilities or deemed non-critical for the direct SQL transformation.

The following test cases are designed to ensure the migrated solution is behaviourally equivalent to the legacy system, covering output parity, transformation correctness, external system replacements, and data quality.

---

### Test Case 1: Output Parity - Full Data Comparison

*   **Purpose:** To verify that the migrated BigQuery job produces an identical dataset in the target table (`sof$ta_rn_da_vda_tk`) as the legacy Oracle job, given the same source data. This is the most comprehensive test for output parity.
*   **Setup:**
    1.  Create a representative and diverse dataset for the source table `sof$ta_rn_einzeln` in both the Oracle legacy environment and the BigQuery migration environment. This dataset should include:
        *   Rows where `DA_RN_msisdn` is NOT NULL, others are NULL.
        *   Rows where `VDA_RN_msisdn` is NOT NULL, others are NULL.
        *   Rows where `TK_RN_msisdn` is NOT NULL, others are NULL.
        *   Rows where multiple `MSISDN` columns are NOT NULL.
        *   Rows where all three `MSISDN` columns (`DA_RN_msisdn`, `VDA_RN_msisdn`, `TK_RN_msisdn`) are NULL.
        *   Rows with various data types (strings, numbers, dates) and NULL values in non-filtering columns.
        *   A sufficient volume of data to simulate production conditions.
    2.  Ensure both target tables (`sof$ta_rn_da_vda_tk` in Oracle and BigQuery) are empty before execution.
*   **Action:**
    1.  Execute the legacy KornShell script `k_ausd_bp_ta_rn_da_vda_tk.ksh` with a dummy `p_Stichtag` (e.g., '01012023') and other required parameters.
    2.  Execute the migrated Airflow DAG `d_ausd_bp_ta_rn_da_vda_tk`.
    3.  Extract the full content of `sof$ta_rn_da_vda_tk` from both Oracle and BigQuery.
*   **Pass/Fail Criterion:**
    The dataset extracted from Oracle's `sof$ta_rn_da_vda_tk` must be *identical* to the dataset extracted from BigQuery's `sof$ta_rn_da_vda_tk` after sorting by a unique key or all columns.

    ```python
    # Example using pytest and a hypothetical data comparison utility
    import pandas as pd
    from your_data_comparison_library import compare_dataframes_exact

    def test_output_parity_full_data_comparison(oracle_client, bigquery_client):
        # Setup: Assume source data is already loaded identically
        # Run legacy job (manual or via wrapper)
        # run_legacy_ksh_job()

        # Run migrated Airflow DAG (trigger via API or mock)
        # trigger_airflow_dag('d_ausd_bp_ta_rn_da_vda_tk')

        # Extract data from Oracle
        oracle_query = "SELECT CNTRCT_ID, DA_RN_MSISDN, DA_RN_STATUS, DA_RN_VALID_TO, VDA_RN_MSISDN, VDA_RN_STATUS, VDA_RN_VALID_TO, TK_RN_MSISDN, TK_RN_STATUS, TK_RN_VALID_TO FROM sof$ta_rn_da_vda_tk ORDER BY CNTRCT_ID, DA_RN_MSISDN"
        df_oracle = oracle_client.query_to_dataframe(oracle_query)

        # Extract data from BigQuery
        bq_query = "SELECT CNTRCT_ID, DA_RN_MSISDN, DA_RN_STATUS, DA_RN_VALID_TO, VDA_RN_MSISDN, VDA_RN_STATUS, VDA_RN_VALID_TO, TK_RN_MSISDN, TK_RN_STATUS, TK_RN_VALID_TO FROM `sof$ta_rn_da_vda_tk` ORDER BY CNTRCT_ID, DA_RN_MSISDN"
        df_bigquery = bigquery_client.query_to_dataframe(bq_query)

        # Compare dataframes
        assert compare_dataframes_exact(df_oracle, df_bigquery), "Output dataframes are not identical."
    ```

---

### Test Case 2: Transformation Correctness - `WHERE` Clause Logic

*   **Purpose:** To specifically validate that the `WHERE DA_RN_msisdn IS NOT NULL OR VDA_RN_msisdn IS NOT NULL OR TK_RN_msisdn IS NOT NULL` filtering logic is correctly applied in BigQuery, matching Oracle's behavior.
*   **Setup:**
    1.  Populate `sof$ta_rn_einzeln` in both Oracle and BigQuery with a small, controlled dataset covering all permutations of NULL/NOT NULL for the three `MSISDN` columns:
        *   `CNTRCT_ID = 1`: `DA_RN_msisdn` = '123', `VDA_RN_msisdn` = NULL, `TK_RN_msisdn` = NULL (should be included)
        *   `CNTRCT_ID = 2`: `DA_RN_msisdn` = NULL, `VDA_RN_msisdn` = '456', `TK_RN_msisdn` = NULL (should be included)
        *   `CNTRCT_ID = 3`: `DA_RN_msisdn` = NULL, `VDA_RN_msisdn` = NULL, `TK_RN_msisdn` = '789' (should be included)
        *   `CNTRCT_ID = 4`: `DA_RN_msisdn` = '111', `VDA_RN_msisdn` = '222', `TK_RN_msisdn` = NULL (should be included)
        *   `CNTRCT_ID = 5`: `DA_RN_msisdn` = NULL, `VDA_RN_msisdn` = '333', `TK_RN_msisdn` = '444' (should be included)
        *   `CNTRCT_ID = 6`: `DA_RN_msisdn` = '555', `VDA_RN_msisdn` = NULL, `TK_RN_msisdn` = '666' (should be included)
        *   `CNTRCT_ID = 7`: `DA_RN_msisdn` = '777', `VDA_RN_msisdn` = '888', `TK_RN_msisdn` = '999' (should be included)
        *   `CNTRCT_ID = 8`: `DA_RN_msisdn` = NULL, `VDA_RN_msisdn` = NULL, `TK_RN_msisdn` = NULL (should *not* be included)
    2.  Ensure both target tables (`sof$ta_rn_da_vda_tk`) are empty.
*   **Action:**
    1.  Execute the legacy KornShell script.
    2.  Execute the migrated Airflow DAG.
    3.  Query the `CNTRCT_ID`s present in the target table from both systems.
*   **Pass/Fail Criterion:**
    The set of `CNTRCT_ID`s in Oracle's `sof$ta_rn_da_vda_tk` must be identical to the set of `CNTRCT_ID`s in BigQuery's `sof$ta_rn_da_vda_tk`. Specifically, `CNTRCT_ID`s 1 through 7 should be present, and `CNTRCT_ID` 8 should be absent.

    ```sql
    -- SQL assertion for BigQuery (similar for Oracle)
    SELECT COUNT(DISTINCT CNTRCT_ID) FROM `sof$ta_rn_da_vda_tk`
    WHERE CNTRCT_ID = 8; -- Expected result: 0

    SELECT COUNT(DISTINCT CNTRCT_ID) FROM `sof$ta_rn_da_vda_tk`
    WHERE CNTRCT_ID IN (1,2,3,4,5,6,7); -- Expected result: 7
    ```

---

### Test Case 3: Data Type and NULL Handling for Non-Filtering Columns

*   **Purpose:** To ensure that all columns, especially those not directly involved in the `WHERE` clause, maintain their data types and NULL/non-NULL values correctly during migration. This covers `CNTRCT_ID`, `DA_RN_STATUS`, `DA_RN_VALID_TO`, etc.
*   **Setup:**
    1.  Populate `sof$ta_rn_einzeln` in both Oracle and BigQuery with rows that satisfy the `WHERE` clause, but contain diverse data types and NULL values across all other columns.
        *   Example: `CNTRCT_ID` as number/string, `STATUS` as string, `VALID_TO` as date/timestamp, with some of these being NULL.
    2.  Ensure both target tables (`sof$ta_rn_da_vda_tk`) are empty.
*   **Action:**
    1.  Execute the legacy KornShell script.
    2.  Execute the migrated Airflow DAG.
    3.  Select all columns from a few representative rows in `sof$ta_rn_da_vda_tk` from both systems and compare their values and types.
*   **Pass/Fail Criterion:**
    For each selected row, the values and their corresponding data types (e.g., `NULL` vs. empty string, date format, numeric precision) in BigQuery's `sof$ta_rn_da_vda_tk` must exactly match those in Oracle's `sof$ta_rn_da_vda_tk`.

    ```python
    # Example using pytest
    def test_data_type_and_null_handling(oracle_client, bigquery_client):
        # Setup: Load specific test data into sof$ta_rn_einzeln
        # ... (e.g., CNTRCT_ID=100, DA_RN_MSISDN='123', DA_RN_STATUS=NULL, DA_RN_VALID_TO='2023-01-01')
        # Run jobs

        oracle_result = oracle_client.query_one_row("SELECT CNTRCT_ID, DA_RN_STATUS, DA_RN_VALID_TO FROM sof$ta_rn_da_vda_tk WHERE CNTRCT_ID = 100")
        bq_result = bigquery_client.query_one_row("SELECT CNTRCT_ID, DA_RN_STATUS, DA_RN_VALID_TO FROM `sof$ta_rn_da_vda_tk` WHERE CNTRCT_ID = 100")

        assert oracle_result['CNTRCT_ID'] == bq_result['CNTRCT_ID']
        assert oracle_result['DA_RN_STATUS'] == bq_result['DA_RN_STATUS'] # Should handle NULLs correctly
        assert str(oracle_result['DA_RN_VALID_TO']) == str(bq_result['DA_RN_VALID_TO']) # Convert dates to string for comparison
    ```

---

### Test Case 4: Target Table Truncation

*   **Purpose:** To confirm that the `TRUNCATE TABLE` operation is correctly executed before the `INSERT` in both the legacy and migrated jobs, ensuring idempotency and preventing duplicate data.
*   **Setup:**
    1.  Pre-populate `sof$ta_rn_da_vda_tk` in both Oracle and BigQuery with a known set of "old" records (e.g., `CNTRCT_ID`s 900, 901, 902).
    2.  Populate `sof$ta_rn_einzeln` in both environments with a distinct set of "new" records that satisfy the `WHERE` clause (e.g., `CNTRCT_ID`s 1, 2, 3).
*   **Action:**
    1.  Execute the legacy KornShell script.
    2.  Execute the migrated Airflow DAG.
    3.  Query the `CNTRCT_ID`s present in the target table from both systems.
*   **Pass/Fail Criterion:**
    The target table `sof$ta_rn_da_vda_tk` in both Oracle and BigQuery must *only* contain the "new" records (e.g., `CNTRCT_ID`s 1, 2, 3). The "old" records (900, 901, 902) must be absent.

    ```sql
    -- SQL assertion for BigQuery (similar for Oracle)
    SELECT COUNT(*) FROM `sof$ta_rn_da_vda_tk` WHERE CNTRCT_ID IN (900, 901, 902); -- Expected result: 0
    SELECT COUNT(*) FROM `sof$ta_rn_da_vda_tk` WHERE CNTRCT_ID IN (1, 2, 3);     -- Expected result: 3
    ```

---

### Test Case 5: Row Count Verification

*   **Purpose:** To verify that the total number of rows inserted into the target table is consistent between the legacy and migrated systems. This also implicitly validates the `WHERE` clause and `TRUNCATE` operations.
*   **Setup:**
    1.  Populate `sof$ta_rn_einzeln` in both Oracle and BigQuery with a known number of rows (e.g., 1000 rows), where a specific subset (e.g., 750 rows) will satisfy the `WHERE` clause.
    2.  Ensure both target tables (`sof$ta_rn_da_vda_tk`) are empty.
*   **Action:**
    1.  Execute the legacy KornShell script.
    2.  Execute the migrated Airflow DAG.
    3.  Count the rows in `sof$ta_rn_da_vda_tk` from both systems.
    4.  (Optional for legacy) Extract the record count from the `tmpFile` generated by the ksh script.
    5.  (Optional for migrated) If Airflow captures the row count (e.g., via `BigQueryExecuteQueryOperator`'s `result_extractor` or a subsequent `BigQueryGetDataOperator`), retrieve this value.
*   **Pass/Fail Criterion:**
    The `COUNT(*)` from Oracle's `sof$ta_rn_da_vda_tk` must be identical to the `COUNT(*)` from BigQuery's `sof$ta_rn_da_vda_tk`. Both should equal the expected number of inserted rows (e.g., 750). If applicable, any captured record counts should also match.

    ```sql
    -- SQL assertion for BigQuery (similar for Oracle)
    SELECT COUNT(*) FROM `sof$ta_rn_da_vda_tk`; -- Expected result: 750
    ```

---

### Test Case 6: Schema and Column Assertions

*   **Purpose:** To ensure that the target BigQuery table `sof$ta_rn_da_vda_tk` has the correct schema (column names, data types, nullability) as derived from the Oracle source and the migration design.
*   **Setup:**
    1.  Identify the exact schema of the Oracle `sof$ta_rn_da_vda_tk` table (column names, data types, precision, nullability).
    2.  Ensure the BigQuery target table `sof$ta_rn_da_vda_tk` has been created (either manually or by the `CREATE_IF_NEEDED` disposition of the Airflow task).
*   **Action:**
    1.  Query the `INFORMATION_SCHEMA` in both Oracle and BigQuery to retrieve the schema definition for `sof$ta_rn_da_vda_tk`.
*   **Pass/Fail Criterion:**
    The BigQuery table's schema must match the Oracle table's schema in terms of:
    *   **Column Names:** All column names must be identical.
    *   **Data Types:** BigQuery data types must be compatible and functionally equivalent to Oracle's (e.g., `VARCHAR2(X)` -> `STRING`, `NUMBER` -> `INT64`/`BIGNUMERIC`/`FLOAT64`, `DATE`/`TIMESTAMP` -> `DATE`/`TIMESTAMP`).
    *   **Nullability:** Nullability constraints should be consistent where applicable (e.g., if a column was `NOT NULL` in Oracle and is expected to remain so, it should be `REQUIRED` in BigQuery).

    ```sql
    -- BigQuery SQL to check schema
    SELECT
        column_name,
        data_type,
        is_nullable
    FROM
        `your_project.your_dataset.INFORMATION_SCHEMA.COLUMNS`
    WHERE
        table_name = 'sof$ta_rn_da_vda_tk'
    ORDER BY
        ordinal_position;

    -- Oracle SQL to check schema (example, syntax may vary slightly)
    SELECT
        column_name,
        data_type,
        nullable
    FROM
        ALL_TAB_COLUMNS
    WHERE
        owner = 'YOUR_SCHEMA' AND table_name = 'SOF$TA_RN_DA_VDA_TK'
    ORDER BY
        column_id;
    ```

---

### Test Case 7: External System Replacements - Parameter Handling (Orchestration Layer)

*   **Purpose:** To confirm that the Airflow DAG correctly handles the absence of parameters that were validated by the legacy KornShell script but are not directly used by the core SQL logic. This validates the design decision to replace ksh parameter handling with Airflow's implicit context.
*   **Setup:**
    1.  Identify the parameters validated by the ksh script: `p_JobKennung`, `p_Stichtag`, `p_EintragsNr`.
    2.  Ensure the legacy ksh script's parameter validation (`pruefeParameterGesetzt`) is active.
*   **Action:**
    1.  Attempt to execute the legacy KornShell script *without* providing the required parameters (e.g., `k_ausd_bp_ta_rn_da_vda_tk.ksh`).
    2.  Trigger the migrated Airflow DAG `d_ausd_bp_ta_rn_da_vda_tk` *without* providing any `dag_run.conf` parameters.
*   **Pass/Fail Criterion:**
    1.  The legacy KornShell script must fail with an error message indicating missing parameters (e.g., "FEHLER: 0 E 193 Notwendiges Argument fehlt").
    2.  The migrated Airflow DAG must complete successfully, as the BigQuery SQL does not depend on these parameters. This confirms that the migration correctly identified these parameters as non-critical for the SQL transformation itself and that Airflow's default behavior is acceptable. If these parameters were critical for *other* parts of the DAG (e.g., dynamic table naming, logging), then the Airflow DAG would need explicit parameter handling and validation.

    ```python
    # Example pytest for Airflow DAG execution (mocking or actual trigger)
    from airflow.models.dagbag import DagBag
    from airflow.utils.state import State

    def test_airflow_dag_parameter_handling():
        dagbag = DagBag(dag_folder='path/to/your/dags', include_examples=False)
        dag = dagbag.get_dag('d_ausd_bp_ta_rn_da_vda_tk')

        # Simulate a DAG run without explicit parameters
        dr = dag.create_dagrun(
            run_id='test_run_no_params',
            start_date=dag.start_date,
            execution_date=dag.start_date,
            state=State.RUNNING,
            conf={} # No parameters passed
        )
        # Execute the task (this would typically be handled by Airflow scheduler)
        # For a unit test, you might mock the BigQuery operator or run a local test instance.
        # For integration, you'd trigger the DAG and check its status.

        # Assert that the DAG run completes successfully
        # (This requires checking Airflow UI/logs or mocking task execution)
        # For a real integration test, you'd poll the DAG run status.
        # assert dr.get_state() == State.SUCCESS
        # For this specific test, we're confirming it *doesn't fail due to missing parameters*
        # which is implicitly covered if the BQ task runs successfully.
    ```

---

### Test Case 8: Commented-out Code and `v_datum` Relevance (Risk Mitigation)

*   **Purpose:** To explicitly address the commented-out `sed`, `sort`, `join` logic and the `v_datum` variable from `isbert_schema.dwtk_meldungen` mentioned in the design document, ensuring that their non-migration is a conscious and validated decision.
*   **Setup:**
    1.  Review the original KornShell script for commented-out sections.
    2.  Review the Oracle SQL script for variables like `v_datum` that are defined but not used in the core `INSERT` statement.
*   **Action:**
    1.  Consult with business stakeholders and legacy system experts to confirm the current status and future requirements for the commented-out `sed`/`sort`/`join` operations on `cibasis_data*.dat` files.
    2.  Confirm with stakeholders that the `v_datum` variable (derived from `isbert_schema.dwtk_meldungen`) is indeed not a critical input for *this specific data transformation* and its absence in the BigQuery SQL is acceptable.
*   **Pass/Fail Criterion:**
    1.  **Commented-out Code:** Obtain explicit confirmation (e.g., in writing, meeting minutes) that the commented-out code is obsolete and does not need to be migrated. If there's a future requirement, a separate migration plan should be initiated.
    2.  **`v_datum`:** Obtain explicit confirmation that `v_datum` is not used by the `d_ausd_bp_ta_rn_da_vda_tk.sql` script and its omission from the BigQuery SQL is correct. If it were used, the BigQuery SQL would need to incorporate a lookup from the BigQuery equivalent of `isbert_schema.dwtk_meldungen`.

    *(This is a documentation and stakeholder engagement test, not a runnable code test.)*

---