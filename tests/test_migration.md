As a senior data-migration QA engineer, I've analyzed the provided migration design and generated code for `k_ausd_v_ta_inv_acc.ksh`. The migration involves re-platforming a KornShell orchestration script to an Airflow DAG and its invoked SQL to BigQuery.

The tests below cover output parity, transformation correctness, external system replacements, and data quality/schema assertions. A critical observation is that while the legacy script passes `p_JobKennung` and `p_EintragsNr` to `starteSQLSkript`, the migrated BigQuery SQL (`d_ausd_v_ta_inv_acc.sql`) does not appear to use these parameters. The Airflow DAG validates them, but they are not passed down to the SQL. This is a potential behavioral difference or an indication that the original SQL also didn't use them directly for the `INSERT` logic. This is highlighted in Test Case 9.

---

## Migration Validation Tests for `k_ausd_v_ta_inv_acc.ksh`

### Test Case 1: Successful Data Migration & Output Parity

*   **Purpose**: Verify that with valid inputs, the migrated job produces the exact same data in the target BigQuery table as the legacy job would produce in its target table, given identical source data. This implicitly covers transformation correctness (joins, column mapping, data types, NULL handling).
*   **Setup**:
    1.  **Legacy System**: Prepare a comprehensive set of representative source data in the Oracle tables (`sof_ta_inv_assign`, `sof_ta_inv_def`, `sof_ta_acc_ref`). This data should include various join scenarios (matches, no matches), NULL values in non-join columns, edge cases for data types, and potential duplicates that would result from the join logic. Execute the legacy `k_ausd_v_ta_inv_acc.ksh` script with valid parameters. Extract the final state of the legacy target table (`ta_inv_acc`) into a canonical format (e.g., CSV, JSON, or a temporary table in a comparison database).
    2.  **Migrated System**: Load the *exact same* source data into the corresponding BigQuery tables (`gcp-project-id.isrpt_isbert_prod.sof_ta_inv_assign`, `sof_ta_inv_def`, `sof_ta_acc_ref`).
*   **Action**: Trigger the Airflow DAG `k_ausd_v_ta_inv_acc_dag` with valid `p_job_kennung` and `p_eintrags_nr` (e.g., `{"p_job_kennung": "TEST_JOB", "p_eintrags_nr": "123"}`) via the Airflow UI or CLI.
*   **Pass/Fail Criterion**:
    1.  The Airflow DAG run completes successfully.
    2.  The row count of `gcp-project-id.isrpt_isbert_prod.sof_ta_inv_acc` in BigQuery matches the row count of the legacy `ta_inv_acc` table.
    3.  A deep comparison of the data in `gcp-project-id.isrpt_isbert_prod.sof_ta_inv_acc` with the legacy output shows identical content (all columns, all rows). This can be achieved by:
        *   Exporting both datasets to CSV and comparing them using a diff tool.
        *   Using a `MINUS` (or `EXCEPT DISTINCT` in BigQuery) query between the two datasets (after appropriate type casting/normalization if needed) to ensure no differences.

    ```sql
    -- Example BigQuery assertion for data parity (assuming legacy data is in a staging table `legacy_ta_inv_acc`)
    SELECT
        'Only in BigQuery' AS difference_type,
        *
    FROM
        `gcp-project-id.isrpt_isbert_prod.sof_ta_inv_acc`
    EXCEPT DISTINCT
    SELECT
        *
    FROM
        `gcp-project-id.isrpt_isbert_prod.legacy_ta_inv_acc` -- Staging table with legacy output

    UNION ALL

    SELECT
        'Only in Legacy' AS difference_type,
        *
    FROM
        `gcp-project-id.isrpt_isbert_prod.legacy_ta_inv_acc`
    EXCEPT DISTINCT
    SELECT
        *
    FROM
        `gcp-project-id.isrpt_isbert_prod.sof_ta_inv_acc`;

    -- PASS if the query returns 0 rows.
    ```

### Test Case 2: Parameter Validation - Missing `p_job_kennung`

*   **Purpose**: Verify that the DAG correctly handles a missing `p_job_kennung` parameter, failing the job and logging an appropriate error, mirroring the legacy script's behavior (exit code 193).
*   **Setup**: Ensure the target table is empty or contains known data that will be unaffected by a failed run.
*   **Action**: Trigger the Airflow DAG `k_ausd_v_ta_inv_acc_dag` with `p_eintrags_nr` provided (e.g., `{"p_eintrags_nr": "123"}`) but `p_job_kennung` omitted from the DAG run configuration.
*   **Pass/Fail Criterion**:
    1.  The Airflow DAG run fails.
    2.  The `parse_and_validate_parameters` task fails.
    3.  The Airflow logs for the `parse_and_validate_parameters` task contain messages indicating the missing parameter and the error code 193, e.g., "Missing or empty parameter: p_job_kennung" and "Error 193: Jobkennung parameter is missing or empty."
    4.  The `truncate_target_table` and `insert_data_task` tasks are not executed (they should be skipped or upstream failed).

    ```python
    # Example pytest for parameter validation (conceptual, requires Airflow testing framework)
    from airflow.models.dagrun import DagRun
    from airflow.utils.state import State
    from unittest.mock import MagicMock, patch
    from dags.k_ausd_v_ta_inv_acc_dag import _parse_and_validate_parameters
    from dags.utils.error_handling import log_error

    def test_missing_job_kennung_fails_validation():
        mock_ti = MagicMock()
        mock_dag_run = MagicMock(conf={"p_eintrags_nr": "123"}) # Missing p_job_kennung

        with patch('dags.k_ausd_v_ta_inv_acc_dag.log_error') as mock_log_error:
            with pytest.raises(ValueError, match="Missing or empty parameter: p_job_kennung"):
                _parse_and_validate_parameters(ti=mock_ti, dag_run=mock_dag_run)

            mock_log_error.assert_called_once_with(0, "E", 193, "Jobkennung parameter is missing or empty.")
            mock_ti.xcom_push.assert_not_called() # No parameters pushed on failure
    ```

### Test Case 3: Parameter Validation - Missing `p_eintrags_nr`

*   **Purpose**: Verify that the DAG correctly handles a missing `p_eintrags_nr` parameter, failing the job and logging an appropriate error, mirroring the legacy script's behavior (exit code 193).
*   **Setup**: Same as Test Case 2.
*   **Action**: Trigger the Airflow DAG `k_ausd_v_ta_inv_acc_dag` with `p_job_kennung` provided (e.g., `{"p_job_kennung": "TEST_JOB"}`) but `p_eintrags_nr` omitted from the DAG run configuration.
*   **Pass/Fail Criterion**:
    1.  The Airflow DAG run fails.
    2.  The `parse_and_validate_parameters` task fails.
    3.  The Airflow logs for the `parse_and_validate_parameters` task contain messages indicating the missing parameter and the error code 193, e.g., "Missing or empty parameter: p_eintrags_nr" and "Error 193: EintragsNr parameter is missing or empty."
    4.  The `truncate_target_table` and `insert_data_task` tasks are not executed.

    ```python
    # Example pytest for parameter validation (conceptual)
    # Similar to test_missing_job_kennung_fails_validation, but with p_eintrags_nr missing
    def test_missing_eintrags_nr_fails_validation():
        mock_ti = MagicMock()
        mock_dag_run = MagicMock(conf={"p_job_kennung": "TEST_JOB"}) # Missing p_eintrags_nr

        with patch('dags.k_ausd_v_ta_inv_acc_dag.log_error') as mock_log_error:
            with pytest.raises(ValueError, match="Missing or empty parameter: p_eintrags_nr"):
                _parse_and_validate_parameters(ti=mock_ti, dag_run=mock_dag_run)

            mock_log_error.assert_called_once_with(0, "E", 193, "EintragsNr parameter is missing or empty.")
            mock_ti.xcom_push.assert_not_called()
    ```

### Test Case 4: Empty Source Tables - No Data Inserted

*   **Purpose**: Verify that if all source tables are empty, the job runs successfully but inserts no records into the target table.
*   **Setup**: Ensure `gcp-project-id.isrpt_isbert_prod.sof_ta_inv_assign`, `sof_ta_inv_def`, and `sof_ta_acc_ref` are all empty in BigQuery.
*   **Action**: Trigger the Airflow DAG `k_ausd_v_ta_inv_acc_dag` with valid parameters.
*   **Pass/Fail Criterion**:
    1.  The Airflow DAG completes successfully.
    2.  The `insert_data_task` completes successfully.
    3.  A query to `SELECT COUNT(*) FROM gcp-project-id.isrpt_isbert_prod.sof_ta_inv_acc` returns 0.
    4.  The `log_record_count` task logs "---------- ENDE Datenverarbeitung ----------" and the placeholder message for record count.

    ```sql
    -- BigQuery assertion
    SELECT COUNT(*) FROM `gcp-project-id.isrpt_isbert_prod.sof_ta_inv_acc`;
    -- PASS if result is 0.
    ```

### Test Case 5: No Matching Records - No Data Inserted

*   **Purpose**: Verify that if source tables contain data but no records satisfy the join conditions, the job runs successfully but inserts no records.
*   **Setup**: Populate `gcp-project-id.isrpt_isbert_prod.sof_ta_inv_assign`, `sof_ta_inv_def`, and `sof_ta_acc_ref` with data such that no combination of rows satisfies `ia.inv_definition_id = id.inv_definition_id` AND `id.acc_ref_id = ar.acc_ref_id`. For example, use disjoint sets of IDs across the tables (e.g., `inv_definition_id` in `sof_ta_inv_assign` never matches `inv_definition_id` in `sof_ta_inv_def`).
*   **Action**: Trigger the Airflow DAG `k_ausd_v_ta_inv_acc_dag` with valid parameters.
*   **Pass/Fail Criterion**:
    1.  The Airflow DAG completes successfully.
    2.  The `insert_data_task` completes successfully.
    3.  A query to `SELECT COUNT(*) FROM gcp-project-id.isrpt_isbert_prod.sof_ta_inv_acc` returns 0.
    4.  The `log_record_count` task logs "---------- ENDE Datenverarbeitung ----------" and the placeholder message for record count.

    ```sql
    -- BigQuery assertion
    SELECT COUNT(*) FROM `gcp-project-id.isrpt_isbert_prod.sof_ta_inv_acc`;
    -- PASS if result is 0.
    ```

### Test Case 6: Schema and Data Type Integrity

*   **Purpose**: Verify that the target BigQuery table `sof_ta_inv_acc` has the correct schema (column names, data types, nullability) as inferred from the source SQL and BigQuery's type system.
*   **Setup**: Ensure the target table is created (implicitly by the first run or explicitly).
*   **Action**: Run the DAG with valid parameters and some data. After successful completion, query the schema of `sof_ta_inv_acc` in BigQuery.
*   **Pass/Fail Criterion**: The schema of `gcp-project-id.isrpt_isbert_prod.sof_ta_inv_acc` matches the expected schema.
    *   `cntrct_id`: `INTEGER` (or `INT64`)
    *   `inv_definition_id`: `INTEGER` (or `INT64`)
    *   `inv_pay_ty_cv`: `STRING`
    *   `inv_media_cv`: `STRING`
    *   `billcycle_id`: `INTEGER` (or `INT64`)
    *   `sales_tax_freed`: `BOOLEAN` (if source is 0/1, or `INT64`/`STRING` if not converted)
    *   `account_reference`: `STRING`
    *   `rechn_inh_konfig_text`: `STRING`
    *   Nullability should also be checked based on source column nullability and join behavior. For example, if `cntrct_id` is `NOT NULL` in the source `sof_ta_inv_assign`, it should ideally be `NOT NULL` in the target `sof_ta_inv_acc` (though BigQuery doesn't enforce `NOT NULL` at the schema level, it's a data quality expectation).

    ```sql
    -- BigQuery assertion (conceptual, using INFORMATION_SCHEMA)
    SELECT
        column_name,
        data_type,
        is_nullable
    FROM
        `gcp-project-id.isrpt_isbert_prod.INFORMATION_SCHEMA.COLUMNS`
    WHERE
        table_name = 'sof_ta_inv_acc'
    ORDER BY
        ordinal_position;

    -- PASS if the returned schema matches the expected column names, data types, and nullability.
    ```

### Test Case 7: Orchestration - Truncate Behavior

*   **Purpose**: Verify that the `truncate_target_table` task correctly clears the target table before new data is inserted, ensuring a full refresh behavior as implied by the design.
*   **Setup**:
    1.  Populate `gcp-project-id.isrpt_isbert_prod.sof_ta_inv_acc` with some dummy data that is *not* expected to be generated by the `insert_data_task` (e.g., `INSERT INTO ... VALUES (999, 999, 'DUMMY', 'DUMMY', 999, FALSE, 'DUMMY_ACC', 'DUMMY_TEXT')`).
    2.  Populate source tables (`sof_ta_inv_assign`, `sof_ta_inv_def`, `sof_ta_acc_ref`) with data that *will* result in new records being inserted (e.g., 5 records).
*   **Action**: Trigger the Airflow DAG `k_ausd_v_ta_inv_acc_dag` with valid parameters.
*   **Pass/Fail Criterion**:
    1.  The Airflow DAG completes successfully.
    2.  The `truncate_target_table` task completes successfully.
    3.  The final `sof_ta_inv_acc` table contains *only* the data inserted by `insert_data_task`, and *none* of the dummy data from the setup. The row count should match the expected output from the `insert_data_task` alone (e.g., 5 records, not 5 + 1 dummy record).

    ```sql
    -- BigQuery assertion
    SELECT COUNT(*) FROM `gcp-project-id.isrpt_isbert_prod.sof_ta_inv_acc`;
    -- PASS if the count matches the expected number of records from the insert_data_task,
    -- and a SELECT * query confirms the dummy data is gone.
    ```

### Test Case 8: External System Replacement - Record Count Logging

*   **Purpose**: Verify that the mechanism for reporting record counts (replacing the temporary file) correctly logs the designated placeholder message, acknowledging the current design's limitation.
*   **Setup**: Run the DAG with valid parameters and some data that will be inserted.
*   **Action**: Observe the Airflow logs for the `log_record_count` task.
*   **Pass/Fail Criterion**:
    1.  The log for `log_record_count` contains "---------- ENDE Datenverarbeitung ----------".
    2.  The log for `log_record_count` contains "Processed records: [Count not directly available from INSERT statement, check BigQuery job statistics for affected rows or add a COUNT task]".
    *This test explicitly validates the current design's approach to record count reporting. If a future design change implements actual row count retrieval (e.g., via BigQuery job statistics or a subsequent `COUNT(*)` query), this test would need to be updated.*

### Test Case 9: Parameter Usage in SQL (Critical Check)

*   **Purpose**: Verify that the `p_job_kennung` and `p_eintrags_nr` parameters, which are validated by the DAG, are *not* used in the BigQuery SQL. This is a critical check because the design document states "pass p_EintragsNr, p_JobKennung as variables to the SQL script" but the generated SQL does not use them.
*   **Setup**: None.
*   **Action**: Review the `insert_data_task`'s SQL string within `dags/k_ausd_v_ta_inv_acc_dag.py`.
*   **Pass/Fail Criterion**: The SQL string within `insert_data_task` should *not* contain references to `p_job_kennung` or `p_eintrags_nr` (e.g., `{{ params.p_job_kennung }}` or `{{ ti.xcom_pull(key='p_job_kennung') }}`).
    *   **If it does contain them**: This indicates a discrepancy with the provided `d_ausd_v_ta_inv_acc.sql` and requires further investigation to determine if the parameters *should* be used in the SQL and if the SQL needs modification. This would be a **FAIL** against the provided SQL.
    *   **If it does NOT contain them (as per the provided code)**: This test passes, confirming the current behavior. However, it highlights a potential gap between the design document's intent ("pass ... as variables to the SQL script") and the actual implementation. This should be documented as an accepted deviation or a future enhancement.

    ```python
    # Example pytest for checking SQL content
    import pytest
    from dags.k_ausd_v_ta_inv_acc_dag import insert_data_task

    def test_sql_does_not_use_job_parameters():
        sql_content = insert_data_task.sql
        assert "p_job_kennung" not in sql_content.lower()
        assert "p_eintrags_nr" not in sql_content.lower()
        # PASS if both assertions are true.
        # If they were found, this test would fail, indicating a discrepancy.
    ```