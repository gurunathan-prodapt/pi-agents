The migration of `r_ausd_bp_ta_bpr_instance.ksh` to a BigQuery Stored Procedure involves significant changes in orchestration, logging, and data processing. The core data transformation logic, originally in `k_ausd_bp_ta_bpr_instance.ksh`, is now embedded within the BigQuery `MERGE` statement. The tests below focus on validating the behavior of the BigQuery Stored Procedure against the described legacy logic and the provided migration design.

**Important Note on Kernel Script Logic:**
The migration design explicitly states that the content and complexity of `k_ausd_bp_ta_bpr_instance.ksh` are currently unknown, and the `MERGE` statement in the BigQuery Stored Procedure is a placeholder. The tests for transformation correctness (Tests 6-10) are based on the *assumed* `MERGE` logic provided in the design document. A thorough analysis of `k_ausd_bp_ta_bpr_instance.ksh` is critical to refine these tests and ensure complete behavioral equivalence.

---

## Migration Validation Tests for `project.dataset.ausd_bp_ta_bpr_instance`

### Test 1: Default Parameter Handling (No Stichtag, No Wiederanlaufwert)

*   **Purpose:** Verify that when no `Stichtag` or `Wiederanlaufwert` is provided, the procedure correctly defaults `Stichtag` to the current system date and `Wiederanlaufwert` to 0, and executes the data processing.
*   **Setup:**
    1.  Ensure `project.dataset.source_contract_cache` contains diverse test data, including records that would be selected by `CURRENT_DATE()` as `Stichtag` and some that wouldn't.
    2.  `project.dataset.target_table` should be empty or contain some initial data for `MERGE` to operate on.
    3.  `project.dataset.job_log` and `project.dataset.job_error_log` should be empty.
*   **Action:**
    Execute the BigQuery Stored Procedure without any parameters:
    ```sql
    CALL `project.dataset.ausd_bp_ta_bpr_instance`(NULL, NULL);
    ```
*   **Pass/Fail Criterion:**
    *   The procedure completes successfully without raising an error.
    *   `project.dataset.job_log` contains three entries for the run:
        *   One `START` entry with `stichtag` set to `CURRENT_DATE()` and `wiederanlaufWert` as 0.
        *   One `INFO` entry (if `wiederanlaufWert` was > 0, which it isn't here).
        *   One `SUCCESS` entry.
    *   `project.dataset.job_error_log` is empty.
    *   `project.dataset.target_table` contains data resulting from the `MERGE` operation, filtered by `CURRENT_DATE()` as `Stichtag` and no `DWH_VERTRAG_ID` restart filter. The number of rows and their content should match the expected outcome if `CURRENT_DATE()` was explicitly passed as `Stichtag` and `0` as `Wiederanlaufwert`.

### Test 2: Stichtag Provided, Wiederanlaufwert Defaulted

*   **Purpose:** Verify that the procedure correctly uses a provided `Stichtag` and defaults `Wiederanlaufwert` to 0.
*   **Setup:**
    1.  Populate `project.dataset.source_contract_cache` with data relevant to a specific `Stichtag` (e.g., '01012023').
    2.  `project.dataset.target_table` should be empty or contain initial data.
    3.  `project.dataset.job_log` and `project.dataset.job_error_log` should be empty.
*   **Action:**
    Execute the BigQuery Stored Procedure with a specific `Stichtag`:
    ```sql
    CALL `project.dataset.ausd_bp_ta_bpr_instance`('01012023', NULL);
    ```
*   **Pass/Fail Criterion:**
    *   The procedure completes successfully.
    *   `project.dataset.job_log` contains `START` and `SUCCESS` entries. The `START` entry's `stichtag` is '2023-01-01' and `wiederanlaufWert` is 0.
    *   `project.dataset.job_error_log` is empty.
    *   `project.dataset.target_table` contains data processed using '2023-01-01' as `Stichtag` and no `DWH_VERTRAG_ID` restart filter.

### Test 3: Wiederanlaufwert Provided, Stichtag Defaulted

*   **Purpose:** Verify that the procedure correctly uses a provided `Wiederanlaufwert` and defaults `Stichtag` to the current system date.
*   **Setup:**
    1.  Populate `project.dataset.source_contract_cache` with data, including `DWH_VERTRAG_ID` values both above and below the test `Wiederanlaufwert` (e.g., 1000).
    2.  `project.dataset.target_table` should contain some records with `DWH_VERTRAG_ID >= 1000` that would be deleted by the restart logic.
    3.  `project.dataset.job_log` and `project.dataset.job_error_log` should be empty.
*   **Action:**
    Execute the BigQuery Stored Procedure with a specific `Wiederanlaufwert`:
    ```sql
    CALL `project.dataset.ausd_bp_ta_bpr_instance`(NULL, 1000);
    ```
*   **Pass/Fail Criterion:**
    *   The procedure completes successfully.
    *   `project.dataset.job_log` contains `START`, `INFO` (for delete), and `SUCCESS` entries. The `START` entry's `stichtag` is `CURRENT_DATE()` and `wiederanlaufWert` is 1000.
    *   `project.dataset.job_error_log` is empty.
    *   `project.dataset.target_table` has records with `DWH_VERTRAG_ID >= 1000` deleted, and new/updated records based on `CURRENT_DATE()` as `Stichtag` and `DWH_VERTRAG_ID > 1000` filter applied to the `MERGE` source.

### Test 4: Both Stichtag and Wiederanlaufwert Provided

*   **Purpose:** Verify that the procedure correctly uses both provided `Stichtag` and `Wiederanlaufwert`.
*   **Setup:**
    1.  Populate `project.dataset.source_contract_cache` with data relevant to the chosen `Stichtag` ('15062023') and `Wiederanlaufwert` (e.g., 500).
    2.  `project.dataset.target_table` should contain records with `DWH_VERTRAG_ID >= 500` that would be deleted.
    3.  `project.dataset.job_log` and `project.dataset.job_error_log` should be empty.
*   **Action:**
    Execute the BigQuery Stored Procedure with both parameters:
    ```sql
    CALL `project.dataset.ausd_bp_ta_bpr_instance`('15062023', 500);
    ```
*   **Pass/Fail Criterion:**
    *   The procedure completes successfully.
    *   `project.dataset.job_log` contains `START`, `INFO` (for delete), and `SUCCESS` entries. The `START` entry's `stichtag` is '2023-06-15' and `wiederanlaufWert` is 500.
    *   `project.dataset.job_error_log` is empty.
    *   `project.dataset.target_table` reflects the deletion of records with `DWH_VERTRAG_ID >= 500` and the `MERGE` operation using '2023-06-15' as `Stichtag` and `DWH_VERTRAG_ID > 500` as a filter.

### Test 5: Restart Logic (DELETE) Correctness

*   **Purpose:** Verify that the `DELETE` statement for restart logic is executed only when `v_wiederanlaufWert > 0` and correctly removes records from `target_table`.
*   **Setup:**
    1.  `project.dataset.source_contract_cache` can be empty or contain data.
    2.  `project.dataset.target_table` contains:
        *   `DWH_VERTRAG_ID = 100`, `target_column_1 = 'A'`
        *   `DWH_VERTRAG_ID = 500`, `target_column_1 = 'B'`
        *   `DWH_VERTRAG_ID = 1000`, `target_column_1 = 'C'`
        *   `DWH_VERTRAG_ID = 1500`, `target_column_1 = 'D'`
    3.  `project.dataset.job_log` and `project.dataset.job_error_log` should be empty.
*   **Action:**
    Execute the procedure with `Wiederanlaufwert = 1000` and a dummy `Stichtag` (data processing is not the focus here):
    ```sql
    CALL `project.dataset.ausd_bp_ta_bpr_instance`('01012023', 1000);
    ```
*   **Pass/Fail Criterion:**
    *   The procedure completes successfully.
    *   `project.dataset.job_log` contains an `INFO` entry indicating deletion for `Wiederanlaufwert = 1000`.
    *   `project.dataset.target_table` should only contain records with `DWH_VERTRAG_ID < 1000`. Specifically, records with `DWH_VERTRAG_ID = 100` and `500` should remain, while `1000` and `1500` should be deleted.
    *   **SQL Assertion:**
        ```sql
        SELECT COUNT(*) FROM `project.dataset.target_table` WHERE DWH_VERTRAG_ID >= 1000; -- Should be 0
        SELECT COUNT(*) FROM `project.dataset.target_table` WHERE DWH_VERTRAG_ID < 1000;  -- Should be 2
        ```

### Test 6: Core Data Logic - Filtering (`Gueltig_von`, `Gueltig_bis`, `LADEDATUM`)

*   **Purpose:** Verify that the `MERGE` statement correctly applies the date-based filtering conditions: `src.Gueltig_von <= v_stichtag`, `v_stichtag < src.Gueltig_bis`, and `src.LADEDATUM < v_stichtag`.
*   **Setup:**
    1.  `project.dataset.source_contract_cache` contains records with various combinations of `Gueltig_von`, `Gueltig_bis`, and `LADEDATUM` relative to a chosen `Stichtag` (e.g., '01072023').
        *   Record A: `Gueltig_von = '2023-01-01'`, `Gueltig_bis = '2023-12-31'`, `LADEDATUM = '2023-06-01'` (Should be selected)
        *   Record B: `Gueltig_von = '2023-07-01'`, `Gueltig_bis = '2023-12-31'`, `LADEDATUM = '2023-06-01'` (Should be selected, `Gueltig_von <= Stichtag` is true)
        *   Record C: `Gueltig_von = '2023-01-01'`, `Gueltig_bis = '2023-07-01'`, `LADEDATUM = '2023-06-01'` (Should NOT be selected, `Stichtag < Gueltig_bis` is false)
        *   Record D: `Gueltig_von = '2023-01-01'`, `Gueltig_bis = '2023-12-31'`, `LADEDATUM = '2023-07-01'` (Should NOT be selected, `LADEDATUM < Stichtag` is false)
        *   Record E: `Gueltig_von = '2023-08-01'`, `Gueltig_bis = '2023-12-31'`, `LADEDATUM = '2023-06-01'` (Should NOT be selected, `Gueltig_von <= Stichtag` is false)
    2.  `project.dataset.target_table` is empty.
*   **Action:**
    Execute the procedure with `Stichtag = '01072023'` and `Wiederanlaufwert = 0`:
    ```sql
    CALL `project.dataset.ausd_bp_ta_bpr_instance`('01072023', 0);
    ```
*   **Pass/Fail Criterion:**
    *   The procedure completes successfully.
    *   `project.dataset.target_table` contains only records A and B.
    *   **SQL Assertion:**
        ```sql
        SELECT DWH_VERTRAG_ID FROM `project.dataset.target_table` ORDER BY DWH_VERTRAG_ID;
        -- Expected result: [A.DWH_VERTRAG_ID, B.DWH_VERTRAG_ID]
        ```

### Test 7: Core Data Logic - Wiederanlaufwert Filter in MERGE

*   **Purpose:** Verify that the `MERGE` statement correctly applies the `(v_wiederanlaufWert = 0 OR src.DWH_VERTRAG_ID > v_wiederanlaufWert)` condition.
*   **Setup:**
    1.  `project.dataset.source_contract_cache` contains records that would pass the date filters, with `DWH_VERTRAG_ID` values both above and below a test `Wiederanlaufwert` (e.g., 500).
        *   Record F: `DWH_VERTRAG_ID = 400` (Passes date filters)
        *   Record G: `DWH_VERTRAG_ID = 500` (Passes date filters)
        *   Record H: `DWH_VERTRAG_ID = 600` (Passes date filters)
    2.  `project.dataset.target_table` is empty.
*   **Action:**
    Execute the procedure with `Stichtag = '01072023'` (assuming all records pass date filters) and `Wiederanlaufwert = 500`:
    ```sql
    CALL `project.dataset.ausd_bp_ta_bpr_instance`('01072023', 500);
    ```
*   **Pass/Fail Criterion:**
    *   The procedure completes successfully.
    *   `project.dataset.target_table` should only contain records with `DWH_VERTRAG_ID > 500`. Specifically, only Record H should be present.
    *   **SQL Assertion:**
        ```sql
        SELECT DWH_VERTRAG_ID FROM `project.dataset.target_table` ORDER BY DWH_VERTRAG_ID;
        -- Expected result: [H.DWH_VERTRAG_ID]
        ```

### Test 8: Core Data Logic - MERGE `WHEN MATCHED` (Update)

*   **Purpose:** Verify that existing records in `target_table` are correctly updated by the `MERGE` statement when `DWH_VERTRAG_ID` matches.
*   **Setup:**
    1.  `project.dataset.source_contract_cache` contains:
        *   `DWH_VERTRAG_ID = 100`, `example_column_1 = 'NewValue1'`, `example_column_2 = 10` (and passes date filters for '01012023')
    2.  `project.dataset.target_table` contains:
        *   `DWH_VERTRAG_ID = 100`, `target_column_1 = 'OldValue1'`, `target_column_2 = 5`, `last_updated_at = '2022-01-01 00:00:00 UTC'`
    3.  `project.dataset.job_log` and `project.dataset.job_error_log` should be empty.
*   **Action:**
    Execute the procedure with `Stichtag = '01012023'` and `Wiederanlaufwert = 0`:
    ```sql
    CALL `project.dataset.ausd_bp_ta_bpr_instance`('01012023', 0);
    ```
*   **Pass/Fail Criterion:**
    *   The procedure completes successfully.
    *   `project.dataset.target_table` should have the record with `DWH_VERTRAG_ID = 100` updated:
        *   `target_column_1` should be 'NewValue1'.
        *   `target_column_2` should be 10.
        *   `last_updated_at` should be a recent timestamp.
    *   **SQL Assertion:**
        ```sql
        SELECT target_column_1, target_column_2 FROM `project.dataset.target_table` WHERE DWH_VERTRAG_ID = 100;
        -- Expected result: ('NewValue1', 10)
        ```

### Test 9: Core Data Logic - MERGE `WHEN NOT MATCHED` (Insert)

*   **Purpose:** Verify that new records from `source_contract_cache` are correctly inserted into `target_table` when `DWH_VERTRAG_ID` does not match.
*   **Setup:**
    1.  `project.dataset.source_contract_cache` contains:
        *   `DWH_VERTRAG_ID = 200`, `example_column_1 = 'NewInsert1'`, `example_column_2 = 20` (and passes date filters for '01012023')
    2.  `project.dataset.target_table` is empty.
    3.  `project.dataset.job_log` and `project.dataset.job_error_log` should be empty.
*   **Action:**
    Execute the procedure with `Stichtag = '01012023'` and `Wiederanlaufwert = 0`:
    ```sql
    CALL `project.dataset.ausd_bp_ta_bpr_instance`('01012023', 0);
    ```
*   **Pass/Fail Criterion:**
    *   The procedure completes successfully.
    *   `project.dataset.target_table` should contain a new record with `DWH_VERTRAG_ID = 200`:
        *   `target_column_1` should be 'NewInsert1'.
        *   `target_column_2` should be 20.
        *   `last_updated_at` should be a recent timestamp.
    *   **SQL Assertion:**
        ```sql
        SELECT target_column_1, target_column_2 FROM `project.dataset.target_table` WHERE DWH_VERTRAG_ID = 200;
        -- Expected result: ('NewInsert1', 20)
        ```

### Test 10: NULL Handling in Source Data

*   **Purpose:** Verify how the `MERGE` statement handles NULL values in critical date columns (`Gueltig_von`, `Gueltig_bis`, `LADEDATUM`) and other columns.
*   **Setup:**
    1.  `project.dataset.source_contract_cache` contains records with:
        *   Record I: `Gueltig_von = NULL` (and other dates pass for '01012023')
        *   Record J: `Gueltig_bis = NULL` (and other dates pass for '01012023')
        *   Record K: `LADEDATUM = NULL` (and other dates pass for '01012023')
        *   Record L: `example_column_1 = NULL`, `example_column_2 = NULL` (and all dates pass for '01012023')
    2.  `project.dataset.target_table` is empty.
*   **Action:**
    Execute the procedure with `Stichtag = '01012023'` and `Wiederanlaufwert = 0`:
    ```sql
    CALL `project.dataset.ausd_bp_ta_bpr_instance`('01012023', 0);
    ```
*   **Pass/Fail Criterion:**
    *   The procedure completes successfully.
    *   Records I, J, K should *not* be inserted into `target_table` because `NULL` values in date comparisons (`<=`, `<`) typically evaluate to `UNKNOWN` and thus `FALSE`.
    *   Record L *should* be inserted, and its `target_column_1` and `target_column_2` should be `NULL` in `target_table`, demonstrating correct NULL propagation.
    *   **SQL Assertion:**
        ```sql
        SELECT COUNT(*) FROM `project.dataset.target_table` WHERE DWH_VERTRAG_ID IN (I.ID, J.ID, K.ID); -- Should be 0
        SELECT target_column_1, target_column_2 FROM `project.dataset.target_table` WHERE DWH_VERTRAG_ID = L.ID;
        -- Expected result: (NULL, NULL)
        ```

### Test 11: Parameter Validation Error Handling (Invalid Stichtag)

*   **Purpose:** Verify that the procedure correctly handles invalid `Stichtag` format, logs the error, and terminates.
*   **Setup:**
    1.  `project.dataset.job_log` and `project.dataset.job_error_log` should be empty.
*   **Action:**
    Execute the BigQuery Stored Procedure with an invalid `Stichtag` format:
    ```sql
    CALL `project.dataset.ausd_bp_ta_bpr_instance`('2023-01-01', 0); -- Expected DDMMYYYY, provided YYYY-MM-DD
    ```
*   **Pass/Fail Criterion:**
    *   The procedure execution fails and raises an error (e.g., `SQLSTATE '45000'`).
    *   `project.dataset.job_log` contains a `START` entry and a `FAILURE` entry for the run, with `error_message` indicating a `Stichtag` parsing issue.
    *   `project.dataset.job_error_log` contains an entry with `job_name`, `error_nr = 193`, `error_arg = 'Stichtag'`, and `error_details` related to the invalid format.
    *   `project.dataset.target_table` remains unchanged.

### Test 12: SQL Execution Error Handling

*   **Purpose:** Verify that the procedure correctly catches and logs SQL errors during data processing (e.g., during `MERGE` or `DELETE`), updates the job log, and re-raises the error.
*   **Setup:**
    1.  Introduce a temporary, controlled error condition in the `MERGE` statement (e.g., by attempting to insert a string into an `INT64` column, or by violating a `NOT NULL` constraint if one exists on `target_table`). This might require temporarily modifying the stored procedure or the target table schema for the test.
    2.  `project.dataset.job_log` and `project.dataset.job_error_log` should be empty.
*   **Action:**
    Execute the procedure with valid parameters, triggering the controlled SQL error:
    ```sql
    -- Example: Assuming a temporary modification to the MERGE statement to cause an error
    -- e.g., INSERT (..., target_column_2) VALUES (..., 'not_an_int');
    CALL `project.dataset.ausd_bp_ta_bpr_instance`('01012023', 0);
    ```
*   **Pass/Fail Criterion:**
    *   The procedure execution fails and raises an error.
    *   `project.dataset.job_log` contains a `START` entry and a `FAILURE` entry for the run, with `error_message` reflecting the SQL error.
    *   `project.dataset.job_error_log` contains an entry with `job_name`, `error_nr = 192` (or a specific SQL error code), `error_arg = 'SQL_EXECUTION_ERROR'`, and `error_details` containing the BigQuery error message.
    *   `project.dataset.target_table` should be in a consistent state (either unchanged if the error occurred early, or partially updated/deleted if the error occurred mid-transaction, depending on BigQuery's transaction model for `MERGE` and `DELETE`).

### Test 13: Job Log Entries for Full Lifecycle

*   **Purpose:** Verify that `project.dataset.job_log` accurately records the job's lifecycle (START, INFO, SUCCESS/FAILURE) with correct details.
*   **Setup:**
    1.  `project.dataset.job_log` should be empty.
    2.  `project.dataset.source_contract_cache` and `target_table` are populated to allow a successful run with restart.
*   **Action:**
    Execute the procedure with `Stichtag = '01012023'` and `Wiederanlaufwert = 100`:
    ```sql
    CALL `project.dataset.ausd_bp_ta_bpr_instance`('01012023', 100);
    ```
*   **Pass/Fail Criterion:**
    *   The procedure completes successfully.
    *   `project.dataset.job_log` contains exactly three entries for this `entry_nr`:
        1.  `status = 'START'`, `stichtag = '2023-01-01'`, `error_message` contains `Wiederanlaufwert: 100`.
        2.  `status = 'INFO'`, `error_message` indicates deletion for `DWH_VERTRAG_ID >= 100`.
        3.  `status = 'SUCCESS'`, `error_message = 'Job completed successfully.'`.
    *   All entries should have the same `entry_nr`, `job_name`, `script_name`, and `stichtag`. `created_at` should be sequential.
    *   **SQL Assertion (Pytest example):**
        ```python
        def test_job_log_lifecycle(bigquery_client):
            # ... setup ...
            bigquery_client.query("CALL `project.dataset.ausd_bp_ta_bpr_instance`('01012023', 100);").result()

            logs = list(bigquery_client.query(f"""
                SELECT status, stichtag, error_message
                FROM `project.dataset.job_log`
                WHERE job_name = 'r_ausd_bp_ta_bpr_instance'
                ORDER BY created_at
            """).result())

            assert len(logs) == 3
            assert logs[0].status == 'START'
            assert logs[0].stichtag == date(2023, 1, 1)
            assert 'Wiederanlaufwert: 100' in logs[0].error_message

            assert logs[1].status == 'INFO'
            assert 'Deleted records from target_table with DWH_VERTRAG_ID >= 100' in logs[1].error_message

            assert logs[2].status == 'SUCCESS'
            assert logs[2].error_message == 'Job completed successfully.'
        ```

### Test 14: Data Quality - Schema and Type Assertions

*   **Purpose:** Verify that the `target_table` schema matches expectations, especially after the `MERGE` operation, ensuring data types are correct and `NOT NULL` constraints (if any) are respected.
*   **Setup:**
    1.  Ensure `project.dataset.source_contract_cache` contains data that would be inserted/updated into `target_table`.
    2.  `project.dataset.target_table` is empty.
*   **Action:**
    Execute the procedure with valid parameters to populate `target_table`:
    ```sql
    CALL `project.dataset.ausd_bp_ta_bpr_instance`('01012023', 0);
    ```
*   **Pass/Fail Criterion:**
    *   The procedure completes successfully.
    *   The schema of `project.dataset.target_table` matches the expected DDL (e.g., `DWH_VERTRAG_ID` is `INT64`, `target_column_1` is `STRING`, `target_column_2` is `NUMERIC`, `last_updated_at` is `TIMESTAMP`).
    *   No type conversion errors occurred during `MERGE`.
    *   **SQL Assertion (Pytest example for schema):**
        ```python
        def test_target_table_schema(bigquery_client):
            table = bigquery_client.get_table('project.dataset.target_table')
            schema_fields = {field.name: field.field_type for field in table.schema}

            expected_schema = {
                'DWH_VERTRAG_ID': 'INT64',
                'target_column_1': 'STRING',
                'target_column_2': 'NUMERIC',
                'last_updated_at': 'TIMESTAMP'
            }

            for field_name, field_type in expected_schema.items():
                assert field_name in schema_fields
                assert schema_fields[field_name] == field_type
        ```

### Test 15: Row Count Parity (Full Run)

*   **Purpose:** Verify that the total number of rows in `target_table` after a full run (`Wiederanlaufwert = 0`) matches the expected count based on the source data and filtering logic.
*   **Setup:**
    1.  Populate `project.dataset.source_contract_cache` with a known number of records (e.g., 100 records) that satisfy the date filters for a specific `Stichtag`.
    2.  `project.dataset.target_table` is empty.
*   **Action:**
    Execute the procedure with `Stichtag = '01012023'` and `Wiederanlaufwert = 0`:
    ```sql
    CALL `project.dataset.ausd_bp_ta_bpr_instance`('01012023', 0);
    ```
*   **Pass/Fail Criterion:**
    *   The procedure completes successfully.
    *   The count of rows in `project.dataset.target_table` equals the number of records in `source_contract_cache` that pass all `MERGE` source filters.
    *   **SQL Assertion:**
        ```sql
        SELECT COUNT(*) FROM `project.dataset.target_table`;
        -- Expected result: 100 (or whatever the expected count is based on source data and filters)
        ```

### Test 16: Row Count Parity (Restart Run)

*   **Purpose:** Verify that the total number of rows in `target_table` after a restart run (`Wiederanlaufwert > 0`) correctly reflects the deletion and subsequent `MERGE` operations.
*   **Setup:**
    1.  `project.dataset.source_contract_cache` contains 100 records that pass date filters.
    2.  `project.dataset.target_table` initially contains 100 records (from a previous full run).
    3.  Assume `Wiederanlaufwert = 50` will cause 50 records to be deleted from `target_table` and 50 new/updated records to be processed from `source_contract_cache` (where `DWH_VERTRAG_ID > 50`).
*   **Action:**
    Execute the procedure with `Stichtag = '01012023'` and `Wiederanlaufwert = 50`:
    ```sql
    CALL `project.dataset.ausd_bp_ta_bpr_instance`('01012023', 50);
    ```
*   **Pass/Fail Criterion:**
    *   The procedure completes successfully.
    *   The count of rows in `project.dataset.target_table` should be consistent with the `DELETE` operation (removing `DWH_VERTRAG_ID >= 50`) and the subsequent `MERGE` (inserting/updating `DWH_VERTRAG_ID > 50`). If the source data has 100 records, and 50 are deleted, and then 50 are re-inserted/updated, the final count might still be 100, but the *content* of the records with `DWH_VERTRAG_ID >= 50` should be updated. This test should focus on the *net change* or final state.
    *   **SQL Assertion:**
        ```sql
        -- Assuming 100 records initially, 50 deleted, 50 re-inserted/updated.
        -- The final count might still be 100 if all deleted records are re-inserted.
        SELECT COUNT(*) FROM `project.dataset.target_table`;
        -- Expected result: 100 (if all deleted records are re-inserted/updated)
        -- Or, if source data for DWH_VERTRAG_ID > 50 is less than 50, the count would be 50 + (count of source DWH_VERTRAG_ID > 50).
        -- This requires careful calculation based on specific test data.
        ```