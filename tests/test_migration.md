As a senior data-migration QA engineer, I've analyzed the migration design for `r_ausd_bp_ta_cntrct_dist.ksh` to Google BigQuery Stored Procedures. The following test cases are designed to validate the behavioral equivalence of the migrated code, covering output parity, transformation correctness, external system replacements, and data quality assertions.

**Assumptions for Testing:**
*   BigQuery project and dataset (`my_project.my_dataset`) are configured.
*   The `job_log`, `job_status`, `dwh_vertrag_cache`, and `fos_tabelle` tables exist in `my_project.my_dataset`.
*   The `ausd_bp_ta_cntrct_dist_core` and `ausd_bp_ta_cntrct_dist_wrapper` stored procedures are deployed.
*   `fos_tabelle` has the same schema as `dwh_vertrag_cache` to accommodate `SELECT src.*`.
*   Test data for `dwh_vertrag_cache` is prepared as described in the setup for relevant tests.
*   All `CURRENT_DATE()` calls in BigQuery will resolve to a consistent date for a given test run (e.g., by mocking or running tests on a specific date). For these tests, we'll assume `CURRENT_DATE()` is `2023-07-01`.

---

### Test Case 1: Successful Full Load with Default Parameters

**Purpose:** Verify that the job runs successfully when no `Stichtag` or `Wiederanlaufwert` is provided, defaulting to the system date and a restart value of 0, respectively. This tests the default parameter handling and a basic successful execution flow.

**Setup:**
1.  Ensure `my_project.my_dataset.job_log` and `my_project.my_dataset.job_status` tables are empty.
2.  Populate `my_project.my_dataset.dwh_vertrag_cache` with diverse test data, including records that should and should not be selected based on the default `Stichtag` (e.g., `2023-07-01`) and `LADEDATUM < Stichtag` filter.
    ```sql
    -- Assume CURRENT_DATE() is '2023-07-01' for this test
    TRUNCATE TABLE `my_project.my_dataset.dwh_vertrag_cache`;
    INSERT INTO `my_project.my_dataset.dwh_vertrag_cache` (dwh_vertrag_id, gueltig_von, gueltig_bis, ladedatum, col1, col2) VALUES
    (100, '2023-01-01', '2023-12-31', '2023-06-30', 'A', 'X'), -- Valid
    (101, '2023-03-01', '2024-02-28', '2023-06-25', 'B', 'Y'), -- Valid
    (102, '2023-06-01', '2023-06-15', '2023-06-01', 'C', 'Z'), -- Invalid (Stichtag >= gueltig_bis)
    (103, '2023-07-01', '2023-07-31', '2023-06-30', 'D', 'W'), -- Valid
    (104, '2023-07-01', '2023-07-31', '2023-07-01', 'E', 'V'), -- Invalid (LADEDATUM not < Stichtag)
    (105, '2023-07-01', '2023-07-31', '2023-07-02', 'F', 'U'), -- Invalid (LADEDATUM not < Stichtag)
    (106, '2023-07-15', '2023-07-20', '2023-07-10', 'G', 'T'), -- Invalid (gueltig_von > Stichtag)
    (107, '2023-06-01', '2023-06-30', '2023-06-05', 'H', 'S'); -- Invalid (Stichtag >= gueltig_bis)
    TRUNCATE TABLE `my_project.my_dataset.fos_tabelle`;
    ```

**Action:**
Execute the wrapper stored procedure without any parameters.
```sql
CALL `my_project.my_dataset.ausd_bp_ta_cntrct_dist_wrapper`(NULL, NULL);
```

**Pass/Fail Criterion:**
1.  **Output Parity / Transformation Correctness:** The `my_project.my_dataset.fos_tabelle` should contain exactly 3 records: `dwh_vertrag_id` 100, 101, 103. The data in these records should be identical to the source.
    ```python
    # pytest assertion
    def test_full_load_default_parameters(bigquery_client):
        # ... setup ...
        bigquery_client.query("CALL `my_project.my_dataset.ausd_bp_ta_cntrct_dist_wrapper`(NULL, NULL)").result()

        result = bigquery_client.query("SELECT dwh_vertrag_id FROM `my_project.my_dataset.fos_tabelle` ORDER BY dwh_vertrag_id").result()
        assert [row.dwh_vertrag_id for row in result] == [100, 101, 103]

        # Verify full data parity for one record
        source_data = bigquery_client.query("SELECT * FROM `my_project.my_dataset.dwh_vertrag_cache` WHERE dwh_vertrag_id = 100").result().to_dataframe()
        target_data = bigquery_client.query("SELECT * FROM `my_project.my_dataset.fos_tabelle` WHERE dwh_vertrag_id = 100").result().to_dataframe()
        pd.testing.assert_frame_equal(source_data, target_data)
    ```
2.  **Logging & Status:**
    *   `my_project.my_dataset.job_log` should contain at least 3 `INFO` entries (Job started, Core procedure success, Job finished OK) and no `ERROR` entries for `job_name = 'ausd_bp_ta_cntrct_dist'`. The `stichtag` in logs should be `01072023` (or `CURRENT_DATE()` formatted).
    *   `my_project.my_dataset.job_status` should contain one entry for `job_name = 'ausd_bp_ta_cntrct_dist'` with `status = 'OK'`.
    ```python
    # pytest assertion
    def test_full_load_logging_status(bigquery_client):
        # ... setup and action ...
        log_entries = bigquery_client.query("SELECT log_level, message, stichtag FROM `my_project.my_dataset.job_log` WHERE job_name = 'ausd_bp_ta_cntrct_dist' ORDER BY created_at").result()
        log_messages = [row.message for row in log_entries]
        assert any("Job started" in msg for msg in log_messages)
        assert any("Die Abarbeitung wurde ohne erkennbare Fehler beendet" in msg for msg in log_messages)
        assert not any("ERROR" in row.log_level for row in log_entries)
        assert all(row.stichtag == '01072023' for row in log_entries if row.stichtag is not None)

        status_entry = bigquery_client.query("SELECT status FROM `my_project.my_dataset.job_status` WHERE job_name = 'ausd_bp_ta_cntrct_dist'").result().to_dataframe()
        assert status_entry['status'].iloc[0] == 'OK'
    ```

---

### Test Case 2: Specific Stichtag and Restart Value

**Purpose:** Validate that the job correctly processes data using explicitly provided `Stichtag` and `Wiederanlaufwert` parameters, including the restart logic (delete and insert based on `DWH_VERTRAG_ID`).

**Setup:**
1.  Ensure `my_project.my_dataset.job_log` and `my_project.my_dataset.job_status` tables are empty.
2.  Populate `my_project.my_dataset.dwh_vertrag_cache` with data.
    ```sql
    TRUNCATE TABLE `my_project.my_dataset.dwh_vertrag_cache`;
    INSERT INTO `my_project.my_dataset.dwh_vertrag_cache` (dwh_vertrag_id, gueltig_von, gueltig_bis, ladedatum, col1, col2) VALUES
    (100, '2023-01-01', '2023-12-31', '2023-06-30', 'A', 'X'),
    (101, '2023-03-01', '2024-02-28', '2023-06-25', 'B', 'Y'),
    (102, '2023-06-01', '2023-06-15', '2023-06-01', 'C', 'Z'),
    (103, '2023-07-01', '2023-07-31', '2023-06-30', 'D', 'W'),
    (104, '2023-07-01', '2023-07-31', '2023-07-01', 'E', 'V'),
    (105, '2023-07-01', '2023-07-31', '2023-07-02', 'F', 'U'),
    (108, '2023-07-01', '2023-07-31', '2023-06-25', 'I', 'R'), -- Valid for Stichtag 01072023
    (109, '2023-07-01', '2023-07-31', '2023-06-25', 'J', 'Q'), -- Valid for Stichtag 01072023
    (110, '2023-07-01', '2023-07-31', '2023-06-25', 'K', 'P'); -- Valid for Stichtag 01072023
    -- Pre-populate fos_tabelle to test deletion logic
    TRUNCATE TABLE `my_project.my_dataset.fos_tabelle`;
    INSERT INTO `my_project.my_dataset.fos_tabelle` (dwh_vertrag_id, gueltig_von, gueltig_bis, ladedatum, col1, col2) VALUES
    (100, '2023-01-01', '2023-12-31', '2023-06-30', 'A', 'X'),
    (101, '2023-03-01', '2024-02-28', '2023-06-25', 'B', 'Y'),
    (108, '2023-07-01', '2023-07-31', '2023-06-25', 'I', 'R'),
    (109, '2023-07-01', '2023-07-31', '2023-06-25', 'J', 'Q'),
    (110, '2023-07-01', '2023-07-31', '2023-06-25', 'K', 'P'),
    (111, '2023-07-01', '2023-07-31', '2023-06-25', 'L', 'O'); -- This record should be deleted by restart logic
    ```
    We will use `Stichtag = '01072023'` and `Wiederanlaufwert = 108`.

**Action:**
Execute the wrapper stored procedure with specific parameters.
```sql
CALL `my_project.my_dataset.ausd_bp_ta_cntrct_dist_wrapper`('01072023', 108);
```

**Pass/Fail Criterion:**
1.  **Output Parity / Transformation Correctness:**
    *   `my_project.my_dataset.fos_tabelle` should have records 100, 101 (from previous state, not deleted), and 109, 110 (inserted because `dwh_vertrag_id > 108`). Record 108 should *not* be present because the `DELETE` condition is `>= 108` and `INSERT` condition is `> 108`. Record 111 should be deleted.
    *   The final `fos_tabelle` should contain `dwh_vertrag_id`s: 100, 101, 109, 110.
    ```python
    # pytest assertion
    def test_specific_stichtag_restart_value(bigquery_client):
        # ... setup ...
        bigquery_client.query("CALL `my_project.my_dataset.ausd_bp_ta_cntrct_dist_wrapper`('01072023', 108)").result()

        result = bigquery_client.query("SELECT dwh_vertrag_id FROM `my_project.my_dataset.fos_tabelle` ORDER BY dwh_vertrag_id").result()
        assert [row.dwh_vertrag_id for row in result] == [100, 101, 109, 110]
    ```
2.  **Logging & Status:**
    *   `my_project.my_dataset.job_log` should contain `INFO` entries with `stichtag = '01072023'` and no `ERROR` entries.
    *   `my_project.my_dataset.job_status` should contain one entry with `status = 'OK'`.

---

### Test Case 3: Invalid Stichtag Format (Wrapper Error Handling)

**Purpose:** Verify that the wrapper procedure correctly handles an invalid `Stichtag` format, logs an error, and sets the job status to `FAILED`. This tests the `SIGNAL SQLSTATE` and `EXCEPTION WHEN ERROR` blocks.

**Setup:**
1.  Ensure `my_project.my_dataset.job_log` and `my_project.my_dataset.job_status` tables are empty.
2.  `my_project.my_dataset.dwh_vertrag_cache` and `my_project.my_dataset.fos_tabelle` can be empty or contain any data, as the error should occur before core logic execution.

**Action:**
Execute the wrapper stored procedure with an invalid `Stichtag` format.
```sql
CALL `my_project.my_dataset.ausd_bp_ta_cntrct_dist_wrapper`('2023-07-01', NULL);
```

**Pass/Fail Criterion:**
1.  **Error Handling:** The call should raise an error (e.g., `45000` SQLSTATE) indicating a problem with the `Stichtag` parameter or date parsing.
    ```python
    # pytest assertion
    import pytest
    def test_invalid_stichtag_format(bigquery_client):
        # ... setup ...
        with pytest.raises(Exception) as e:
            bigquery_client.query("CALL `my_project.my_dataset.ausd_bp_ta_cntrct_dist_wrapper`('2023-07-01', NULL)").result()
        assert "Invalid date format" in str(e.value) or "Failed to parse date" in str(e.value) # BigQuery error message for PARSE_DATE
    ```
2.  **Logging & Status:**
    *   `my_project.my_dataset.job_log` should contain an `ERROR` entry with a message related to the date parsing failure (e.g., `AppError: Abbruch - Invalid date format...`).
    *   `my_project.my_dataset.job_status` should contain one entry for `job_name = 'ausd_bp_ta_cntrct_dist'` with `status = 'FAILED'`.
    ```python
    # pytest assertion
    def test_invalid_stichtag_logging_status(bigquery_client):
        # ... setup and action (with try-except to catch the error) ...
        log_entries = bigquery_client.query("SELECT log_level, message FROM `my_project.my_dataset.job_log` WHERE job_name = 'ausd_bp_ta_cntrct_dist'").result()
        assert any("ERROR" in row.log_level and "AppError: Abbruch" in row.message for row in log_entries)

        status_entry = bigquery_client.query("SELECT status FROM `my_project.my_dataset.job_status` WHERE job_name = 'ausd_bp_ta_cntrct_dist'").result().to_dataframe()
        assert status_entry['status'].iloc[0] == 'FAILED'
    ```

---

### Test Case 4: Core Logic Failure (Exception Handling)

**Purpose:** Verify that if an error occurs within the `ausd_bp_ta_cntrct_dist_core` procedure (e.g., due to a data type mismatch or table not found), the wrapper procedure catches it, logs the error, and updates the job status to `FAILED`.

**Setup:**
1.  Ensure `my_project.my_dataset.job_log` and `my_project.my_dataset.job_status` tables are empty.
2.  Modify `my_project.my_dataset.ausd_bp_ta_cntrct_dist_core` temporarily to force an error (e.g., try to insert into a non-existent column or perform an invalid cast).
    ```sql
    -- Temporarily modify core SP to cause an error
    CREATE OR REPLACE PROCEDURE `my_project.my_dataset.ausd_bp_ta_cntrct_dist_core`(
      IN p_jobkennung STRING,
      IN p_stichtag STRING,
      IN p_eintragsnr INT64,
      IN p_wiederanlaufwert INT64
    )
    BEGIN
      -- Force an error: e.g., divide by zero
      SELECT 1 / 0;
    END;
    ```

**Action:**
Execute the wrapper stored procedure with valid parameters.
```sql
CALL `my_project.my_dataset.ausd_bp_ta_cntrct_dist_wrapper`('01072023', NULL);
```

**Pass/Fail Criterion:**
1.  **Error Handling:** The call should raise an error, and the error message should reflect the forced error from the core procedure.
    ```python
    # pytest assertion
    import pytest
    def test_core_logic_failure(bigquery_client):
        # ... setup (including temporary core SP modification) ...
        with pytest.raises(Exception) as e:
            bigquery_client.query("CALL `my_project.my_dataset.ausd_bp_ta_cntrct_dist_wrapper`('01072023', NULL)").result()
        assert "Division by zero" in str(e.value) # Or whatever error was forced
    ```
2.  **Logging & Status:**
    *   `my_project.my_dataset.job_log` should contain an `ERROR` entry with a message like `AppError: Abbruch - Division by zero`.
    *   `my_project.my_dataset.job_status` should contain one entry for `job_name = 'ausd_bp_ta_cntrct_dist'` with `status = 'FAILED'`.
    ```python
    # pytest assertion
    def test_core_logic_failure_logging_status(bigquery_client):
        # ... setup and action (with try-except) ...
        log_entries = bigquery_client.query("SELECT log_level, message FROM `my_project.my_dataset.job_log` WHERE job_name = 'ausd_bp_ta_cntrct_dist'").result()
        assert any("ERROR" in row.log_level and "AppError: Abbruch" in row.message for row in log_entries)

        status_entry = bigquery_client.query("SELECT status FROM `my_project.my_dataset.job_status` WHERE job_name = 'ausd_bp_ta_cntrct_dist'").result().to_dataframe()
        assert status_entry['status'].iloc[0] == 'FAILED'
    ```
**Cleanup:** Revert `my_project.my_dataset.ausd_bp_ta_cntrct_dist_core` to its original, correct implementation after this test.

---

### Test Case 5: Empty Source Table

**Purpose:** Verify that the job handles an empty source table gracefully, resulting in an empty target table and a successful job status.

**Setup:**
1.  Ensure `my_project.my_dataset.job_log` and `my_project.my_dataset.job_status` tables are empty.
2.  Ensure `my_project.my_dataset.dwh_vertrag_cache` is empty.
    ```sql
    TRUNCATE TABLE `my_project.my_dataset.dwh_vertrag_cache`;
    TRUNCATE TABLE `my_project.my_dataset.fos_tabelle`;
    ```

**Action:**
Execute the wrapper stored procedure with valid parameters.
```sql
CALL `my_project.my_dataset.ausd_bp_ta_cntrct_dist_wrapper`('01072023', NULL);
```

**Pass/Fail Criterion:**
1.  **Output Parity / Row Count:** `my_project.my_dataset.fos_tabelle` should remain empty.
    ```python
    # pytest assertion
    def test_empty_source_table(bigquery_client):
        # ... setup ...
        bigquery_client.query("CALL `my_project.my_dataset.ausd_bp_ta_cntrct_dist_wrapper`('01072023', NULL)").result()

        result = bigquery_client.query("SELECT COUNT(*) FROM `my_project.my_dataset.fos_tabelle`").result().to_dataframe()
        assert result['f0_'].iloc[0] == 0
    ```
2.  **Logging & Status:**
    *   `my_project.my_dataset.job_log` should contain `INFO` entries and no `ERROR` entries.
    *   `my_project.my_dataset.job_status` should contain one entry with `status = 'OK'`.

---

### Test Case 6: Date Filtering Logic (Transformation Correctness)

**Purpose:** Thoroughly test the date filtering conditions: `Gueltig_von <= Stichtag < Gueltig_bis` and `LADEDATUM < Stichtag`.

**Setup:**
1.  Ensure `my_project.my_dataset.job_log` and `my_project.my_dataset.job_status` tables are empty.
2.  Populate `my_project.my_dataset.dwh_vertrag_cache` with specific date scenarios.
    ```sql
    TRUNCATE TABLE `my_project.my_dataset.dwh_vertrag_cache`;
    INSERT INTO `my_project.my_dataset.dwh_vertrag_cache` (dwh_vertrag_id, gueltig_von, gueltig_bis, ladedatum, col1, col2) VALUES
    -- Stichtag = '01072023' (2023-07-01)
    (200, '2023-01-01', '2023-12-31', '2023-06-30', 'A', 'X'), -- Valid: G_von <= S < G_bis AND L < S
    (201, '2023-07-01', '2023-12-31', '2023-06-30', 'B', 'Y'), -- Valid: G_von = S, S < G_bis AND L < S
    (202, '2023-01-01', '2023-07-01', '2023-06-30', 'C', 'Z'), -- Invalid: S not < G_bis (S = G_bis)
    (203, '2023-07-02', '2023-12-31', '2023-06-30', 'D', 'W'), -- Invalid: G_von not <= S (G_von > S)
    (204, '2023-01-01', '2023-12-31', '2023-07-01', 'E', 'V'), -- Invalid: L not < S (L = S)
    (205, '2023-01-01', '2023-12-31', '2023-07-02', 'F', 'U'), -- Invalid: L not < S (L > S)
    (206, '2023-06-30', '2023-07-02', '2023-06-30', 'G', 'T'); -- Valid: G_von < S < G_bis AND L < S
    TRUNCATE TABLE `my_project.my_dataset.fos_tabelle`;
    ```

**Action:**
Execute the wrapper stored procedure with `Stichtag = '01072023'`.
```sql
CALL `my_project.my_dataset.ausd_bp_ta_cntrct_dist_wrapper`('01072023', NULL);
```

**Pass/Fail Criterion:**
1.  **Output Parity / Transformation Correctness:** `my_project.my_dataset.fos_tabelle` should contain exactly 3 records: `dwh_vertrag_id` 200, 201, 206.
    ```python
    # pytest assertion
    def test_date_filtering_logic(bigquery_client):
        # ... setup ...
        bigquery_client.query("CALL `my_project.my_dataset.ausd_bp_ta_cntrct_dist_wrapper`('01072023', NULL)").result()

        result = bigquery_client.query("SELECT dwh_vertrag_id FROM `my_project.my_dataset.fos_tabelle` ORDER BY dwh_vertrag_id").result()
        assert [row.dwh_vertrag_id for row in result] == [200, 201, 206]
    ```

---

### Test Case 7: `job_log` and `job_status` Schema Assertions

**Purpose:** Verify that the `job_log` and `job_status` tables have the expected schema and data types, ensuring external system replacements are correctly implemented.

**Setup:**
1.  Ensure `my_project.my_dataset.job_log` and `my_project.my_dataset.job_status` tables exist.
2.  Execute a successful run of the wrapper procedure to populate these tables.

**Action:**
Query the information schema for the tables.

**Pass/Fail Criterion:**
1.  **Schema Assertion:** The schema of `job_log` and `job_status` tables should match the DDL provided in the migration design.
    ```python
    # pytest assertion
    def test_job_log_schema(bigquery_client):
        table_id = "my_project.my_dataset.job_log"
        table = bigquery_client.get_table(table_id)
        expected_schema = [
            bigquery.SchemaField("entry_no", "INT64"),
            bigquery.SchemaField("job_name", "STRING"),
            bigquery.SchemaField("log_level", "STRING"),
            bigquery.SchemaField("message", "STRING"),
            bigquery.SchemaField("stichtag", "STRING"),
            bigquery.SchemaField("sysdate", "STRING"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
        ]
        assert table.schema == expected_schema

    def test_job_status_schema(bigquery_client):
        table_id = "my_project.my_dataset.job_status"
        table = bigquery_client.get_table(table_id)
        expected_schema = [
            bigquery.SchemaField("entry_no", "INT64"),
            bigquery.SchemaField("job_name", "STRING"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("updated_at", "TIMESTAMP"),
        ]
        assert table.schema == expected_schema
    ```

---