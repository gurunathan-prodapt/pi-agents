As a senior data-migration QA engineer, I've analyzed the migration design for `k_ausd_v_ta_vertrag_tmp.ksh` to BigQuery Stored Procedures and Airflow. The core challenge is that the underlying SQL script (`d_ausd_v_ta_vertrag_tmp.sql`) is unanalyzed, meaning data transformation logic cannot be fully tested. My tests will focus on the orchestration, parameter handling, error management, and logging functionalities as defined in the migration design and the provided BigQuery Stored Procedure pseudocode.

**Key Assumptions for Testing:**
*   The provided BigQuery Stored Procedure `project.dataset.r_ausd_vertrag` accurately reflects the migrated control logic of the KornShell script, excluding any job registration/deactivation logic not present in the pseudocode.
*   The placeholder `project.dataset.d_ausd_v_ta_vertrag_tmp` procedure, as provided, is sufficient for testing its invocation and basic logging.
*   The `ta_vertrag_tmp` table will have an `entry_nr` column for filtering, as implied by the `WHERE entry_nr = p_EintragsNr` clause in the `r_ausd_vertrag` procedure.

---

## Migration Validation Tests for `k_ausd_v_ta_vertrag_tmp.ksh`

### 1. Test: Parameter Validation - Missing `p_JobKennung`

**Purpose:** Verify that the migrated BigQuery Stored Procedure `r_ausd_vertrag` correctly handles a missing or empty `p_JobKennung` parameter, mirroring the legacy script's error handling (`ErrNr=193`, `ErrArg="Jobkennung"`, `exit $ErrNr`). This tests transformation correctness for parameter parsing and error signaling.

**Setup:**
1.  Ensure the `project.dataset.error_log` table is empty before execution.
2.  The BigQuery Stored Procedure `project.dataset.r_ausd_vertrag` and `project.dataset.d_ausd_v_ta_vertrag_tmp` are deployed.

**Action:**
Attempt to execute the `r_ausd_vertrag` stored procedure with a `NULL` or empty string for `p_JobKennung` and a valid value for `p_EintragsNr`.

```sql
-- Attempt to call with missing p_JobKennung
CALL `project.dataset.r_ausd_vertrag`(NULL, 'TEST_ENTRY_001');
-- Or:
-- CALL `project.dataset.r_ausd_vertrag`('', 'TEST_ENTRY_001');
```

**Pass/Fail Criteria:**
*   **Pass:**
    *   The `CALL` statement fails with an error, specifically `SIGNAL SQLSTATE '45000'` with `MESSAGE_TEXT = 'Bitte ueber Rahmenscript aufrufen'`.
    *   The `project.dataset.error_log` table contains exactly one new row with:
        *   `module_name = 'r_ausd_vertrag'`
        *   `error_nr = 193`
        *   `error_arg = 'Jobkennung'`
        *   `message = 'Bitte ueber Rahmenscript aufrufen'`
*   **Fail:**
    *   The procedure executes successfully without error.
    *   The `error_log` table does not contain the expected error entry.
    *   The error message or code does not match the expected values.

### 2. Test: Parameter Validation - Missing `p_EintragsNr`

**Purpose:** Verify that the migrated BigQuery Stored Procedure `r_ausd_vertrag` correctly handles a missing or empty `p_EintragsNr` parameter, consistent with the legacy script's error handling (`ErrNr=193`, `ErrArg="EintragsNr"`, `exit $ErrNr`). This tests transformation correctness for parameter parsing and error signaling.

**Setup:**
1.  Ensure the `project.dataset.error_log` table is empty before execution.
2.  The BigQuery Stored Procedure `project.dataset.r_ausd_vertrag` and `project.dataset.d_ausd_v_ta_vertrag_tmp` are deployed.

**Action:**
Attempt to execute the `r_ausd_vertrag` stored procedure with a valid `p_JobKennung` and a `NULL` or empty string for `p_EintragsNr`.

```sql
-- Attempt to call with missing p_EintragsNr
CALL `project.dataset.r_ausd_vertrag`('TEST_JOB_001', NULL);
-- Or:
-- CALL `project.dataset.r_ausd_vertrag`('TEST_JOB_001', '');
```

**Pass/Fail Criteria:**
*   **Pass:**
    *   The `CALL` statement fails with an error, specifically `SIGNAL SQLSTATE '45000'` with `MESSAGE_TEXT = 'Bitte ueber Rahmenscript aufrufen'`.
    *   The `project.dataset.error_log` table contains exactly one new row with:
        *   `module_name = 'r_ausd_vertrag'`
        *   `error_nr = 193`
        *   `error_arg = 'EintragsNr'`
        *   `message = 'Bitte ueber Rahmenscript aufrufen'`
*   **Fail:**
    *   The procedure executes successfully without error.
    *   The `error_log` table does not contain the expected error entry.
    *   The error message or code does not match the expected values.

### 3. Test: Valid Parameters - Core Logic Invocation and Logging

**Purpose:** Verify that when provided with valid parameters, the `r_ausd_vertrag` procedure successfully:
1.  Calls the `d_ausd_v_ta_vertrag_tmp` stored procedure.
2.  Logs the job run details, including the correct record count from `ta_vertrag_tmp` based on `p_EintragsNr`.
This tests output parity (successful execution, log entries), transformation correctness (SQL script invocation, temporary file replacement with `COUNT(*)`), and data quality (correct record count).

**Setup:**
1.  Ensure `project.dataset.error_log` and `project.dataset.job_run_log` tables are empty.
2.  Populate `project.dataset.ta_vertrag_tmp` with test data. For example:
    ```sql
    TRUNCATE TABLE `project.dataset.ta_vertrag_tmp`;
    INSERT INTO `project.dataset.ta_vertrag_tmp` (entry_nr, job_kennung, vertrag_id, vertrag_datum, some_value, load_ts) VALUES
    ('VALID_ENTRY_001', 'JOB_A', 'V001', '2023-01-01', 100.50, CURRENT_TIMESTAMP()),
    ('VALID_ENTRY_001', 'JOB_A', 'V002', '2023-01-02', 200.75, CURRENT_TIMESTAMP()),
    ('VALID_ENTRY_002', 'JOB_B', 'V003', '2023-01-03', 300.00, CURRENT_TIMESTAMP());
    ```
3.  The expected record count for `p_EintragsNr = 'VALID_ENTRY_001'` is 2.

**Action:**
Execute the `r_ausd_vertrag` stored procedure with valid `p_JobKennung` and `p_EintragsNr`.

```sql
CALL `project.dataset.r_ausd_vertrag`('VALID_JOB_001', 'VALID_ENTRY_001');
```

**Pass/Fail Criteria:**
*   **Pass:**
    *   The `CALL` statement executes successfully without any errors.
    *   The `project.dataset.error_log` table remains empty.
    *   The `project.dataset.job_run_log` table contains exactly two new rows:
        *   One row from the `d_ausd_v_ta_vertrag_tmp` procedure call, with `tab_name = 'd_ausd_v_ta_vertrag_tmp_called'`.
        *   One row from the `r_ausd_vertrag` procedure itself, with:
            *   `job_kennung = 'VALID_JOB_001'`
            *   `eintrags_nr = 'VALID_ENTRY_001'`
            *   `tab_name = 'ta_vertrag_tmp'`
            *   `records = 2` (matching the count of rows in `ta_vertrag_tmp` for `entry_nr = 'VALID_ENTRY_001'`).
*   **Fail:**
    *   The procedure fails or logs an error.
    *   The `job_run_log` table does not contain the expected entries or the `records` count is incorrect.

### 4. Test: `ta_vertrag_tmp` Schema Validation

**Purpose:** Verify that the `ta_vertrag_tmp` table in BigQuery has the correct schema, including column names and data types, as specified in the migration design and DDL. This tests data quality and schema assertions.

**Setup:**
The `project.dataset.ta_vertrag_tmp` table has been created using the provided DDL.

**Action:**
Query BigQuery's `INFORMATION_SCHEMA.COLUMNS` to retrieve the schema of the `ta_vertrag_tmp` table.

```sql
SELECT
    column_name,
    data_type
FROM
    `project.dataset.INFORMATION_SCHEMA.COLUMNS`
WHERE
    table_name = 'ta_vertrag_tmp'
ORDER BY
    ordinal_position;
```

**Pass/Fail Criteria:**
*   **Pass:** The query result matches the expected schema:
    | column_name   | data_type |
    | :------------ | :-------- |
    | entry_nr      | STRING    |
    | job_kennung   | STRING    |
    | vertrag_id    | STRING    |
    | vertrag_datum | DATE      |
    | some_value    | NUMERIC   |
    | load_ts       | TIMESTAMP |
*   **Fail:** Any discrepancy in column names, data types, or order.

### 5. Test: `error_log` and `job_run_log` Schema Validation

**Purpose:** Verify that the `error_log` and `job_run_log` tables in BigQuery have the correct schemas, including column names and data types, as specified in the migration design and DDL. This tests data quality and schema assertions.

**Setup:**
The `project.dataset.error_log` and `project.dataset.job_run_log` tables have been created using their respective DDLs.

**Action:**
Query BigQuery's `INFORMATION_SCHEMA.COLUMNS` for both tables.

```sql
-- For error_log
SELECT
    column_name,
    data_type
FROM
    `project.dataset.INFORMATION_SCHEMA.COLUMNS`
WHERE
    table_name = 'error_log'
ORDER BY
    ordinal_position;

-- For job_run_log
SELECT
    column_name,
    data_type
FROM
    `project.dataset.INFORMATION_SCHEMA.COLUMNS`
WHERE
    table_name = 'job_run_log'
ORDER BY
    ordinal_position;
```

**Pass/Fail Criteria:**
*   **Pass:**
    *   `error_log` schema matches:
        | column_name | data_type |
        | :---------- | :-------- |
        | error_ts    | TIMESTAMP |
        | module_name | STRING    |
        | error_nr    | INT64     |
        | error_arg   | STRING    |
        | message     | STRING    |
    *   `job_run_log` schema matches:
        | column_name | data_type |
        | :---------- | :-------- |
        | run_ts      | TIMESTAMP |
        | job_kennung | STRING    |
        | eintrags_nr | STRING    |
        | tab_name    | STRING    |
        | records     | INT64     |
*   **Fail:** Any discrepancy in column names, data types, or order for either table.

### 6. Test: Airflow DAG Integration (End-to-End Execution)

**Purpose:** Verify that the Airflow DAG (`k_ausd_v_ta_vertrag_tmp_dag.py`) can successfully trigger the BigQuery Stored Procedure `r_ausd_vertrag` with parameters, and that the entire orchestration flow completes as expected, resulting in correct log entries. This tests external-system replacements (Airflow replacing shell execution) and output parity.

**Setup:**
1.  All BigQuery tables (`ta_vertrag_tmp`, `error_log`, `job_run_log`) and Stored Procedures (`r_ausd_vertrag`, `d_ausd_v_ta_vertrag_tmp`) are deployed in the target BigQuery project and dataset.
2.  The Airflow DAG `k_ausd_v_ta_vertrag_tmp_dag.py` is deployed to an Airflow environment (e.g., Cloud Composer).
3.  The Airflow environment has a `google_cloud_default` connection configured to access the BigQuery project.
4.  `project.dataset.ta_vertrag_tmp` is populated with test data (e.g., 2 rows for `entry_nr = 'DAG_ENTRY_001'`).
5.  `project.dataset.error_log` and `project.dataset.job_run_log` tables are empty.

**Action:**
Manually trigger the `k_ausd_v_ta_vertrag_tmp_dag` in Airflow, providing the following DAG run configuration parameters:

```json
{
  "p_job_kennung": "DAG_JOB_001",
  "p_eintrags_nr": "DAG_ENTRY_001"
}
```

**Pass/Fail Criteria:**
*   **Pass:**
    *   The Airflow DAG run completes successfully (all tasks green).
    *   The `project.dataset.error_log` table remains empty.
    *   The `project.dataset.job_run_log` table contains exactly two new rows:
        *   One row from the `d_ausd_v_ta_vertrag_tmp` procedure call, with `tab_name = 'd_ausd_v_ta_vertrag_tmp_called'`.
        *   One row from the `r_ausd_vertrag` procedure itself, with:
            *   `job_kennung = 'DAG_JOB_001'`
            *   `eintrags_nr = 'DAG_ENTRY_001'`
            *   `tab_name = 'ta_vertrag_tmp'`
            *   `records = 2` (matching the count of rows in `ta_vertrag_tmp` for `entry_nr = 'DAG_ENTRY_001'`).
*   **Fail:**
    *   The Airflow DAG run fails or encounters errors.
    *   The `job_run_log` table does not contain the expected entries or the `records` count is incorrect.
    *   The `error_log` table contains unexpected entries.

---
**Note on Unresolved Items / Risks:**
As highlighted in the migration design, the content of `d_ausd_v_ta_vertrag_tmp.sql` is unknown. The tests above validate the *orchestration* and *control flow* of the `ksh` script's migration. Once `d_ausd_v_ta_vertrag_tmp.sql` is analyzed and migrated to `project.dataset.d_ausd_v_ta_vertrag_tmp`, additional tests will be required to verify the data transformation logic within that specific stored procedure (e.g., joins, aggregations, filters, data type handling, NULL handling, and edge cases on the actual data).