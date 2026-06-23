The migration of `r_ausd_v_ta_discount.ksh` to BigQuery involves translating shell script orchestration, parameter handling, and logging into BigQuery stored procedures and tables. The tests below focus on validating this translation, ensuring the BigQuery implementation behaves equivalently to the legacy KornShell wrapper.

**Assumptions:**
*   `your_gcp_project_id` and `your_bq_dataset_id` are placeholders for your actual GCP project ID and BigQuery dataset ID.
*   The `k_ausd_v_ta_discount_proc` (core logic) is a placeholder and will be modified as needed to simulate success or failure for testing purposes.
*   Tests are designed to be executed sequentially, with cleanup (truncation) before each relevant test.

---

### Test Case 1: Successful Execution with Default Parameters

**Purpose:** Verify the `vertragsdatenabgleich_wrapper_proc` executes successfully with default parameters, correctly logs job start/end, and updates job status. This covers output parity for logging and status, and transformation correctness for control flow.

**Setup:**
1.  Ensure the `job_log` and `job_status` tables are empty.
2.  Ensure `k_ausd_v_ta_discount_proc` is configured to succeed (using the provided placeholder code).

```sql
-- Truncate tables before test
TRUNCATE TABLE your_gcp_project_id.your_bq_dataset_id.job_log;
TRUNCATE TABLE your_gcp_project_id.your_bq_dataset_id.job_status;

-- Ensure k_ausd_v_ta_discount_proc is in a successful state
CREATE OR REPLACE PROCEDURE your_gcp_project_id.your_bq_dataset_id.k_ausd_v_ta_discount_proc(
    IN p_job_kennung STRING,
    IN p_dw_eintrags_nr INT64,
    IN p_stichtag DATE
)
BEGIN
    CALL your_gcp_project_id.your_bq_dataset_id.dwmsg_erzeugeeintrag_proc(
        p_dw_eintrags_nr, p_job_kennung, 'k_ausd_v_ta_discount_proc', 'INFO',
        CONCAT('Core processing started for Stichtag: ', CAST(p_stichtag AS STRING))
    );
    CALL your_gcp_project_id.your_bq_dataset_id.dwmsg_erzeugeeintrag_proc(
        p_dw_eintrags_nr, p_job_kennung, 'k_ausd_v_ta_discount_proc', 'INFO',
        'Core processing completed successfully.'
    );
END;
```

**Action:**
Execute the wrapper procedure without any parameters.

```sql
CALL your_gcp_project_id.your_bq_dataset_id.vertragsdatenabgleich_wrapper_proc();
```

**Pass/Fail Criterion:**
1.  **`job_log` content:**
    *   Query `SELECT * FROM your_gcp_project_id.your_bq_dataset_id.job_log ORDER BY log_timestamp;`
    *   Expected: At least 8 `INFO` entries.
    *   Messages should include:
        *   `'*******************************************************************************'` (start and end)
        *   `'Vertragsdatenabgleich Version 1.0.0-BQ'`
        *   `'Job Kennung: <UUID>, Entry No: 1'`
        *   `'Start Time: <timestamp>'`
        *   `'Stichtag: <current_date>'`
        *   `'Log Identifier (Conceptual): job_log_<UUID>.log'`
        *   `'Core processing started for Stichtag: <current_date>'`
        *   `'Core processing completed successfully.'`
        *   `'Job completed successfully.'`
        *   `'Wrapper procedure finished for Job Kennung: <UUID>'`
    *   The `job_entry_nr` should be `1`.
    *   The `job_kennung` should be a valid UUID and consistent across all entries for this run.
    *   The `stichtag` in the log should match `CURRENT_DATE()`.
2.  **`job_status` content:**
    *   Query `SELECT * FROM your_gcp_project_id.your_bq_dataset_id.job_status;`
    *   Expected: Exactly one row.
    *   `job_entry_nr` should be `1`.
    *   `job_kennung` should match the UUID from `job_log`.
    *   `stichtag` should be `CURRENT_DATE()`.
    *   `status_code` should be `'OK'`.
    *   `status_text` should be `'Completed successfully'`.

```python
# Example pytest assertion (conceptual, requires BigQuery client)
def test_successful_execution_default_params(bq_client):
    bq_client.query("TRUNCATE TABLE your_gcp_project_id.your_bq_dataset_id.job_log;").result()
    bq_client.query("TRUNCATE TABLE your_gcp_project_id.your_bq_dataset_id.job_status;").result()
    # ... (ensure k_ausd_v_ta_discount_proc is set to succeed)

    bq_client.query("CALL your_gcp_project_id.your_bq_dataset_id.vertragsdatenabgleich_wrapper_proc();").result()

    job_log_results = list(bq_client.query("SELECT * FROM your_gcp_project_id.your_bq_dataset_id.job_log ORDER BY log_timestamp;").result())
    job_status_results = list(bq_client.query("SELECT * FROM your_gcp_project_id.your_bq_dataset_id.job_status;").result())

    assert len(job_log_results) >= 8
    assert job_log_results[0]['job_entry_nr'] == 1
    job_kennung = job_log_results[0]['job_kennung']
    assert all(row['job_kennung'] == job_kennung for row in job_log_results)
    assert any("Job completed successfully." in row['message'] for row in job_log_results)
    assert any(f"Stichtag: {datetime.date.today().isoformat()}" in row['message'] for row in job_log_results)

    assert len(job_status_results) == 1
    assert job_status_results[0]['job_entry_nr'] == 1
    assert job_status_results[0]['job_kennung'] == job_kennung
    assert job_status_results[0]['stichtag'] == datetime.date.today()
    assert job_status_results[0]['status_code'] == 'OK'
    assert job_status_results[0]['status_text'] == 'Completed successfully'
```

---

### Test Case 2: Successful Execution with Specific Stichtag Parameter

**Purpose:** Verify the wrapper procedure correctly accepts and uses the `p_stichtag` parameter, demonstrating transformation correctness for parameter handling.

**Setup:**
1.  Ensure the `job_log` and `job_status` tables are empty.
2.  Ensure `k_ausd_v_ta_discount_proc` is configured to succeed.

```sql
-- Truncate tables before test
TRUNCATE TABLE your_gcp_project_id.your_bq_dataset_id.job_log;
TRUNCATE TABLE your_gcp_project_id.your_bq_dataset_id.job_status;

-- Ensure k_ausd_v_ta_discount_proc is in a successful state (same as Test Case 1)
-- (Re-create if it was modified for other tests)
CREATE OR REPLACE PROCEDURE your_gcp_project_id.your_bq_dataset_id.k_ausd_v_ta_discount_proc(
    IN p_job_kennung STRING,
    IN p_dw_eintrags_nr INT64,
    IN p_stichtag DATE
)
BEGIN
    CALL your_gcp_project_id.your_bq_dataset_id.dwmsg_erzeugeeintrag_proc(
        p_dw_eintrags_nr, p_job_kennung, 'k_ausd_v_ta_discount_proc', 'INFO',
        CONCAT('Core processing started for Stichtag: ', CAST(p_stichtag AS STRING))
    );
    CALL your_gcp_project_id.your_bq_dataset_id.dwmsg_erzeugeeintrag_proc(
        p_dw_eintrags_nr, p_job_kennung, 'k_ausd_v_ta_discount_proc', 'INFO',
        'Core processing completed successfully.'
    );
END;
```

**Action:**
Execute the wrapper procedure with a specific `p_stichtag`.

```sql
CALL your_gcp_project_id.your_bq_dataset_id.vertragsdatenabgleich_wrapper_proc(p_stichtag => '2023-01-15');
```

**Pass/Fail Criterion:**
1.  **`job_log` content:**
    *   Query `SELECT * FROM your_gcp_project_id.your_bq_dataset_id.job_log WHERE message LIKE '%Stichtag%';`
    *   Expected: An `INFO` entry with `message` containing `'Stichtag: 2023-01-15'`.
    *   Expected: An `INFO` entry from `k_ausd_v_ta_discount_proc` with `message` containing `'Core processing started for Stichtag: 2023-01-15'`.
2.  **`job_status` content:**
    *   Query `SELECT * FROM your_gcp_project_id.your_bq_dataset_id.job_status;`
    *   Expected: `stichtag` column for the executed job is `'2023-01-15'`.
    *   `status_code` should be `'OK'`.

---

### Test Case 3: Help Parameter (`-h`) Behavior

**Purpose:** Verify that calling the wrapper with `p_help => TRUE` displays the usage message and exits without performing any job processing or logging, replicating the legacy script's `-h` behavior. This covers output parity and transformation correctness for parameter handling.

**Setup:**
1.  Ensure the `job_log` and `job_status` tables are empty.

```sql
-- Truncate tables before test
TRUNCATE TABLE your_gcp_project_id.your_bq_dataset_id.job_log;
TRUNCATE TABLE your_gcp_project_id.your_bq_dataset_id.job_status;
```

**Action:**
Execute the wrapper procedure with `p_help` set to `TRUE`.

```sql
CALL your_gcp_project_id.your_bq_dataset_id.vertragsdatenabgleich_wrapper_proc(p_help => TRUE);
```

**Pass/Fail Criterion:**
1.  **Query Result:** The query execution should return results containing the help messages.
    *   Expected: Rows like `'Usage: CALL your_gcp_project_id.your_bq_dataset_id.vertragsdatenabgleich_wrapper_proc(p_help => TRUE/FALSE, p_stichtag => 'YYYY-MM-DD');'` and `'  -h: Display this help message.'`.
2.  **`job_log` content:**
    *   Query `SELECT COUNT(*) FROM your_gcp_project_id.your_bq_dataset_id.job_log;`
    *   Expected: Count is `0`.
3.  **`job_status` content:**
    *   Query `SELECT COUNT(*) FROM your_gcp_project_id.your_bq_dataset_id.job_status;`
    *   Expected: Count is `0`.

---

### Test Case 4: Error Handling - Core Procedure Failure

**Purpose:** Verify that if the core processing procedure (`k_ausd_v_ta_discount_proc`) fails, the wrapper correctly catches the error, logs it, and updates the job status to 'ERROR'. This covers transformation correctness for error handling and external system replacements for logging/status.

**Setup:**
1.  Ensure the `job_log` and `job_status` tables are empty.
2.  **Modify `k_ausd_v_ta_discount_proc` to simulate an error:**

```sql
-- Truncate tables before test
TRUNCATE TABLE your_gcp_project_id.your_bq_dataset_id.job_log;
TRUNCATE TABLE your_gcp_project_id.your_bq_dataset_id.job_status;

-- Modify k_ausd_v_ta_discount_proc to raise an error
CREATE OR REPLACE PROCEDURE your_gcp_project_id.your_bq_dataset_id.k_ausd_v_ta_discount_proc(
    IN p_job_kennung STRING,
    IN p_dw_eintrags_nr INT64,
    IN p_stichtag DATE
)
BEGIN
    -- Simulate an error
    RAISE USING MESSAGE 'Simulated error in k_ausd_v_ta_discount_proc';
END;
```

**Action:**
Execute the wrapper procedure.

```sql
CALL your_gcp_project_id.your_bq_dataset_id.vertragsdatenabgleich_wrapper_proc();
```

**Pass/Fail Criterion:**
1.  **Procedure Execution:** The `vertragsdatenabgleich_wrapper_proc` call should result in an error being raised to the caller (due to the `RAISE` in `dwmsg_fehlerbehandlung_proc`). The error message should contain `'Job <UUID> failed with error: Simulated error in k_ausd_v_ta_discount_proc'`.
2.  **`job_log` content:**
    *   Query `SELECT * FROM your_gcp_project_id.your_bq_dataset_id.job_log WHERE log_level = 'ERROR';`
    *   Expected: One entry with `log_level = 'ERROR'` and `message` containing `'Job failed: Simulated error in k_ausd_v_ta_discount_proc'`.
3.  **`job_status` content:**
    *   Query `SELECT * FROM your_gcp_project_id.your_bq_dataset_id.job_status;`
    *   Expected: Exactly one row.
    *   `status_code` should be `'ERROR'`.
    *   `status_text` should be `'Simulated error in k_ausd_v_ta_discount_proc'`.
    *   The `job_entry_nr` and `job_kennung` are consistent across `job_log` and `job_status`.

---

### Test Case 5: `DWMSG_ErmittleNr` and `DWMSG_Logdateiname` Correctness (Multiple Runs)

**Purpose:** Verify that `job_entry_nr` increments correctly and `job_kennung` is unique across multiple runs, and `log_filename` is derived correctly. This ensures data quality and correct behavior of the utility procedures.

**Setup:**
1.  Ensure the `job_log` and `job_status` tables are empty.
2.  Ensure `k_ausd_v_ta_discount_proc` is configured to succeed.

```sql
-- Truncate tables before test
TRUNCATE TABLE your_gcp_project_id.your_bq_dataset_id.job_log;
TRUNCATE TABLE your_gcp_project_id.your_bq_dataset_id.job_status;

-- Ensure k_ausd_v_ta_discount_proc is in a successful state (same as Test Case 1)
-- (Re-create if it was modified for other tests)
CREATE OR REPLACE PROCEDURE your_gcp_project_id.your_bq_dataset_id.k_ausd_v_ta_discount_proc(
    IN p_job_kennung STRING,
    IN p_dw_eintrags_nr INT64,
    IN p_stichtag DATE
)
BEGIN
    CALL your_gcp_project_id.your_bq_dataset_id.dwmsg_erzeugeeintrag_proc(
        p_dw_eintrags_nr, p_job_kennung, 'k_ausd_v_ta_discount_proc', 'INFO',
        CONCAT('Core processing started for Stichtag: ', CAST(p_stichtag AS STRING))
    );
    CALL your_gcp_project_id.your_bq_dataset_id.dwmsg_erzeugeeintrag_proc(
        p_dw_eintrags_nr, p_job_kennung, 'k_ausd_v_ta_discount_proc', 'INFO',
        'Core processing completed successfully.'
    );
END;
```

**Action:**
Execute the wrapper procedure twice.

```sql
CALL your_gcp_project_id.your_bq_dataset_id.vertragsdatenabgleich_wrapper_proc();
CALL your_gcp_project_id.your_bq_dataset_id.vertragsdatenabgleich_wrapper_proc();
```

**Pass/Fail Criterion:**
1.  **`job_log` content:**
    *   Query `SELECT job_entry_nr, job_kennung, message FROM your_gcp_project_id.your_bq_dataset_id.job_log WHERE message LIKE 'Job Kennung:%' OR message LIKE 'Log Identifier%';`
    *   Expected: Two distinct `job_entry_nr` values (`1` and `2`).
    *   Expected: Two distinct `job_kennung` values (UUIDs).
    *   For each `job_entry_nr`, the `job_kennung` in the `'Job Kennung:'` message should match the `job_kennung` used in the `'Log Identifier:'` message (e.g., `job_log_<job_kennung>.log`).
2.  **`job_status` content:**
    *   Query `SELECT job_entry_nr, job_kennung FROM your_gcp_project_id.your_bq_dataset_id.job_status;`
    *   Expected: Two distinct rows, one for `job_entry_nr = 1` and one for `job_entry_nr = 2`, each with a unique `job_kennung`.
    *   Both `status_code` should be `'OK'`.

---

### Test Case 6: Schema and Data Type Validation for Logging Tables

**Purpose:** Verify that the `job_log` and `job_status` tables have the correct schema, data types, and nullability constraints as defined in the migration design. This is a data quality and schema assertion.

**Setup:**
No specific setup beyond ensuring the tables exist.

**Action:**
Query BigQuery's `INFORMATION_SCHEMA` for table and column details.

```sql
SELECT
    column_name,
    data_type,
    is_nullable
FROM
    your_gcp_project_id.your_bq_dataset_id.INFORMATION_SCHEMA.COLUMNS
WHERE
    table_name = 'job_log'
ORDER BY
    ordinal_position;

SELECT
    column_name,
    data_type,
    is_nullable
FROM
    your_gcp_project_id.your_bq_dataset_id.INFORMATION_SCHEMA.COLUMNS
WHERE
    table_name = 'job_status'
ORDER BY
    ordinal_position;
```

**Pass/Fail Criterion:**
1.  **`job_log` table schema:**
    *   `job_entry_nr`: `INT64`, `NO` (NOT NULL)
    *   `job_kennung`: `STRING`, `NO` (NOT NULL)
    *   `script_name`: `STRING`, `YES` (NULLABLE)
    *   `log_timestamp`: `TIMESTAMP`, `NO` (NOT NULL)
    *   `log_level`: `STRING`, `YES` (NULLABLE)
    *   `message`: `STRING`, `YES` (NULLABLE)
2.  **`job_status` table schema:**
    *   `job_entry_nr`: `INT64`, `NO` (NOT NULL)
    *   `job_kennung`: `STRING`, `NO` (NOT NULL)
    *   `stichtag`: `DATE`, `NO` (NOT NULL)
    *   `status_code`: `STRING`, `NO` (NOT NULL)
    *   `status_text`: `STRING`, `YES` (NULLABLE)
    *   `last_updated`: `TIMESTAMP`, `YES` (NULLABLE, as `DEFAULT CURRENT_TIMESTAMP()` makes it nullable if not explicitly set, but it will always be populated by the default).

---

### Test Case 7: `DWMSG_SetzeStichtagInfo` Update Behavior

**Purpose:** Verify that `dwmsg_setzestichtaginfo_proc` correctly updates an existing `job_status` entry if `job_entry_nr` and `job_kennung` match, demonstrating transformation correctness for merge/update logic.

**Setup:**
1.  Truncate `job_status`.
2.  Manually insert an initial `job_status` entry.

```sql
-- Truncate tables before test
TRUNCATE TABLE your_gcp_project_id.your_bq_dataset_id.job_status;

-- Insert an initial status entry
INSERT INTO your_gcp_project_id.your_bq_dataset_id.job_status (job_entry_nr, job_kennung, stichtag, status_code, status_text, last_updated)
VALUES (1, 'TEST_JOB_1', '2023-01-01', 'INITIAL', 'Initial status', '2023-01-01 00:00:00 UTC');
```

**Action:**
Call `dwmsg_setzestichtaginfo_proc` with the same `job_entry_nr` and `job_kennung` but different `stichtag`, `status_code`, and `status_text`.

```sql
CALL your_gcp_project_id.your_bq_dataset_id.dwmsg_setzestichtaginfo_proc(
    1, 'TEST_JOB_1', '2023-01-02', 'RUNNING', 'Job started'
);
```

**Pass/Fail Criterion:**
1.  **`job_status` content:**
    *   Query `SELECT * FROM your_gcp_project_id.your_bq_dataset_id.job_status;`
    *   Expected: Exactly one row.
    *   `job_entry_nr` should be `1`.
    *   `job_kennung` should be `'TEST_JOB_1'`.
    *   `stichtag` should be updated to `'2023-01-02'`.
    *   `status_code` should be updated to `'RUNNING'`.
    *   `status_text` should be updated to `'Job started'`.
    *   `last_updated` should be a recent timestamp (after '2023-01-01 00:00:00 UTC').

---