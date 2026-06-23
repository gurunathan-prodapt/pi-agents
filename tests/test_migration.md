As a senior data-migration QA engineer, I've designed a comprehensive suite of validation tests for the migrated BigQuery solution, `sp_k_ausd_bp_ta_cntrct_dist`. These tests aim to ensure behavioral equivalence with the legacy KornShell script `k_ausd_bp_ta_cntrct_dist.ksh`, covering parameter handling, core data transformation, date derivation, and audit logging.

The tests are structured with `purpose`, `setup`, `action`, and `pass/fail criterion` sections, and include runnable BigQuery SQL for clarity.

---

## Migration Validation Tests: `k_ausd_bp_ta_cntrct_dist.ksh` to `sp_k_ausd_bp_ta_cntrct_dist`

**Pre-requisites for all tests:**

1.  BigQuery project and dataset (`project.dataset`) are configured.
2.  The following DDLs have been executed to create the necessary tables:
    *   `ddl/sof_ta_cntrct_dist.sql`
    *   `ddl/job_audit_table.sql`
    *   A `project.dataset.sof_ta_bpr_basis` table exists with a `cntrct_id` column (e.g., `INT64`).
3.  The BigQuery Stored Procedure `bigquery_sp/sp_k_ausd_bp_ta_cntrct_dist.sql` has been deployed.

---

### Test Case 1: Parameter Validation - Missing `p_job_kennung`

**Purpose:** Verify that the stored procedure correctly identifies and rejects calls with a missing or empty `p_job_kennung` parameter, mimicking the legacy `pruefeParameterGesetzt` behavior.

**Setup:**
Ensure `sof_ta_cntrct_dist` and `job_audit_table` are empty.

```sql
TRUNCATE TABLE `project.dataset.sof_ta_cntrct_dist`;
TRUNCATE TABLE `project.dataset.job_audit_table`;
```

**Action:**
Attempt to execute the stored procedure without providing `p_job_kennung`.

```sql
-- This query is expected to fail.
CALL `project.dataset.sp_k_ausd_bp_ta_cntrct_dist`(
    p_job_kennung => '', -- Empty string, equivalent to missing
    p_eintrags_nr => '001',
    p_stichtag => '01012023',
    p_wiederanlauf_wert => NULL
);
```

**Pass/Fail Criterion:**
*   **Pass:** The `CALL` statement fails with an error message containing "Parameter p_job_kennung is missing or empty."
*   **Fail:** The `CALL` statement succeeds, or fails with a different error message.

---

### Test Case 2: Parameter Validation - Missing `p_eintrags_nr`

**Purpose:** Verify that the stored procedure correctly identifies and rejects calls with a missing or empty `p_eintrags_nr` parameter.

**Setup:**
Ensure `sof_ta_cntrct_dist` and `job_audit_table` are empty.

```sql
TRUNCATE TABLE `project.dataset.sof_ta_cntrct_dist`;
TRUNCATE TABLE `project.dataset.job_audit_table`;
```

**Action:**
Attempt to execute the stored procedure without providing `p_eintrags_nr`.

```sql
-- This query is expected to fail.
CALL `project.dataset.sp_k_ausd_bp_ta_cntrct_dist`(
    p_job_kennung => 'TEST_JOB',
    p_eintrags_nr => ' ', -- Whitespace, equivalent to empty
    p_stichtag => '01012023',
    p_wiederanlauf_wert => NULL
);
```

**Pass/Fail Criterion:**
*   **Pass:** The `CALL` statement fails with an error message containing "Parameter p_eintrags_nr is missing or empty."
*   **Fail:** The `CALL` statement succeeds, or fails with a different error message.

---

### Test Case 3: Parameter Validation - Missing `p_stichtag`

**Purpose:** Verify that the stored procedure correctly identifies and rejects calls with a missing or empty `p_stichtag` parameter.

**Setup:**
Ensure `sof_ta_cntrct_dist` and `job_audit_table` are empty.

```sql
TRUNCATE TABLE `project.dataset.sof_ta_cntrct_dist`;
TRUNCATE TABLE `project.dataset.job_audit_table`;
```

**Action:**
Attempt to execute the stored procedure without providing `p_stichtag`.

```sql
-- This query is expected to fail.
CALL `project.dataset.sp_k_ausd_bp_ta_cntrct_dist`(
    p_job_kennung => 'TEST_JOB',
    p_eintrags_nr => '001',
    p_stichtag => NULL, -- NULL value
    p_wiederanlauf_wert => NULL
);
```

**Pass/Fail Criterion:**
*   **Pass:** The `CALL` statement fails with an error message containing "Parameter p_stichtag is missing or empty."
*   **Fail:** The `CALL` statement succeeds, or fails with a different error message.

---

### Test Case 4: Parameter Validation - Invalid `p_stichtag` Format

**Purpose:** Verify that the stored procedure correctly validates the `p_stichtag` format (DDMMYYYY), mimicking the legacy `DWDate_Datum_Check` behavior.

**Setup:**
Ensure `sof_ta_cntrct_dist` and `job_audit_table` are empty.

```sql
TRUNCATE TABLE `project.dataset.sof_ta_cntrct_dist`;
TRUNCATE TABLE `project.dataset.job_audit_table`;
```

**Action:**
Attempt to execute the stored procedure with an invalid `p_stichtag` format.

```sql
-- This query is expected to fail.
CALL `project.dataset.sp_k_ausd_bp_ta_cntrct_dist`(
    p_job_kennung => 'TEST_JOB',
    p_eintrags_nr => '001',
    p_stichtag => '2023-01-01', -- Invalid format
    p_wiederanlauf_wert => NULL
);
```

**Pass/Fail Criterion:**
*   **Pass:** The `CALL` statement fails with an error message containing "Parameter p_stichtag has an invalid format. Expected DDMMYYYY."
*   **Fail:** The `CALL` statement succeeds, or fails with a different error message.

---

### Test Case 5: Core Transformation - Happy Path with Diverse Data

**Purpose:** Verify that the core transformation logic correctly extracts distinct `cntrct_id`s from `sof_ta_bpr_basis` and inserts them into `sof_ta_cntrct_dist`, including handling duplicates and NULLs, and that the `TRUNCATE` behavior is correct. This also covers output parity for the main data.

**Setup:**
1.  Populate `sof_ta_bpr_basis` with test data including duplicates and NULLs.
2.  Ensure `sof_ta_cntrct_dist` is empty or contains old data (to test `TRUNCATE`).
3.  Ensure `job_audit_table` is empty.

```sql
-- Clear and populate sof_ta_bpr_basis
TRUNCATE TABLE `project.dataset.sof_ta_bpr_basis`;
INSERT INTO `project.dataset.sof_ta_bpr_basis` (cntrct_id) VALUES
(101),
(102),
(101), -- Duplicate
(103),
(NULL), -- NULL value
(104),
(102); -- Another duplicate

-- Clear target and audit tables
TRUNCATE TABLE `project.dataset.sof_ta_cntrct_dist`;
TRUNCATE TABLE `project.dataset.job_audit_table`;
```

**Action:**
Execute the stored procedure with valid parameters.

```sql
CALL `project.dataset.sp_k_ausd_bp_ta_cntrct_dist`(
    p_job_kennung => 'HAPPY_PATH',
    p_eintrags_nr => '002',
    p_stichtag => '15032023',
    p_wiederanlauf_wert => '1'
);
```

**Pass/Fail Criterion:**
*   **Pass:**
    1.  The `CALL` statement completes successfully.
    2.  `sof_ta_cntrct_dist` contains exactly the distinct non-NULL `cntrct_id`s from `sof_ta_bpr_basis`: `101, 102, 103, 104`.
    3.  The count of rows in `sof_ta_cntrct_dist` is 4.
    4.  `job_audit_table` contains one entry for this run with `status = 'SUCCESS'` and `output_records = 4`.
*   **Fail:** Any of the above conditions are not met.

```sql
-- Assertion Query (after Action)
SELECT cntrct_id FROM `project.dataset.sof_ta_cntrct_dist` ORDER BY cntrct_id;
-- Expected: 101, 102, 103, 104

SELECT output_records, status FROM `project.dataset.job_audit_table` WHERE job_name = 'HAPPY_PATH';
-- Expected: output_records = 4, status = 'SUCCESS'
```

---

### Test Case 6: Core Transformation - Empty Source Table

**Purpose:** Verify correct behavior when the source table `sof_ta_bpr_basis` is empty. The target table should also be empty, and the record count should be 0.

**Setup:**
1.  Ensure `sof_ta_bpr_basis` is empty.
2.  Ensure `sof_ta_cntrct_dist` is empty or contains old data.
3.  Ensure `job_audit_table` is empty.

```sql
-- Clear source, target, and audit tables
TRUNCATE TABLE `project.dataset.sof_ta_bpr_basis`;
TRUNCATE TABLE `project.dataset.sof_ta_cntrct_dist`;
TRUNCATE TABLE `project.dataset.job_audit_table`;
```

**Action:**
Execute the stored procedure with valid parameters.

```sql
CALL `project.dataset.sp_k_ausd_bp_ta_cntrct_dist`(
    p_job_kennung => 'EMPTY_SOURCE',
    p_eintrags_nr => '003',
    p_stichtag => '20042023',
    p_wiederanlauf_wert => NULL
);
```

**Pass/Fail Criterion:**
*   **Pass:**
    1.  The `CALL` statement completes successfully.
    2.  `sof_ta_cntrct_dist` is empty (contains 0 rows).
    3.  `job_audit_table` contains one entry for this run with `status = 'SUCCESS'` and `output_records = 0`.
*   **Fail:** Any of the above conditions are not met.

```sql
-- Assertion Query (after Action)
SELECT COUNT(*) FROM `project.dataset.sof_ta_cntrct_dist`;
-- Expected: 0

SELECT output_records, status FROM `project.dataset.job_audit_table` WHERE job_name = 'EMPTY_SOURCE';
-- Expected: output_records = 0, status = 'SUCCESS'
```

---

### Test Case 7: Core Transformation - Source Table with Only NULL `cntrct_id`s

**Purpose:** Verify that NULL `cntrct_id`s in the source table are correctly handled (i.e., not inserted into the target table, as `DISTINCT` typically excludes NULLs unless explicitly handled, and the target column is `INT64` which doesn't allow NULLs by default in BigQuery unless specified, but even if it did, `DISTINCT` would treat them as one). The generated DDL for `cntrct_id INT64` implies non-nullable.

**Setup:**
1.  Populate `sof_ta_bpr_basis` with only NULL `cntrct_id`s.
2.  Ensure `sof_ta_cntrct_dist` is empty or contains old data.
3.  Ensure `job_audit_table` is empty.

```sql
-- Clear and populate sof_ta_bpr_basis
TRUNCATE TABLE `project.dataset.sof_ta_bpr_basis`;
INSERT INTO `project.dataset.sof_ta_bpr_basis` (cntrct_id) VALUES
(NULL),
(NULL),
(NULL);

-- Clear target and audit tables
TRUNCATE TABLE `project.dataset.sof_ta_cntrct_dist`;
TRUNCATE TABLE `project.dataset.job_audit_table`;
```

**Action:**
Execute the stored procedure with valid parameters.

```sql
CALL `project.dataset.sp_k_ausd_bp_ta_cntrct_dist`(
    p_job_kennung => 'NULL_SOURCE',
    p_eintrags_nr => '004',
    p_stichtag => '25042023',
    p_wiederanlauf_wert => NULL
);
```

**Pass/Fail Criterion:**
*   **Pass:**
    1.  The `CALL` statement completes successfully.
    2.  `sof_ta_cntrct_dist` is empty (contains 0 rows).
    3.  `job_audit_table` contains one entry for this run with `status = 'SUCCESS'` and `output_records = 0`.
*   **Fail:** Any of the above conditions are not met.

```sql
-- Assertion Query (after Action)
SELECT COUNT(*) FROM `project.dataset.sof_ta_cntrct_dist`;
-- Expected: 0

SELECT output_records, status FROM `project.dataset.job_audit_table` WHERE job_name = 'NULL_SOURCE';
-- Expected: output_records = 0, status = 'SUCCESS'
```

---

### Test Case 8: Date Derivation and Audit Logging Details

**Purpose:** Verify that `v_datum_heute` and `v_datum_gestern` are correctly derived and stored in the `job_audit_table`'s `input_params` JSON. This directly tests the replacement of `gestern.ksh`.

**Setup:**
1.  Populate `sof_ta_bpr_basis` with some data.
2.  Ensure `sof_ta_cntrct_dist` is empty.
3.  Ensure `job_audit_table` is empty.

```sql
-- Clear and populate sof_ta_bpr_basis
TRUNCATE TABLE `project.dataset.sof_ta_bpr_basis`;
INSERT INTO `project.dataset.sof_ta_bpr_basis` (cntrct_id) VALUES (1);

-- Clear target and audit tables
TRUNCATE TABLE `project.dataset.sof_ta_cntrct_dist`;
TRUNCATE TABLE `project.dataset.job_audit_table`;
```

**Action:**
Execute the stored procedure with valid parameters.

```sql
CALL `project.dataset.sp_k_ausd_bp_ta_cntrct_dist`(
    p_job_kennung => 'DATE_CHECK',
    p_eintrags_nr => '005',
    p_stichtag => '01012024',
    p_wiederanlauf_wert => '0'
);
```

**Pass/Fail Criterion:**
*   **Pass:**
    1.  The `CALL` statement completes successfully.
    2.  `job_audit_table` contains one entry for this run with `status = 'SUCCESS'`.
    3.  The `input_params` JSON in the audit entry correctly shows:
        *   `datum_heute` as `CURRENT_DATE()` (the date the test is run).
        *   `datum_gestern` as `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)` (the day before the test is run).
*   **Fail:** Any of the above conditions are not met.

```sql
-- Assertion Query (after Action)
SELECT
    JSON_VALUE(input_params, '$.datum_heute') AS datum_heute_logged,
    JSON_VALUE(input_params, '$.datum_gestern') AS datum_gestern_logged,
    status
FROM `project.dataset.job_audit_table`
WHERE job_name = 'DATE_CHECK';

-- Expected (example if run on 2024-01-02):
-- datum_heute_logged = '2024-01-02'
-- datum_gestern_logged = '2024-01-01'
-- status = 'SUCCESS'
```

---

### Test Case 9: Audit Logging - Failed Run

**Purpose:** Verify that `job_audit_table` correctly logs details for a failed execution, including the error message and `status = 'FAILED'`.

**Setup:**
Ensure `sof_ta_cntrct_dist` and `job_audit_table` are empty.

```sql
TRUNCATE TABLE `project.dataset.sof_ta_cntrct_dist`;
TRUNCATE TABLE `project.dataset.job_audit_table`;
```

**Action:**
Attempt to execute the stored procedure with an invalid parameter (e.g., missing `p_job_kennung`) to force a failure.

```sql
-- This query is expected to fail.
CALL `project.dataset.sp_k_ausd_bp_ta_cntrct_dist`(
    p_job_kennung => '', -- Invalid parameter to trigger failure
    p_eintrags_nr => '006',
    p_stichtag => '01012023',
    p_wiederanlauf_wert => NULL
);
```

**Pass/Fail Criterion:**
*   **Pass:**
    1.  The `CALL` statement fails as expected.
    2.  `job_audit_table` contains one entry for this run with `status = 'FAILED'`.
    3.  The `error_message` field in the audit entry contains "Parameter p_job_kennung is missing or empty."
    4.  `output_records` is NULL or 0 (as no data transformation occurred).
*   **Fail:** Any of the above conditions are not met.

```sql
-- Assertion Query (after Action)
SELECT status, error_message, output_records
FROM `project.dataset.job_audit_table`
WHERE JSON_VALUE(input_params, '$.eintrags_nr') = '006'; -- Use a unique param to identify the run

-- Expected:
-- status = 'FAILED'
-- error_message = 'Job failed: Parameter p_job_kennung is missing or empty.'
-- output_records = NULL or 0
```

---

### Test Case 10: Idempotency and Truncate Behavior

**Purpose:** Verify that running the stored procedure multiple times with the same inputs produces the same final state in `sof_ta_cntrct_dist`, demonstrating the `TRUNCATE` and re-insertion logic.

**Setup:**
1.  Populate `sof_ta_bpr_basis` with some data.
2.  Ensure `sof_ta_cntrct_dist` is empty.
3.  Ensure `job_audit_table` is empty.

```sql
-- Clear and populate sof_ta_bpr_basis
TRUNCATE TABLE `project.dataset.sof_ta_bpr_basis`;
INSERT INTO `project.dataset.sof_ta_bpr_basis` (cntrct_id) VALUES
(201),
(202),
(201); -- Duplicate

-- Clear target and audit tables
TRUNCATE TABLE `project.dataset.sof_ta_cntrct_dist`;
TRUNCATE TABLE `project.dataset.job_audit_table`;
```

**Action:**
Execute the stored procedure twice consecutively with the same valid parameters.

```sql
CALL `project.dataset.sp_k_ausd_bp_ta_cntrct_dist`(
    p_job_kennung => 'IDEMPOTENCY_TEST',
    p_eintrags_nr => '007',
    p_stichtag => '01052023',
    p_wiederanlauf_wert => NULL
);

CALL `project.dataset.sp_k_ausd_bp_ta_cntrct_dist`(
    p_job_kennung => 'IDEMPOTENCY_TEST',
    p_eintrags_nr => '007',
    p_stichtag => '01052023',
    p_wiederanlauf_wert => NULL
);
```

**Pass/Fail Criterion:**
*   **Pass:**
    1.  Both `CALL` statements complete successfully.
    2.  After the second call, `sof_ta_cntrct_dist` contains exactly the distinct `cntrct_id`s: `201, 202`.
    3.  The count of rows in `sof_ta_cntrct_dist` is 2.
    4.  `job_audit_table` contains two entries for `IDEMPOTENCY_TEST`, both with `status = 'SUCCESS'` and `output_records = 2`.
*   **Fail:** Any of the above conditions are not met (e.g., duplicate rows in `sof_ta_cntrct_dist`, incorrect row count, or failure of either call).

```sql
-- Assertion Query (after Action)
SELECT cntrct_id FROM `project.dataset.sof_ta_cntrct_dist` ORDER BY cntrct_id;
-- Expected: 201, 202

SELECT COUNT(*) FROM `project.dataset.sof_ta_cntrct_dist`;
-- Expected: 2

SELECT COUNT(*), SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END), SUM(output_records)
FROM `project.dataset.job_audit_table`
WHERE job_name = 'IDEMPOTENCY_TEST';
-- Expected: COUNT(*) = 2, SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) = 2, SUM(output_records) = 4 (2+2)
```