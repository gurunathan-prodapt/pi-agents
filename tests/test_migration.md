As a senior data-migration QA engineer, I've designed a comprehensive suite of validation tests for the migration of `k_ausd_bp_ta_apn_vertrag.ksh` to BigQuery. These tests cover output parity, transformation correctness, external system replacements, and data quality/schema assertions, as specified in the requirements.

Each test case includes its purpose, setup instructions, the action to be performed (calling the BigQuery Stored Procedure), and concrete pass/fail criteria using BigQuery SQL assertions.

---

### Pre-requisites for all tests:

1.  The `deployment/deploy_bq_assets.sh` script must have been executed successfully, ensuring all BigQuery tables (`DWTK_MELDUNGEN`, `SOF_TA_BPR_APN`, `SOF_TA_APN_VERTRAG`, `error_log`, `job_tracking`) and stored procedures (`sp_d_ausd_bp_ta_apn_vertrag`, `sp_k_ausd_bp_ta_apn_vertrag`) exist in the `project.dataset` BigQuery environment.
2.  For testing purposes, it is highly recommended to run these tests in a dedicated QA or development BigQuery project/dataset to avoid impacting production data.

### Common Setup for each test:

Before executing the "Action" for each test case, ensure the target tables are clean to prevent interference from previous test runs. This can be done with the following SQL:

```sql
TRUNCATE TABLE project.dataset.SOF_TA_APN_VERTRAG;
TRUNCATE TABLE project.dataset.error_log;
TRUNCATE TABLE project.dataset.job_tracking;
-- Source tables (DWTK_MELDUNGEN, SOF_TA_BPR_APN) will be truncated or populated specifically per test case.
```

---

### Test Case 1: Successful End-to-End Execution with Standard Data

**Purpose:**
Verify that the migrated job executes successfully from orchestration to core logic, producing the expected aggregated output in `SOF_TA_APN_VERTRAG` and correctly logging job status, given valid input parameters and typical source data. This covers output parity and basic transformation correctness.

**Setup:**
1.  Clear target tables as per "Common Setup".
2.  Populate `project.dataset.SOF_TA_BPR_APN` with sample data:
    ```sql
    TRUNCATE TABLE project.dataset.SOF_TA_BPR_APN;
    INSERT INTO project.dataset.SOF_TA_BPR_APN (cntrct_id_ref, bpr_id, cntrct_id, access_point_name, effective_date, expiration_date) VALUES
    ('REF1', 101, 'CONTRACT_A', 'APN_ALPHA', '2023-01-01', '2024-01-01'),
    ('REF2', 102, 'CONTRACT_A', 'APN_BETA', '2023-01-01', '2024-01-01'),
    ('REF3', 103, 'CONTRACT_B', 'APN_GAMMA', '2023-01-01', '2024-01-01'),
    ('REF4', 104, 'CONTRACT_A', 'APN_ALPHA', '2023-01-01', '2024-01-01'), -- Duplicate APN for CONTRACT_A, should be distinct in aggregation
    ('REF5', 105, 'CONTRACT_C', 'APN_DELTA', '2023-01-01', '2024-01-01'),
    ('REF6', 106, 'CONTRACT_C', 'APN_EPSILON', '2023-01-01', '2024-01-01'),
    ('REF7', 107, 'CONTRACT_A', 'REF_ZETA', '2023-01-01', '2024-01-01'); -- Additional REF for CONTRACT_A
    ```
3.  `DWTK_MELDUNGEN` can remain empty or contain dummy data as its direct use in `sp_d_ausd_bp_ta_apn_vertrag` is not specified in the migration design.

**Action:**
Execute the migrated BigQuery Stored Procedure with valid parameters:
```sql
CALL project.dataset.sp_k_ausd_bp_ta_apn_vertrag(
    p_JobKennung => 'TEST_JOB_SUCCESS',
    p_EintragsNr => '001',
    p_Stichtag   => '15032024', -- March 15, 2024
    p_wiederanlaufWert => '0'
);
```

**Pass/Fail Criterion:**
1.  **`SOF_TA_APN_VERTRAG` Content:**
    *   Three rows should be inserted.
    *   The aggregated `access_point_names_aggregated` and `cntrct_id_refs_aggregated` should match the expected distinct, sorted, comma-separated values.
    *   `processing_stichtag` should be `2024-03-15`.
    ```sql
    SELECT
        cntrct_id,
        access_point_names_aggregated,
        cntrct_id_refs_aggregated,
        processing_stichtag
    FROM project.dataset.SOF_TA_APN_VERTRAG
    ORDER BY cntrct_id;
    ```
    **Expected Result:**
    | cntrct_id  | access_point_names_aggregated | cntrct_id_refs_aggregated | processing_stichtag |
    |------------|-------------------------------|---------------------------|---------------------|
    | CONTRACT_A | APN_ALPHA, APN_BETA, REF_ZETA | REF1, REF2, REF3, REF4, REF7 | 2024-03-15          |
    | CONTRACT_B | APN_GAMMA                     | REF3                      | 2024-03-15          |
    | CONTRACT_C | APN_DELTA, APN_EPSILON        | REF5, REF6                | 2024-03-15          |

2.  **`job_tracking` Entry:**
    *   Two entries for `eintragsnr = '001'` should exist: one `STARTED` and one `SUCCESS`.
    *   The `SUCCESS` entry should have `record_count = 3`.
    *   `stichtag` should be `2024-03-15`.
    ```sql
    SELECT job_name, status, record_count, stichtag, eintragsnr
    FROM project.dataset.job_tracking
    WHERE job_name = 'k_ausd_bp_ta_apn_vertrag' AND eintragsnr = '001'
    ORDER BY track_timestamp;
    ```
    **Expected:** Two rows, one with `status = 'STARTED'` and one with `status = 'SUCCESS'`, `record_count = 3`, and `stichtag = '2024-03-15'`.

3.  **`error_log` Entry:**
    *   No entries should be present for this job.
    ```sql
    SELECT COUNT(*) FROM project.dataset.error_log WHERE job_name = 'k_ausd_bp_ta_apn_vertrag';
    ```
    **Expected:** 0 rows.

---

### Test Case 2: Parameter Validation - Missing JobKennung

**Purpose:**
Verify that the job correctly identifies and handles a missing `p_JobKennung` parameter, logs an error, and terminates gracefully without processing data. This tests transformation correctness for parameter validation and external system replacement for error logging.

**Setup:**
1.  Clear target tables as per "Common Setup".
2.  Populate `project.dataset.SOF_TA_BPR_APN` with some data (e.g., from Test Case 1) to ensure no processing occurs.

**Action:**
Execute the migrated BigQuery Stored Procedure with `p_JobKennung` as `NULL` (or an empty string `''`):
```sql
-- Attempt to call with NULL JobKennung
CALL project.dataset.sp_k_ausd_bp_ta_apn_vertrag(
    p_JobKennung => NULL, -- Or use ''
    p_EintragsNr => '002',
    p_Stichtag   => '16032024',
    p_wiederanlaufWert => '0'
);
```
*Note: BigQuery stored procedures will raise an error and stop execution if a `SIGNAL SQLSTATE` is encountered.*

**Pass/Fail Criterion:**
1.  **`SOF_TA_APN_VERTRAG` Content:**
    *   No new rows should be inserted.
    ```sql
    SELECT COUNT(*) FROM project.dataset.SOF_TA_APN_VERTRAG;
    ```
    **Expected:** 0 rows.

2.  **`job_tracking` Entry:**
    *   Two entries for `eintragsnr = '002'` should exist: one `STARTED` and one `FAILED`.
    *   The `FAILED` entry's `details` JSON should contain an error message related to missing `Jobkennung`.
    ```sql
    SELECT job_name, status, eintragsnr, JSON_VALUE(details, '$.error_message') AS error_detail
    FROM project.dataset.job_tracking
    WHERE job_name = 'k_ausd_bp_ta_apn_vertrag' AND eintragsnr = '002'
    ORDER BY track_timestamp;
    ```
    **Expected:** Two rows, one 'STARTED', one 'FAILED' with `error_detail` containing "Jobkennung parameter is missing or empty.".

3.  **`error_log` Entry:**
    *   One entry should be present with `error_code = '193'` and `error_message` indicating `Jobkennung parameter is missing or empty.`.
    ```sql
    SELECT error_code, error_message, severity
    FROM project.dataset.error_log
    WHERE job_name = 'k_ausd_bp_ta_apn_vertrag' AND error_code = '193';
    ```
    **Expected:** One row with `error_code = '193'`, `error_message = 'Jobkennung parameter is missing or empty.'`, and `severity = 'ERROR'`.

---

### Test Case 3: Date Validation - Invalid Stichtag Format

**Purpose:**
Verify that the job correctly identifies and handles an invalid `p_Stichtag` format, logs an error, and terminates gracefully without processing data. This tests transformation correctness for date validation and external system replacement for error logging.

**Setup:**
1.  Clear target tables as per "Common Setup".
2.  Populate `project.dataset.SOF_TA_BPR_APN` with some data (e.g., from Test Case 1) to ensure no processing occurs.

**Action:**
Execute the migrated BigQuery Stored Procedure with an invalid `p_Stichtag` format:
```sql
CALL project.dataset.sp_k_ausd_bp_ta_apn_vertrag(
    p_JobKennung => 'TEST_JOB_DATE_FAIL',
    p_EintragsNr => '003',
    p_Stichtag   => '2024-03-17', -- Invalid format (expected DDMMYYYY)
    p_wiederanlaufWert => '0'
);
```

**Pass/Fail Criterion:**
1.  **`SOF_TA_APN_VERTRAG` Content:**
    *   No new rows should be inserted.
    ```sql
    SELECT COUNT(*) FROM project.dataset.SOF_TA_APN_VERTRAG;
    ```
    **Expected:** 0 rows.

2.  **`job_tracking` Entry:**
    *   Two entries for `eintragsnr = '003'` should exist: one `STARTED` and one `FAILED`.
    *   The `FAILED` entry's `details` JSON should reflect the date validation error.
    ```sql
    SELECT job_name, status, eintragsnr, JSON_VALUE(details, '$.error_message') AS error_detail
    FROM project.dataset.job_tracking
    WHERE job_name = 'k_ausd_bp_ta_apn_vertrag' AND eintragsnr = '003'
    ORDER BY track_timestamp;
    ```
    **Expected:** Two rows, one 'STARTED', one 'FAILED' with `error_detail` containing "Stichtag parameter has an invalid date format.".

3.  **`error_log` Entry:**
    *   One entry should be present with `error_code = 'DATE_FORMAT_ERROR'` and `error_message` indicating `Stichtag parameter has an invalid date format (expected DDMMYYYY).`.
    ```sql
    SELECT error_code, error_message, severity
    FROM project.dataset.error_log
    WHERE job_name = 'k_ausd_bp_ta_apn_vertrag' AND error_code = 'DATE_FORMAT_ERROR';
    ```
    **Expected:** One row with `error_code = 'DATE_FORMAT_ERROR'`, `error_message = 'Stichtag parameter has an invalid date format (expected DDMMYYYY).'`, and `severity = 'ERROR'`.

---

### Test Case 4: Transformation Correctness - Empty Source Table

**Purpose:**
Verify that the job handles an empty source table (`SOF_TA_BPR_APN`) gracefully, resulting in an empty target table and correct record count in `job_tracking`. This tests transformation correctness for edge cases and data quality/row count.

**Setup:**
1.  Clear target tables as per "Common Setup".
2.  Ensure `project.dataset.SOF_TA_BPR_APN` is empty:
    ```sql
    TRUNCATE TABLE project.dataset.SOF_TA_BPR_APN;
    ```

**Action:**
Execute the migrated BigQuery Stored Procedure with valid parameters:
```sql
CALL project.dataset.sp_k_ausd_bp_ta_apn_vertrag(
    p_JobKennung => 'TEST_JOB_EMPTY_SOURCE',
    p_EintragsNr => '004',
    p_Stichtag   => '18032024',
    p_wiederanlaufWert => '0'
);
```

**Pass/Fail Criterion:**
1.  **`SOF_TA_APN_VERTRAG` Content:**
    *   No rows should be inserted.
    ```sql
    SELECT COUNT(*) FROM project.dataset.SOF_TA_APN_VERTRAG;
    ```
    **Expected:** 0 rows.

2.  **`job_tracking` Entry:**
    *   Two entries for `eintragsnr = '004'` should exist: one `STARTED` and one `SUCCESS`.
    *   The `SUCCESS` entry should have `record_count = 0`.
    ```sql
    SELECT job_name, status, record_count, stichtag, eintragsnr
    FROM project.dataset.job_tracking
    WHERE job_name = 'k_ausd_bp_ta_apn_vertrag' AND eintragsnr = '004'
    ORDER BY track_timestamp;
    ```
    **Expected:** Two rows, one 'STARTED', one 'SUCCESS' with `record_count = 0`, and `stichtag = '2024-03-18'`.

3.  **`error_log` Entry:**
    *   No entries should be present for this job.
    ```sql
    SELECT COUNT(*) FROM project.dataset.error_log WHERE job_name = 'k_ausd_bp_ta_apn_vertrag';
    ```
    **Expected:** 0 rows.

---

### Test Case 5: Transformation Correctness - String Aggregation Truncation

**Purpose:**
Verify that the `STRING_AGG` function correctly truncates aggregated strings to 100 characters as specified by `SUBSTR(..., 1, 100)` in `sp_d_ausd_bp_ta_apn_vertrag`. This tests transformation correctness for string handling and potential data quality issues.

**Setup:**
1.  Clear target tables as per "Common Setup".
2.  Populate `project.dataset.SOF_TA_BPR_APN` with data designed to create long aggregated strings:
    ```sql
    TRUNCATE TABLE project.dataset.SOF_TA_BPR_APN;
    INSERT INTO project.dataset.SOF_TA_BPR_APN (cntrct_id_ref, bpr_id, cntrct_id, access_point_name, effective_date, expiration_date) VALUES
    ('REF_LONG_1_0123456789012345678901234567890123456789', 201, 'CONTRACT_LONG', 'APN_A_VERY_VERY_VERY_VERY_VERY_VERY_VERY_VERY_VERY_VERY_VERY_VERY_VERY_VERY_VERY_VERY_LONG_NAME_1', '2023-01-01', '2024-01-01'),
    ('REF_LONG_2_0123456789012345678901234567890123456789', 202, 'CONTRACT_LONG', 'APN_B_VERY_VERY_VERY_VERY_VERY_VERY_VERY_VERY_VERY_VERY_VERY_VERY_VERY_VERY_VERY_VERY_LONG_NAME_2', '2023-01-01', '2024-01-01'),
    ('REF_LONG_3_0123456789012345678901234567890123456789', 203, 'CONTRACT_LONG', 'APN_C_VERY_VERY_VERY_VERY_VERY_VERY_VERY_VERY_VERY_VERY_VERY_VERY_VERY_VERY_VERY_VERY_LONG_NAME_3', '2023-01-01', '2024-01-01');
    ```
    *The individual `access_point_name` and `cntrct_id_ref` values are already long, ensuring that their aggregation will exceed 100 characters.*

**Action:**
Execute the migrated BigQuery Stored Procedure with valid parameters:
```sql
CALL project.dataset.sp_k_ausd_bp_ta_apn_vertrag(
    p_JobKennung => 'TEST_JOB_TRUNCATION',
    p_EintragsNr => '005',
    p_Stichtag   => '19032024',
    p_wiederanlaufWert => '0'
);
```

**Pass/Fail Criterion:**
1.  **`SOF_TA_APN_VERTRAG` Content:**
    *   One row should be inserted for `CONTRACT_LONG`.
    *   The length of `access_point_names_aggregated` and `cntrct_id_refs_aggregated` for `CONTRACT_LONG` should be exactly 100 characters.
    ```sql
    SELECT
        LENGTH(access_point_names_aggregated) AS apn_len,
        LENGTH(cntrct_id_refs_aggregated) AS ref_len
    FROM project.dataset.SOF_TA_APN_VERTRAG
    WHERE cntrct_id = 'CONTRACT_LONG';
    ```
    **Expected:** `apn_len = 100` and `ref_len = 100`.

2.  **`job_tracking` Entry:**
    *   One `SUCCESS` entry with `record_count = 1`.

3.  **`error_log` Entry:**
    *   No entries.

---

### Test Case 6: External System Replacement - Job Tracking Status Updates

**Purpose:**
Verify that the `job_tracking` table accurately reflects the job's lifecycle, including `STARTED` and `SUCCESS` or `FAILED` states, and captures relevant metadata. This directly tests the replacement of `FOSJobErzeugeEintrag` logic.

**Setup:**
1.  Clear target tables as per "Common Setup".
2.  Populate `project.dataset.SOF_TA_BPR_APN` with some data (e.g., from Test Case 1).

**Action:**
1.  Execute a successful run:
    ```sql
    CALL project.dataset.sp_k_ausd_bp_ta_apn_vertrag(
        p_JobKennung => 'TRACK_SUCCESS',
        p_EintragsNr => '006A',
        p_Stichtag   => '20032024',
        p_wiederanlaufWert => '0'
    );
    ```
2.  Execute a failed run (e.g., invalid `Stichtag`):
    ```sql
    CALL project.dataset.sp_k_ausd_bp_ta_apn_vertrag(
        p_JobKennung => 'TRACK_FAIL',
        p_EintragsNr => '006B',
        p_Stichtag   => 'INVALID_DATE',
        p_wiederanlaufWert => '0'
    );
    ```

**Pass/Fail Criterion:**
1.  **`job_tracking` for Success:**
    *   Two entries for `eintragsnr = '006A'`: one `STARTED`, one `SUCCESS`.
    *   The `SUCCESS` entry should have `record_count > 0` (based on `SOF_TA_BPR_APN` data).
    *   `stichtag` should be `2024-03-20`.
    *   `details` JSON should contain `job_kennung` and `wiederanlauf_wert`.
    ```sql
    SELECT job_name, status, record_count, stichtag, eintragsnr, JSON_VALUE(details, '$.job_kennung') AS job_kennung_detail
    FROM project.dataset.job_tracking
    WHERE job_name = 'k_ausd_bp_ta_apn_vertrag' AND eintragsnr = '006A'
    ORDER BY track_timestamp;
    ```
    **Expected:** Two rows, one 'STARTED', one 'SUCCESS' with `record_count > 0`, `stichtag = '2024-03-20'`, and `job_kennung_detail = "TRACK_SUCCESS"`.

2.  **`job_tracking` for Failure:**
    *   Two entries for `eintragsnr = '006B'`: one `STARTED`, one `FAILED`.
    *   The `FAILED` entry should have `record_count IS NULL`.
    *   `details` JSON should contain an `error_message`.
    ```sql
    SELECT job_name, status, record_count, eintragsnr, JSON_VALUE(details, '$.error_message') AS error_message_detail
    FROM project.dataset.job_tracking
    WHERE job_name = 'k_ausd_bp_ta_apn_vertrag' AND eintragsnr = '006B'
    ORDER BY track_timestamp;
    ```
    **Expected:** Two rows, one 'STARTED', one 'FAILED' with `record_count IS NULL` and `error_message_detail` indicating a date format error.

3.  **`error_log` for Failure:**
    *   One entry for `TRACK_FAIL` with `severity = 'ERROR'` or `CRITICAL` and an appropriate message.
    ```sql
    SELECT error_message, severity
    FROM project.dataset.error_log
    WHERE job_name = 'k_ausd_bp_ta_apn_vertrag' AND error_code = 'DATE_FORMAT_ERROR';
    ```
    **Expected:** One row with error message related to invalid date format and `severity = 'ERROR'`.

---

### Test Case 7: Transformation Correctness - Date Derivation

**Purpose:**
Verify that `CURRENT_DATE()` and `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)` correctly replace the `gestern.ksh` script for deriving `v_datum_heute` and `v_datum_gestern`.

**Setup:**
1.  Clear target tables as per "Common Setup".
2.  Populate `project.dataset.SOF_TA_BPR_APN` with some data.
3.  **Temporary Modification for Testing:** To directly verify the derived dates, the `sp_k_ausd_bp_ta_apn_vertrag` procedure needs a temporary modification to log `v_datum_heute` and `v_datum_gestern` into the `job_tracking.details` JSON object upon successful completion.
    *   Locate the `UPDATE project.dataset.job_tracking` statement for `status = 'SUCCESS'`.
    *   Modify the `details` JSON object to include:
        ```json
        'derived_heute', CAST(v_datum_heute AS STRING),
        'derived_gestern', CAST(v_datum_gestern AS STRING)
        ```
    *   Redeploy the stored procedure after this modification.

**Action:**
Execute the migrated BigQuery Stored Procedure with valid parameters:
```sql
CALL project.dataset.sp_k_ausd_bp_ta_apn_vertrag(
    p_JobKennung => 'TEST_DATE_DERIVATION',
    p_EintragsNr => '007',
    p_Stichtag   => '21032024',
    p_wiederanlaufWert => '0'
);
```

**Pass/Fail Criterion:**
1.  **`job_tracking` Entry:**
    *   One `SUCCESS` entry for `eintragsnr = '007'` should exist.
    *   The `details` JSON of this entry should contain `derived_heute` and `derived_gestern` matching `CURRENT_DATE()` and `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)` respectively, for the date the test is run.
    ```sql
    SELECT
        JSON_VALUE(details, '$.derived_heute') AS derived_heute_str,
        JSON_VALUE(details, '$.derived_gestern') AS derived_gestern_str
    FROM project.dataset.job_tracking
    WHERE job_name = 'k_ausd_bp_ta_apn_vertrag' AND eintragsnr = '007' AND status = 'SUCCESS';
    ```
    **Expected:** `derived_heute_str` should be `CAST(CURRENT_DATE() AS STRING)` (e.g., '2024-03-21') and `derived_gestern_str` should be `CAST(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AS STRING)` (e.g., '2024-03-20').

---

### Test Case 8: Transformation Correctness - NULL Handling in Source Data

**Purpose:**
Verify how the `STRING_AGG` and `SUBSTR` functions handle `NULL` values in the source columns (`access_point_name`, `cntrct_id_ref`). This ensures that `NULL`s are correctly ignored by `STRING_AGG` and do not cause unexpected errors or output.

**Setup:**
1.  Clear target tables as per "Common Setup".
2.  **Temporary DDL Modification for Testing:** The provided DDL for `SOF_TA_BPR_APN` specifies `NOT NULL` for `access_point_name` and `cntrct_id_ref`. For this test, these constraints must be temporarily relaxed to allow `NULL` insertions.
    ```sql
    -- This is a conceptual step for testing. In a real scenario, you might create a temporary table
    -- with relaxed constraints or use a separate test dataset.
    -- ALTER TABLE project.dataset.SOF_TA_BPR_APN ALTER COLUMN access_point_name DROP NOT NULL;
    -- ALTER TABLE project.dataset.SOF_TA_BPR_APN ALTER COLUMN cntrct_id_ref DROP NOT NULL;
    ```
3.  Populate `project.dataset.SOF_TA_BPR_APN` with data including `NULL` values:
    ```sql
    TRUNCATE TABLE project.dataset.SOF_TA_BPR_APN;
    INSERT INTO project.dataset.SOF_TA_BPR_APN (cntrct_id_ref, bpr_id, cntrct_id, access_point_name, effective_date, expiration_date) VALUES
    ('REF_NULL_1', 301, 'CONTRACT_NULL_APN', NULL, '2023-01-01', '2024-01-01'),
    ('REF_NULL_2', 302, 'CONTRACT_NULL_APN', 'APN_VALID', '2023-01-01', '2024-01-01'),
    (NULL, 303, 'CONTRACT_NULL_REF', 'APN_VALID_2', '2023-01-01', '2024-01-01'),
    ('REF_VALID_3', 304, 'CONTRACT_NULL_REF', 'APN_VALID_3', '2023-01-01', '2024-01-01'),
    ('REF_ALL_NULL', 305, 'CONTRACT_ALL_NULL', NULL, '2023-01-01', '2024-01-01'); -- Only one row, APN is NULL
    ```
    *Note: `STRING_AGG` by default ignores `NULL` values. If all values for a group are `NULL`, it will return `NULL`.*

**Action:**
Execute the migrated BigQuery Stored Procedure:
```sql
CALL project.dataset.sp_k_ausd_bp_ta_apn_vertrag(
    p_JobKennung => 'TEST_NULL_HANDLING',
    p_EintragsNr => '008',
    p_Stichtag   => '22032024',
    p_wiederanlaufWert => '0'
);
```

**Pass/Fail Criterion:**
1.  **`SOF_TA_APN_VERTRAG` Content:**
    *   Three rows should be inserted.
    *   For `CONTRACT_NULL_APN`: `access_point_names_aggregated` should be 'APN_VALID', `cntrct_id_refs_aggregated` should be 'REF_NULL_1, REF_NULL_2'.
    *   For `CONTRACT_NULL_REF`: `access_point_names_aggregated` should be 'APN_VALID_2, APN_VALID_3', `cntrct_id_refs_aggregated` should be 'REF_VALID_3'. (The `NULL` `cntrct_id_ref` is ignored).
    *   For `CONTRACT_ALL_NULL`: `access_point_names_aggregated` should be `NULL` (since `STRING_AGG` ignores `NULL`s and there's only one `NULL` value), `cntrct_id_refs_aggregated` should be 'REF_ALL_NULL'.
    ```sql
    SELECT
        cntrct_id,
        access_point_names_aggregated,
        cntrct_id_refs_aggregated
    FROM project.dataset.SOF_TA_APN_VERTRAG
    ORDER BY cntrct_id;
    ```
    **Expected Result:**
    | cntrct_id         | access_point_names_aggregated | cntrct_id_refs_aggregated |
    |-------------------|-------------------------------|---------------------------|
    | CONTRACT_ALL_NULL | NULL                          | REF_ALL_NULL              |
    | CONTRACT_NULL_APN | APN_VALID                     | REF_NULL_1, REF_NULL_2    |
    | CONTRACT_NULL_REF | APN_VALID_2, APN_VALID_3      | REF_VALID_3               |

2.  **`job_tracking` Entry:**
    *   One `SUCCESS` entry with `record_count = 3`.

3.  **`error_log` Entry:**
    *   No entries.

---

### Test Case 9: Output Parity - Full Data Comparison (Golden Run)

**Purpose:**
To ensure complete output parity, this test compares the final state of `SOF_TA_APN_VERTRAG` after running the migrated job against a "golden" dataset produced by the legacy job with identical input. This is the ultimate test for output parity.

**Setup:**
1.  **Legacy Run:** Execute the original `k_ausd_bp_ta_apn_vertrag.ksh` script with a specific, well-defined set of input data for `DWTK_MELDUNGEN` and `SOF$TA_BPR_APN`.
2.  **Capture Legacy Output:** Extract the resulting data from the legacy `SOF$TA_APN_VERTRAG` table into a "golden" CSV or JSON file. Then, load this golden data into a temporary BigQuery table (e.g., `project.dataset.SOF_TA_APN_VERTRAG_GOLDEN`).
3.  **Migrated Setup:**
    *   Clear target tables as per "Common Setup".
    *   Load the *exact same* input data used for the legacy run into `project.dataset.DWTK_MELDUNGEN` and `project.dataset.SOF_TA_BPR_APN`.
    *   Ensure the `SOF_TA_APN_VERTRAG_GOLDEN` table is populated with the legacy output.

**Action:**
Execute the migrated BigQuery Stored Procedure with the same parameters used for the legacy run:
```sql
CALL project.dataset.sp_k_ausd_bp_ta_apn_vertrag(
    p_JobKennung => 'GOLDEN_RUN',
    p_EintragsNr => '009',
    p_Stichtag   => '23032024', -- Use the same Stichtag as legacy run
    p_wiederanlaufWert => '0'
);
```

**Pass/Fail Criterion:**
1.  **Data Parity:** The data in `project.dataset.SOF_TA_APN_VERTRAG` must be identical to the data in `project.dataset.SOF_TA_APN_VERTRAG_GOLDEN`.
    ```sql
    -- Check row counts first
    SELECT COUNT(*) FROM project.dataset.SOF_TA_APN_VERTRAG;
    SELECT COUNT(*) FROM project.dataset.SOF_TA_APN_VERTRAG_GOLDEN;

    -- Then check for exact data match (should return 0 rows if identical)
    SELECT 'Only in Migrated' AS source, t.* FROM project.dataset.SOF_TA_APN_VERTRAG AS t EXCEPT DISTINCT SELECT 'Only in Migrated', g.* FROM project.dataset.SOF_TA_APN_VERTRAG_GOLDEN AS g
    UNION ALL
    SELECT 'Only in Golden' AS source, g.* FROM project.dataset.SOF_TA_APN_VERTRAG_GOLDEN AS g EXCEPT DISTINCT SELECT 'Only in Golden', t.* FROM project.dataset.SOF_TA_APN_VERTRAG AS t;
    ```
    **Expected:** Both `COUNT(*)` queries return the same number, and the `UNION ALL` query returns 0 rows.

2.  **Record Count Parity:** The `record_count` in `job_tracking` for the `SUCCESS` entry should match the count of rows in `SOF_TA_APN_VERTRAG_GOLDEN`.
    ```sql
    SELECT record_count FROM project.dataset.job_tracking WHERE eintragsnr = '009' AND status = 'SUCCESS';
    ```
    **Expected:** `record_count` matches `COUNT(*) FROM project.dataset.SOF_TA_APN_VERTRAG_GOLDEN`.

---

### Test Case 10: Data Quality - Schema and Data Types

**Purpose:**
Verify that the target BigQuery tables (`SOF_TA_APN_VERTRAG`, `error_log`, `job_tracking`) have the expected schema and data types as defined in the DDLs, and that data is stored correctly according to these types. This is a fundamental data quality assertion.

**Setup:**
1.  Run Test Case 1 (Successful End-to-End Execution) to populate the tables with representative data.

**Action:**
Query BigQuery's `INFORMATION_SCHEMA` to inspect the table schemas and sample data to confirm types.

**Pass/Fail Criterion:**
1.  **`SOF_TA_APN_VERTRAG` Schema:**
    ```sql
    SELECT column_name, data_type, is_nullable
    FROM project.dataset.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = 'SOF_TA_APN_VERTRAG'
    ORDER BY ordinal_position;
    ```
    **Expected:**
    | column_name                 | data_type | is_nullable |
    |-----------------------------|-----------|-------------|
    | cntrct_id                   | STRING    | NO          |
    | access_point_names_aggregated | STRING    | YES         |
    | cntrct_id_refs_aggregated   | STRING    | YES         |
    | processing_stichtag         | DATE      | NO          |

2.  **`error_log` Schema:**
    ```sql
    SELECT column_name, data_type, is_nullable
    FROM project.dataset.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = 'error_log'
    ORDER BY ordinal_position;
    ```
    **Expected:**
    | column_name   | data_type | is_nullable |
    |---------------|-----------|-------------|
    | log_timestamp | TIMESTAMP | YES         |
    | job_name      | STRING    | NO          |
    | error_code    | STRING    | YES         |
    | error_message | STRING    | NO          |
    | severity      | STRING    | YES         |
    | details       | JSON      | YES         |

3.  **`job_tracking` Schema:**
    ```sql
    SELECT column_name, data_type, is_nullable
    FROM project.dataset.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = 'job_tracking'
    ORDER BY ordinal_position;
    ```
    **Expected:**
    | column_name     | data_type | is_nullable |
    |-----------------|-----------|-------------|
    | track_timestamp | TIMESTAMP | YES         |
    | job_name        | STRING    | NO          |
    | status          | STRING    | NO          |
    | record_count    | INT64     | YES         |
    | stichtag        | DATE      | YES         |
    | eintragsnr      | STRING    | YES         |
    | details         | JSON      | YES         |

4.  **Data Type Confirmation (Sample):**
    *   Verify that `processing_stichtag` in `SOF_TA_APN_VERTRAG` is indeed a DATE type (e.g., `SELECT processing_stichtag FROM project.dataset.SOF_TA_APN_VERTRAG LIMIT 1;` should return 'YYYY-MM-DD').
    *   Verify `log_timestamp` and `track_timestamp` are TIMESTAMP.
    *   Verify `record_count` is INT64.
    ```sql
    -- Example for SOF_TA_APN_VERTRAG
    SELECT
        processing_stichtag,
        TYPEOF(processing_stichtag) AS stichtag_type
    FROM project.dataset.SOF_TA_APN_VERTRAG
    WHERE processing_stichtag IS NOT NULL
    LIMIT 1;

    -- Example for job_tracking
    SELECT
        track_timestamp,
        TYPEOF(track_timestamp) AS timestamp_type,
        record_count,
        TYPEOF(record_count) AS record_count_type
    FROM project.dataset.job_tracking
    WHERE track_timestamp IS NOT NULL
    LIMIT 1;
    ```
    **Expected:** `stichtag_type` should be 'DATE', `timestamp_type` should be 'TIMESTAMP', `record_count_type` should be 'INT64'.

---