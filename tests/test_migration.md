As a senior data-migration QA engineer, I've reviewed the migration design and the generated BigQuery code for `k_ausd_v_ta_period.ksh`. The migration involves re-implementing the KornShell orchestration in a BigQuery stored procedure (`r_ausd_vertrag_control`) and the underlying SQL transformation in another BigQuery stored procedure (`d_ausd_v_ta_period`), along with a new `job_audit` table.

**Important Observations & Assumptions for Testing:**

1.  **`VIA` Table Discrepancy**: The migration design states `d_ausd_v_ta_period.sql` writes to `TABLE:SOF$TA_PERIOD` and `TABLE:VIA`. However, the provided BigQuery procedure `d_ausd_v_ta_period` *only* writes to `project.dataset.sof$ta_period`. This is a significant discrepancy. For these tests, I will assume `sof$ta_period` is the *only* target table for the transformation. If `VIA` is a critical output, the BigQuery transformation procedure needs to be updated to include it, and corresponding tests would be required.
2.  **`r_ausd_vertrag_control` Procedure Fixes**:
    *   The provided `r_ausd_vertrag_control` procedure calls `d_ausd_v_ta_period` twice. This has been corrected in the test assumptions to call it once and capture its `SELECT` output using `FOR record IN (CALL ...)` syntax.
    *   The `r_ausd_vertrag_control` procedure was missing `p_carmen_project` and `p_carmen_dataset` parameters, which are required by `d_ausd_v_ta_period`. It is assumed these parameters have been added to `r_ausd_vertrag_control` and are passed down correctly.
3.  **Job Activation/Deactivation Logic**: The original script mentions "aktive Jobs werden ignoriert" and "alte aktive Jobs werden einfach dekativiert," likely handled by the `starteSQLSkript` wrapper. The provided BigQuery code for `r_ausd_vertrag_control` does not explicitly implement this logic beyond logging the current run's status. The tests will verify the logging behavior as implemented, acknowledging this potential functional gap if the legacy job management was more complex.
4.  **Legacy Environment Simulation**: For comparison, these tests assume access to a legacy Oracle environment or a robust simulation that can produce the "expected" output for `sof$ta_period` and the temporary file content. The BigQuery tests will assert against this expected output.
5.  **`DWPA_UTIL_SKRIPT`**: The functionality of this Oracle package is not fully detailed. The BigQuery `d_ausd_v_ta_period` procedure includes a `TRUNCATE TABLE` statement, which is noted as mimicking `DWPA_UTIL_SKRIPT.runstatement`. This specific behavior will be tested.

---

## Migration Validation Tests for `k_ausd_v_ta_period.ksh`

### Test Case 1: Happy Path - Full Data Flow & Output Parity

**Purpose:**
To verify that the migrated BigQuery orchestration and transformation procedures execute successfully with valid inputs, producing the expected data in the target table (`sof$ta_period`) and logging correct audit information, demonstrating output parity with the legacy system.

**Setup:**
1.  **BigQuery Tables**: Ensure `project.dataset.job_audit`, `project.isbert_schema.dwtk_meldungen`, `project.dataset.cds$ta_period`, `project.dataset.cds$ta_time_meas_cv`, `project.dataset.cds$ta_description`, and `project.dataset.sof$ta_period` exist with their defined schemas.
2.  **Source Data (BigQuery)**:
    *   `project.isbert_schema.dwtk_meldungen`: Insert a record for `JOB_KENNUNG = 'TEST_JOB_HAPPY'` with `TIMECREATED = '2023-01-15 10:00:00 UTC'`.
    *   `project.dataset.cds$ta_period`:
        ```sql
        INSERT INTO `project.dataset.cds$ta_period` (period_id, number_time_measurement, time_meas_cv, insert_at, modified_at) VALUES
        (1, 100, 'CV1', '2023-01-10', NULL),
        (2, 200, 'CV2', '2023-01-12', '2023-01-13'),
        (3, 300, 'CV1', '2023-01-16', NULL), -- Should be included if as_of_date is 2023-01-15
        (4, 400, 'CV3', '2023-01-05', '2023-01-06');
        ```
    *   `project.dataset.cds$ta_time_meas_cv`:
        ```sql
        INSERT INTO `project.dataset.cds$ta_time_meas_cv` (time_meas_cv, description_id) VALUES
        ('CV1', 101),
        ('CV2', 102);
        ```
    *   `project.dataset.cds$ta_description`:
        ```sql
        INSERT INTO `project.dataset.cds$ta_description` (description_id, description) VALUES
        (101, 'Unit A'),
        (102, 'Unit B');
        ```
3.  **Expected Output (Legacy Simulation)**: Simulate the legacy `k_ausd_v_ta_period.ksh` script run with `p_JobKennung='TEST_JOB_HAPPY'` and `p_EintragsNr='123'` against equivalent Oracle data. Capture the final state of `SOF$TA_PERIOD` and the content of the temporary file (e.g., `v_records=2`).

**Action:**
Execute the BigQuery orchestration procedure:
```sql
CALL `project.dataset.r_ausd_vertrag_control`(
    p_job_kennung => 'TEST_JOB_HAPPY',
    p_eintragsnr => '123',
    p_carmen_project => 'project',
    p_carmen_dataset => 'dataset',
    p_as_of_date => NULL -- Let it derive from dwtk_meldungen
);
```

**Pass/Fail Criterion:**
1.  **Output Parity (`sof$ta_period`)**:
    *   The row count in `project.dataset.sof$ta_period` must match the legacy `SOF$TA_PERIOD` (expected: 2 records).
    *   The data content in `project.dataset.sof$ta_period` must exactly match the legacy `SOF$TA_PERIOD`.
    ```sql
    -- Expected data in sof$ta_period:
    -- period_id | number_time_measurement | time_meas_cv | einheit | bfc_age
    -- ----------|-------------------------|--------------|---------|----------
    -- 1         | 100                     | CV1          | Unit A  | 2023-01-10
    -- 2         | 200                     | CV2          | Unit B  | 2023-01-12
    -- (Record 3 is excluded because insert_at (2023-01-16) > as_of_date (2023-01-15))
    -- (Record 4 is excluded because time_meas_cv 'CV3' has no matching description)
    ```
2.  **Audit Log (`job_audit`)**:
    *   A new record must exist in `project.dataset.job_audit` with `job_id = 'k_ausd_v_ta_period'` and `status = 'SUCCESS'`.
    *   `processed_records` must be `2`.
    *   `job_kennung_param` must be `'TEST_JOB_HAPPY'`.
    *   `eintragsnr_param` must be `'123'`.
    *   `start_time` and `end_time` must be populated, with `end_time` > `start_time`.
    *   `error_message` must be `NULL`.
    *   `log_details` should contain a message indicating success and the `as_of_date` used (which should be `2023-01-15`).

### Test Case 2: Transformation Correctness - `v_as_of_date` Determination

**Purpose:**
To verify the logic for determining the `v_as_of_date` within `d_ausd_v_ta_period` based on `p_as_of_date` parameter, `dwtk_meldungen` table, and the fallback default.

**Setup:**
1.  **BigQuery Tables**: Same as Test Case 1.
2.  **Source Data (BigQuery)**:
    *   `project.dataset.cds$ta_period`:
        ```sql
        INSERT INTO `project.dataset.cds$ta_period` (period_id, number_time_measurement, time_meas_cv, insert_at, modified_at) VALUES
        (10, 1000, 'CV1', '2023-02-01', NULL),
        (11, 1100, 'CV1', '2023-02-10', NULL),
        (12, 1200, 'CV1', '2023-02-20', NULL);
        ```
    *   `project.dataset.cds$ta_time_meas_cv` and `project.dataset.cds$ta_description` with matching 'CV1' and 'Unit A'.
    *   Clear `project.isbert_schema.dwtk_meldungen` before each sub-scenario.
    *   Clear `project.dataset.sof$ta_period` before each sub-scenario.

**Action (Sub-scenarios):**

*   **Scenario A: `p_as_of_date` provided.**
    ```sql
    CALL `project.dataset.r_ausd_vertrag_control`(
        p_job_kennung => 'TEST_JOB_ASOF_A',
        p_eintragsnr => '124',
        p_carmen_project => 'project',
        p_carmen_dataset => 'dataset',
        p_as_of_date => '2023-02-15' -- Explicit date
    );
    ```
*   **Scenario B: `p_as_of_date` NULL, `dwtk_meldungen` has entry.**
    ```sql
    INSERT INTO `project.isbert_schema.dwtk_meldungen` (job_kennung, timecreated) VALUES
    ('TEST_JOB_ASOF_B', '2023-02-05 10:00:00 UTC');

    CALL `project.dataset.r_ausd_vertrag_control`(
        p_job_kennung => 'TEST_JOB_ASOF_B',
        p_eintragsnr => '125',
        p_carmen_project => 'project',
        p_carmen_dataset => 'dataset',
        p_as_of_date => NULL
    );
    ```
*   **Scenario C: `p_as_of_date` NULL, `dwtk_meldungen` has NO entry.**
    ```sql
    -- Ensure no entry for 'TEST_JOB_ASOF_C' in dwtk_meldungen
    CALL `project.dataset.r_ausd_vertrag_control`(
        p_job_kennung => 'TEST_JOB_ASOF_C',
        p_eintragsnr => '126',
        p_carmen_project => 'project',
        p_carmen_dataset => 'dataset',
        p_as_of_date => NULL
    );
    ```

**Pass/Fail Criterion:**
1.  **Scenario A (`p_as_of_date` = '2023-02-15')**:
    *   `project.dataset.sof$ta_period` must contain 2 records (period_id 10, 11).
    *   `job_audit` for `TEST_JOB_ASOF_A` must show `processed_records = 2` and `log_details` reflecting `as_of_date = '2023-02-15'`.
2.  **Scenario B (`dwtk_meldungen` = '2023-02-05')**:
    *   `project.dataset.sof$ta_period` must contain 1 record (period_id 10).
    *   `job_audit` for `TEST_JOB_ASOF_B` must show `processed_records = 1` and `log_details` reflecting `as_of_date = '2023-02-05'`.
3.  **Scenario C (Fallback to '1900-01-01')**:
    *   `project.dataset.sof$ta_period` must contain 0 records (as all `insert_at` dates are after '1900-01-01').
    *   `job_audit` for `TEST_JOB_ASOF_C` must show `processed_records = 0` and `log_details` reflecting `as_of_date = '1900-01-01'`.

### Test Case 3: Transformation Correctness - Joins, Filters, NULL Handling

**Purpose:**
To verify the correctness of join conditions, `WHERE` clause filters (`insert_at`, `modified_at`), and how NULL values in source columns are handled during transformation.

**Setup:**
1.  **BigQuery Tables**: Same as Test Case 1.
2.  **Source Data (BigQuery)**: Clear all source tables.
    *   `project.dataset.cds$ta_period`:
        ```sql
        INSERT INTO `project.dataset.cds$ta_period` (period_id, number_time_measurement, time_meas_cv, insert_at, modified_at) VALUES
        (20, 100, 'CV_MATCH', '2023-03-01', NULL), -- Match, included
        (21, 200, 'CV_MATCH', '2023-03-05', '2023-03-02'), -- Match, excluded (modified_at < as_of_date)
        (22, 300, 'CV_MATCH', '2023-03-03', '2023-03-06'), -- Match, included (modified_at > as_of_date)
        (23, 400, 'CV_NO_MATCH', '2023-03-04', NULL), -- No join match, excluded
        (24, NULL, 'CV_MATCH', '2023-03-02', NULL), -- NULL number_time_measurement, included
        (25, 500, NULL, '2023-03-01', NULL), -- NULL time_meas_cv, no join match, excluded
        (26, 600, 'CV_MATCH', '2023-03-10', NULL); -- insert_at > as_of_date, excluded
        ```
    *   `project.dataset.cds$ta_time_meas_cv`:
        ```sql
        INSERT INTO `project.dataset.cds$ta_time_meas_cv` (time_meas_cv, description_id) VALUES
        ('CV_MATCH', 201);
        ```
    *   `project.dataset.cds$ta_description`:
        ```sql
        INSERT INTO `project.dataset.cds$ta_description` (description_id, description) VALUES
        (201, 'Unit C');
        ```
    *   `project.isbert_schema.dwtk_meldungen`: Insert a record for `JOB_KENNUNG = 'TEST_JOB_TRANSFORM'` with `TIMECREATED = '2023-03-05 10:00:00 UTC'`.

**Action:**
Execute the BigQuery orchestration procedure:
```sql
CALL `project.dataset.r_ausd_vertrag_control`(
    p_job_kennung => 'TEST_JOB_TRANSFORM',
    p_eintragsnr => '127',
    p_carmen_project => 'project',
    p_carmen_dataset => 'dataset',
    p_as_of_date => NULL -- as_of_date will be 2023-03-05
);
```

**Pass/Fail Criterion:**
1.  **Output Parity (`sof$ta_period`)**:
    *   `project.dataset.sof$ta_period` must contain exactly 3 records.
    *   The data content must be:
        ```
        period_id | number_time_measurement | time_meas_cv | einheit | bfc_age
        ----------|-------------------------|--------------|---------|----------
        20        | 100                     | CV_MATCH     | Unit C  | 2023-03-01
        22        | 300                     | CV_MATCH     | Unit C  | 2023-03-03
        24        | NULL                    | CV_MATCH     | Unit C  | 2023-03-02
        ```
2.  **Audit Log (`job_audit`)**:
    *   A new record must exist in `project.dataset.job_audit` with `job_id = 'k_ausd_v_ta_period'` and `status = 'SUCCESS'`.
    *   `processed_records` must be `3`.
    *   `log_details` should reflect `as_of_date = '2023-03-05'`.

### Test Case 4: Edge Case - Empty Source Tables

**Purpose:**
To verify the job handles scenarios where source tables are empty gracefully, resulting in an empty target table and correct audit logging.

**Setup:**
1.  **BigQuery Tables**: Same as Test Case 1.
2.  **Source Data (BigQuery)**: Ensure `project.dataset.cds$ta_period`, `project.dataset.cds$ta_time_meas_cv`, `project.dataset.cds$ta_description` are all empty.
3.  **Target Table**: Ensure `project.dataset.sof$ta_period` is empty.
4.  `project.isbert_schema.dwtk_meldungen`: Insert a record for `JOB_KENNUNG = 'TEST_JOB_EMPTY'` with `TIMECREATED = '2023-04-01 10:00:00 UTC'`.

**Action:**
Execute the BigQuery orchestration procedure:
```sql
CALL `project.dataset.r_ausd_vertrag_control`(
    p_job_kennung => 'TEST_JOB_EMPTY',
    p_eintragsnr => '128',
    p_carmen_project => 'project',
    p_carmen_dataset => 'dataset',
    p_as_of_date => NULL
);
```

**Pass/Fail Criterion:**
1.  **Output Parity (`sof$ta_period`)**:
    *   `project.dataset.sof$ta_period` must remain empty (0 records).
2.  **Audit Log (`job_audit`)**:
    *   A new record must exist in `project.dataset.job_audit` with `job_id = 'k_ausd_v_ta_period'` and `status = 'SUCCESS'`.
    *   `processed_records` must be `0`.
    *   `error_message` must be `NULL`.

### Test Case 5: Parameter Validation

**Purpose:**
To verify that the `r_ausd_vertrag_control` procedure correctly validates its input parameters (`p_job_kennung`, `p_eintragsnr`) and fails with an appropriate error message if they are missing or empty, without attempting the data transformation.

**Setup:**
1.  **BigQuery Tables**: Same as Test Case 1.
2.  **Source Data (BigQuery)**: Populate `cds$ta_period` with some data to ensure the transformation *would* produce results if called.
3.  **Target Table**: Ensure `project.dataset.sof$ta_period` is empty.

**Action (Sub-scenarios):**

*   **Scenario A: `p_job_kennung` is NULL.**
    ```sql
    CALL `project.dataset.r_ausd_vertrag_control`(
        p_job_kennung => NULL,
        p_eintragsnr => '129',
        p_carmen_project => 'project',
        p_carmen_dataset => 'dataset',
        p_as_of_date => NULL
    );
    ```
*   **Scenario B: `p_job_kennung` is empty string.**
    ```sql
    CALL `project.dataset.r_ausd_vertrag_control`(
        p_job_kennung => '',
        p_eintragsnr => '130',
        p_carmen_project => 'project',
        p_carmen_dataset => 'dataset',
        p_as_of_date => NULL
    );
    ```
*   **Scenario C: `p_eintragsnr` is NULL.**
    ```sql
    CALL `project.dataset.r_ausd_vertrag_control`(
        p_job_kennung => 'TEST_JOB_PARAM_C',
        p_eintragsnr => NULL,
        p_carmen_project => 'project',
        p_carmen_dataset => 'dataset',
        p_as_of_date => NULL
    );
    ```

**Pass/Fail Criterion:**
1.  **`sof$ta_period`**: For all scenarios, `project.dataset.sof$ta_period` must remain empty. This confirms the transformation procedure was not executed.
2.  **Audit Log (`job_audit`)**:
    *   For each scenario, a new record must exist in `project.dataset.job_audit` with `job_id = 'k_ausd_v_ta_period'` and `status = 'FAILED'`.
    *   `processed_records` must be `0`.
    *   `error_message` must contain a message indicating the specific parameter validation failure (e.g., "JobKennung (p_job_kennung) cannot be NULL or empty." or "EintragsNr (p_eintragsnr) cannot be NULL or empty.").
    *   `log_details` should contain the detailed error information.

### Test Case 6: Error Handling - Transformation Failure

**Purpose:**
To verify that if the `d_ausd_v_ta_period` procedure encounters an error during data transformation, the `r_ausd_vertrag_control` procedure correctly catches it, logs the failure, and ensures the target table is not left in an inconsistent state.

**Setup:**
1.  **BigQuery Tables**: Same as Test Case 1.
2.  **Source Data (BigQuery)**: Populate `cds$ta_period` with data that would normally be processed.
3.  **Target Table**: Populate `project.dataset.sof$ta_period` with some existing data (e.g., `(999, 1, 'OLD', 'OLD', '1970-01-01')`) to test the `TRUNCATE` behavior and ensure no partial inserts.
4.  **Simulate Error**: Temporarily modify `d_ausd_v_ta_period` to intentionally cause an error (e.g., `SELECT 1/0;` or `RAISE EXCEPTION 'Simulated transformation error.';` within the `INSERT` statement).

**Action:**
Execute the BigQuery orchestration procedure:
```sql
CALL `project.dataset.r_ausd_vertrag_control`(
    p_job_kennung => 'TEST_JOB_FAIL',
    p_eintragsnr => '131',
    p_carmen_project => 'project',
    p_carmen_dataset => 'dataset',
    p_as_of_date => NULL
);
```

**Pass/Fail Criterion:**
1.  **`sof$ta_period`**:
    *   `project.dataset.sof$ta_period` must be empty (0 records). This confirms that the `TRUNCATE` occurred, but the subsequent `INSERT` failed and was rolled back (or the procedure's `EXCEPTION` block handled it, preventing partial data).
2.  **Audit Log (`job_audit`)**:
    *   A new record must exist in `project.dataset.job_audit` with `job_id = 'k_ausd_v_ta_period'` and `status = 'FAILED'`.
    *   `processed_records` must be `0`.
    *   `error_message` must contain a message related to the simulated error (e.g., "Simulated transformation error." or "division by zero").
    *   `log_details` should contain the detailed error information, including SQLSTATE and SQLCODE if available.

### Test Case 7: External System Replacement - `TRUNCATE` Behavior

**Purpose:**
To verify that the `TRUNCATE TABLE` operation in `d_ausd_v_ta_period` correctly mimics the legacy behavior of clearing the target table before inserting new data, as implied by `DWPA_UTIL_SKRIPT.runstatement`.

**Setup:**
1.  **BigQuery Tables**: Same as Test Case 1.
2.  **Source Data (BigQuery)**:
    *   `project.dataset.cds$ta_period`: Insert 2 valid records that *should* be processed.
    *   `project.dataset.cds$ta_time_meas_cv` and `project.dataset.cds$ta_description` with matching data.
    *   `project.isbert_schema.dwtk_meldungen`: Insert a record for `JOB_KENNUNG = 'TEST_JOB_TRUNCATE'` with `TIMECREATED = '2023-05-01 10:00:00 UTC'`.
3.  **Target Table (`sof$ta_period`)**: Pre-populate `project.dataset.sof$ta_period` with 5 "old" records.
    ```sql
    INSERT INTO `project.dataset.sof$ta_period` (period_id, number_time_measurement, time_meas_cv, einheit, bfc_age) VALUES
    (1000, 1, 'OLD1', 'Old Unit', '2022-01-01'),
    (1001, 2, 'OLD2', 'Old Unit', '2022-01-02'),
    (1002, 3, 'OLD3', 'Old Unit', '2022-01-03'),
    (1003, 4, 'OLD4', 'Old Unit', '2022-01-04'),
    (1004, 5, 'OLD5', 'Old Unit', '2022-01-05');
    ```

**Action:**
Execute the BigQuery orchestration procedure:
```sql
CALL `project.dataset.r_ausd_vertrag_control`(
    p_job_kennung => 'TEST_JOB_TRUNCATE',
    p_eintragsnr => '132',
    p_carmen_project => 'project',
    p_carmen_dataset => 'dataset',
    p_as_of_date => NULL
);
```

**Pass/Fail Criterion:**
1.  **`sof$ta_period`**:
    *   The final row count in `project.dataset.sof$ta_period` must be exactly 2 (matching the newly processed records from `cds$ta_period`).
    *   None of the pre-populated "old" records (period_id 1000-1004) should be present in `project.dataset.sof$ta_period`.
2.  **Audit Log (`job_audit`)**:
    *   A new record must exist in `project.dataset.job_audit` with `job_id = 'k_ausd_v_ta_period'` and `status = 'SUCCESS'`.
    *   `processed_records` must be `2`.

---

These test cases cover the critical aspects of the migration, including output parity, transformation logic, error handling, parameter validation, and the replacement of external system interactions (Oracle reads, temporary file communication, and `TRUNCATE` behavior). The identified discrepancies (VIA table, job activation/deactivation logic) should be addressed in the migration plan or code if they are critical to the legacy system's functionality.