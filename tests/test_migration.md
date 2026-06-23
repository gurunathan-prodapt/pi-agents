The following migration validation tests are designed to ensure the BigQuery implementation of `k_ausd_v_ta_inv_acc.ksh` and `d_ausd_v_ta_inv_acc.sql` is behaviorally equivalent to the legacy system. The tests cover output parity, transformation correctness, external system replacements, and data quality assertions.

**Assumptions for Test Execution:**
*   A BigQuery project (`your-gcp-project-id`) and two datasets (`your_dataset_id` for control tables/SPs, `your_sof_dataset_id` for source/target data tables) exist.
*   The BigQuery `job_table`, `error_log`, `d_ausd_v_ta_inv_acc` (SP), and `r_ausd_vertrag_control` (SP) have been deployed to `your-gcp-project-id.your_dataset_id`.
*   The source and target data tables (`sof$ta_inv_assign`, `sof$ta_inv_def`, `sof$ta_acc_ref`, `sof$ta_inv_acc`) have been deployed to `your-gcp-project-id.your_sof_dataset_id` with the following schemas:
    *   `sof$ta_inv_assign`: `cntrct_id STRING, inv_definition_id STRING`
    *   `sof$ta_inv_def`: `inv_definition_id STRING, inv_pay_ty_cv STRING, inv_media_cv STRING, billcycle_id STRING, sales_tax_freed BOOL, acc_ref_id STRING, rechn_inh_konfig_text STRING`
    *   `sof$ta_acc_ref`: `acc_ref_id STRING, account_reference STRING`
    *   `sof$ta_inv_acc`: `cntrct_id STRING, inv_definition_id STRING, inv_pay_ty_cv STRING, inv_media_cv STRING, billcycle_id STRING, sales_tax_freed BOOL, account_reference STRING, rechn_inh_konfig_text STRING`
*   A `pytest` framework with `google.cloud.bigquery` client is used for test orchestration. Placeholders like `PROJECT_ID`, `DATASET_ID`, `SOF_DATASET_ID` should be replaced with actual values.

---

### Test Case 1: Parameter Validation - Missing `p_JobKennung`

*   **Purpose**: Verify that the migrated BigQuery stored procedure correctly handles a missing `p_JobKennung` parameter, logs an error, and updates the `job_table` accordingly. This mirrors the `pruefeParameterGesetzt Jobkennung p_JobKennung` and `DWMSG_MeldeFehler` behavior in the ksh script.
*   **Setup**:
    1.  Clear all relevant tables: `job_table`, `error_log`, `sof$ta_inv_acc`.
*   **Action**:
    1.  Call the control stored procedure `r_ausd_vertrag_control` with `p_JobKennung` as `NULL` and a valid `p_EintragsNr`.

    ```python
    # Python (pytest) code snippet
    from google.cloud import bigquery

    client = bigquery.Client(project=PROJECT_ID)
    _clear_tables(client) # Helper to truncate all relevant tables

    job_kennung = None
    eintrags_nr = "TEST_ENTRY_001"

    # Attempt to call the procedure
    success, error_msg = _call_control_procedure(client, job_kennung, eintrags_nr)
    assert not success # Expecting failure
    ```
*   **Pass/Fail Criterion**:
    1.  The procedure call should raise an error (e.g., `SIGNAL SQLSTATE` in BigQuery).
    2.  Query `job_table`: It should contain one entry for the attempted job with `active_flag = FALSE`, `error_code = 193`, and `error_message` containing "Jobkennung ist nicht gesetzt."
    3.  Query `error_log`: It should contain one entry with `error_nr = 193` and the corresponding error message.
    4.  Query `sof$ta_inv_acc`: It should be empty.

    ```sql
    -- SQL Assertions
    SELECT COUNT(*) FROM `PROJECT_ID.DATASET_ID.job_table` WHERE active_flag = FALSE AND error_code = 193 AND error_message LIKE '%Jobkennung ist nicht gesetzt%'; -- Should be 1
    SELECT COUNT(*) FROM `PROJECT_ID.DATASET_ID.error_log` WHERE error_nr = 193 AND message LIKE '%Jobkennung ist nicht gesetzt%'; -- Should be 1
    SELECT COUNT(*) FROM `PROJECT_ID.SOF_DATASET_ID.sof$ta_inv_acc`; -- Should be 0
    ```

---

### Test Case 2: Parameter Validation - Missing `p_EintragsNr`

*   **Purpose**: Verify that the migrated BigQuery stored procedure correctly handles a missing `p_EintragsNr` parameter, logs an error, and updates the `job_table` accordingly. This mirrors the `pruefeParameterGesetzt EintragsNr p_EintragsNr` and `DWMSG_MeldeFehler` behavior in the ksh script.
*   **Setup**:
    1.  Clear all relevant tables: `job_table`, `error_log`, `sof$ta_inv_acc`.
*   **Action**:
    1.  Call the control stored procedure `r_ausd_vertrag_control` with a valid `p_JobKennung` and `p_EintragsNr` as `NULL`.

    ```python
    # Python (pytest) code snippet
    client = bigquery.Client(project=PROJECT_ID)
    _clear_tables(client)

    job_kennung = "VALID_JOB_002"
    eintrags_nr = None

    success, error_msg = _call_control_procedure(client, job_kennung, eintrags_nr)
    assert not success # Expecting failure
    ```
*   **Pass/Fail Criterion**:
    1.  The procedure call should raise an error.
    2.  Query `job_table`: It should contain one entry for the attempted job with `active_flag = FALSE`, `error_code = 193`, and `error_message` containing "EintragsNr ist nicht gesetzt."
    3.  Query `error_log`: It should contain one entry with `error_nr = 193` and the corresponding error message.
    4.  Query `sof$ta_inv_acc`: It should be empty.

    ```sql
    -- SQL Assertions
    SELECT COUNT(*) FROM `PROJECT_ID.DATASET_ID.job_table` WHERE active_flag = FALSE AND error_code = 193 AND error_message LIKE '%EintragsNr ist nicht gesetzt%'; -- Should be 1
    SELECT COUNT(*) FROM `PROJECT_ID.DATASET_ID.error_log` WHERE error_nr = 193 AND message LIKE '%EintragsNr ist nicht gesetzt%'; -- Should be 1
    SELECT COUNT(*) FROM `PROJECT_ID.SOF_DATASET_ID.sof$ta_inv_acc`; -- Should be 0
    ```

---

### Test Case 3: Successful Initial Job Execution (Output Parity & Transformation Correctness)

*   **Purpose**: Verify the end-to-end successful execution of the migrated job, including job control, data transformation, and record count update. This covers output parity and transformation correctness for a happy path.
*   **Setup**:
    1.  Clear all relevant tables.
    2.  Populate `sof$ta_inv_assign`, `sof$ta_inv_def`, `sof$ta_acc_ref` with sample data that will result in a specific number of rows after joins.

    ```python
    # Python (pytest) code snippet
    client = bigquery.Client(project=PROJECT_ID)
    _clear_tables(client)

    inv_assign_data = [("C1", "ID1"), ("C2", "ID2")]
    inv_def_data = [("ID1", "PAY1", "MEDIA1", "BC1", True, "AR1", "TEXT1"), ("ID2", "PAY2", "MEDIA2", "BC2", False, "AR2", "TEXT2")]
    acc_ref_data = [("AR1", "ACC_REF_1"), ("AR2", "ACC_REF_2")]
    _insert_sof_data(client, inv_assign_data, inv_def_data, acc_ref_data)

    job_kennung = "SUCCESS_JOB_003"
    eintrags_nr = "ENTRY_003"
    ```
*   **Action**:
    1.  Call `my_project.my_dataset.r_ausd_vertrag_control` with valid `p_JobKennung` and `p_EintragsNr`.

    ```python
    # Python (pytest) code snippet
    success, error_msg = _call_control_procedure(client, job_kennung, eintrags_nr)
    assert success # Expecting success
    ```
*   **Pass/Fail Criterion**:
    1.  The procedure should complete without error.
    2.  Query `sof$ta_inv_acc`: It should contain 2 rows with the correct transformed data.
        *   Row 1: `('C1', 'ID1', 'PAY1', 'MEDIA1', 'BC1', TRUE, 'ACC_REF_1', 'TEXT1')`
        *   Row 2: `('C2', 'ID2', 'PAY2', 'MEDIA2', 'BC2', FALSE, 'ACC_REF_2', 'TEXT2')`
    3.  Query `job_table`: It should contain one entry for the job with `job_kennung = 'SUCCESS_JOB_003'`, `eintrags_nr = 'ENTRY_003'`, `active_flag = FALSE`, `error_code = 0`, `error_message = 'Successfully completed'`, and `record_count = 2`.
    4.  Query `error_log`: It should be empty.

    ```sql
    -- SQL Assertions
    SELECT COUNT(*) FROM `PROJECT_ID.SOF_DATASET_ID.sof$ta_inv_acc`; -- Should be 2
    SELECT * FROM `PROJECT_ID.SOF_DATASET_ID.sof$ta_inv_acc` ORDER BY cntrct_id;
    -- Expected:
    -- C1, ID1, PAY1, MEDIA1, BC1, TRUE, ACC_REF_1, TEXT1
    -- C2, ID2, PAY2, MEDIA2, BC2, FALSE, ACC_REF_2, TEXT2

    SELECT COUNT(*) FROM `PROJECT_ID.DATASET_ID.job_table`
    WHERE job_kennung = 'SUCCESS_JOB_003'
      AND eintrags_nr = 'ENTRY_003'
      AND active_flag = FALSE
      AND error_code = 0
      AND error_message = 'Successfully completed'
      AND record_count = 2; -- Should be 1

    SELECT COUNT(*) FROM `PROJECT_ID.DATASET_ID.error_log`; -- Should be 0
    ```

---

### Test Case 4: Active Job Ignored (Job Control)

*   **Purpose**: Verify that if an active job with the same `job_kennung` and `tab_name` already exists, the new execution is ignored, mirroring the ksh script's "aktive Jobs werden ignoriert" logic.
*   **Setup**:
    1.  Clear all relevant tables.
    2.  Insert an entry into `job_table` with `job_kennung = 'ACTIVE_JOB_004'`, `tab_name = 'ta_inv_acc'`, `active_flag = TRUE`.
    3.  Populate source tables with some data.
    4.  Ensure `sof$ta_inv_acc` is empty.

    ```python
    # Python (pytest) code snippet
    client = bigquery.Client(project=PROJECT_ID)
    _clear_tables(client)

    _insert_job_table_entry(client, "ACTIVE_JOB_004", "OLD_ENTRY_004", "ta_inv_acc", True)
    inv_assign_data = [("C3", "ID3")]
    inv_def_data = [("ID3", "PAY3", "MEDIA3", "BC3", True, "AR3", "TEXT3")]
    acc_ref_data = [("AR3", "ACC_REF_3")]
    _insert_sof_data(client, inv_assign_data, inv_def_data, acc_ref_data)

    job_kennung = "ACTIVE_JOB_004" # Same job_kennung
    eintrags_nr = "NEW_ENTRY_004" # New eintrags_nr
    ```
*   **Action**:
    1.  Call `my_project.my_dataset.r_ausd_vertrag_control` with the same `p_JobKennung` as the active job, and a new `p_EintragsNr`.

    ```python
    # Python (pytest) code snippet
    success, error_msg = _call_control_procedure(client, job_kennung, eintrags_nr)
    assert success # Expecting success (graceful return, not an error)
    ```
*   **Pass/Fail Criterion**:
    1.  The procedure should complete without raising an error (it should `RETURN` gracefully).
    2.  Query `sof$ta_inv_acc`: It should remain empty (no data transformation should occur).
    3.  Query `job_table`: It should still contain only the *original* active entry (`job_kennung = 'ACTIVE_JOB_004'`, `eintrags_nr = 'OLD_ENTRY_004'`, `active_flag = TRUE`). No new entry should be created for the ignored run.
    4.  Query `error_log`: It should contain an entry indicating that an active job was ignored.

    ```sql
    -- SQL Assertions
    SELECT COUNT(*) FROM `PROJECT_ID.SOF_DATASET_ID.sof$ta_inv_acc`; -- Should be 0
    SELECT COUNT(*) FROM `PROJECT_ID.DATASET_ID.job_table`; -- Should be 1
    SELECT COUNT(*) FROM `PROJECT_ID.DATASET_ID.job_table`
    WHERE job_kennung = 'ACTIVE_JOB_004' AND eintrags_nr = 'OLD_ENTRY_004' AND active_flag = TRUE; -- Should be 1
    SELECT COUNT(*) FROM `PROJECT_ID.DATASET_ID.error_log`
    WHERE message LIKE '%Active job for Jobkennung: ACTIVE_JOB_004 and TabName: ta_inv_acc already exists. Ignoring execution%'; -- Should be 1
    ```

---

### Test Case 5: Deactivation of Old Active Jobs (Job Control)

*   **Purpose**: Verify that a new job run deactivates *other* active jobs for the same `tab_name` but a different `job_kennung`, mirroring the ksh script's "alte aktive Jobs werden einfach dekativiert" logic.
*   **Setup**:
    1.  Clear all relevant tables.
    2.  Insert an entry into `job_table` with `job_kennung = 'OLD_ACTIVE_JOB_005'`, `tab_name = 'ta_inv_acc'`, `active_flag = TRUE`.
    3.  Populate source tables with data that should be processed by the new job.
    4.  Ensure `sof$ta_inv_acc` is empty.

    ```python
    # Python (pytest) code snippet
    client = bigquery.Client(project=PROJECT_ID)
    _clear_tables(client)

    _insert_job_table_entry(client, "OLD_ACTIVE_JOB_005", "OLD_ENTRY_005", "ta_inv_acc", True)
    inv_assign_data = [("C4", "ID4")]
    inv_def_data = [("ID4", "PAY4", "MEDIA4", "BC4", True, "AR4", "TEXT4")]
    acc_ref_data = [("AR4", "ACC_REF_4")]
    _insert_sof_data(client, inv_assign_data, inv_def_data, acc_ref_data)

    job_kennung = "NEW_JOB_005"
    eintrags_nr = "NEW_ENTRY_005"
    ```
*   **Action**:
    1.  Call `my_project.my_dataset.r_ausd_vertrag_control` with `p_JobKennung = 'NEW_JOB_005'` and `p_EintragsNr = 'NEW_ENTRY_005'`.

    ```python
    # Python (pytest) code snippet
    success, error_msg = _call_control_procedure(client, job_kennung, eintrags_nr)
    assert success # Expecting success
    ```
*   **Pass/Fail Criterion**:
    1.  The procedure should complete successfully.
    2.  Query `job_table`:
        *   The entry for `OLD_ACTIVE_JOB_005` should have `active_flag = FALSE`, `completed_ts` updated, and `error_message = 'Deactivated by new job run'`.
        *   A new entry for `NEW_JOB_005` should exist, marked as successfully completed (`active_flag = FALSE`, `error_code = 0`, `record_count = 1`).
    3.  Query `sof$ta_inv_acc`: It should contain the correctly transformed data (1 row).

    ```sql
    -- SQL Assertions
    SELECT COUNT(*) FROM `PROJECT_ID.DATASET_ID.job_table`
    WHERE job_kennung = 'OLD_ACTIVE_JOB_005'
      AND active_flag = FALSE
      AND error_message = 'Deactivated by new job run'; -- Should be 1

    SELECT COUNT(*) FROM `PROJECT_ID.DATASET_ID.job_table`
    WHERE job_kennung = 'NEW_JOB_005'
      AND eintrags_nr = 'NEW_ENTRY_005'
      AND active_flag = FALSE
      AND error_code = 0
      AND record_count = 1; -- Should be 1

    SELECT COUNT(*) FROM `PROJECT_ID.SOF_DATASET_ID.sof$ta_inv_acc`; -- Should be 1
    ```

---

### Test Case 6: Data Transformation Correctness - Joins and Column Mapping

*   **Purpose**: Verify that the `d_ausd_v_ta_inv_acc` stored procedure correctly performs the joins and maps columns as specified in the SQL. This is a core transformation correctness test.
*   **Setup**:
    1.  Clear all relevant tables.
    2.  Populate `sof$ta_inv_assign`, `sof$ta_inv_def`, `sof$ta_acc_ref` with diverse sample data, including cases where joins might not match (to ensure inner join behavior) and specific values for each column.

    ```python
    # Python (pytest) code snippet
    client = bigquery.Client(project=PROJECT_ID)
    _clear_tables(client)

    inv_assign_data = [
        ("C_A", "ID_X"), # Matches all
        ("C_B", "ID_Y"), # Matches all
        ("C_C", "ID_Z"), # ID_Z not in inv_def, should not join
        ("C_D", "ID_X")  # Duplicate inv_definition_id in assign, should produce duplicate output
    ]
    inv_def_data = [
        ("ID_X", "PAY_X", "MEDIA_X", "BC_X", True, "AR_P", "TEXT_X"), # Matches all
        ("ID_Y", "PAY_Y", "MEDIA_Y", "BC_Y", False, "AR_Q", "TEXT_Y"), # Matches all
        ("ID_W", "PAY_W", "MEDIA_W", "BC_W", True, "AR_R", "TEXT_W")  # ID_W not in inv_assign, should not join
    ]
    acc_ref_data = [
        ("AR_P", "ACC_REF_P"), # Matches ID_X
        ("AR_Q", "ACC_REF_Q"), # Matches ID_Y
        ("AR_S", "ACC_REF_S")  # AR_S not in inv_def, should not join
    ]
    _insert_sof_data(client, inv_assign_data, inv_def_data, acc_ref_data)

    job_kennung = "TRANSFORM_JOB_006"
    eintrags_nr = "ENTRY_006"
    ```
*   **Action**:
    1.  Call `my_project.my_dataset.r_ausd_vertrag_control` with valid parameters.

    ```python
    # Python (pytest) code snippet
    success, error_msg = _call_control_procedure(client, job_kennung, eintrags_nr)
    assert success # Expecting success
    ```
*   **Pass/Fail Criterion**:
    1.  The procedure should complete successfully.
    2.  Query `sof$ta_inv_acc`: It should contain 3 rows. The content of these rows must exactly match the expected output from the join logic.
        *   Expected Rows (order may vary):
            *   `('C_A', 'ID_X', 'PAY_X', 'MEDIA_X', 'BC_X', TRUE, 'ACC_REF_P', 'TEXT_X')`
            *   `('C_B', 'ID_Y', 'PAY_Y', 'MEDIA_Y', 'BC_Y', FALSE, 'ACC_REF_Q', 'TEXT_Y')`
            *   `('C_D', 'ID_X', 'PAY_X', 'MEDIA_X', 'BC_X', TRUE, 'ACC_REF_P', 'TEXT_X')`
    3.  `job_table` should show `record_count = 3` for this job.

    ```sql
    -- SQL Assertions
    SELECT COUNT(*) FROM `PROJECT_ID.SOF_DATASET_ID.sof$ta_inv_acc`; -- Should be 3

    -- Detailed check for content
    SELECT
        cntrct_id, inv_definition_id, inv_pay_ty_cv, inv_media_cv, billcycle_id, sales_tax_freed, account_reference, rechn_inh_konfig_text
    FROM `PROJECT_ID.SOF_DATASET_ID.sof$ta_inv_acc`
    ORDER BY cntrct_id, inv_definition_id;
    -- Expected output (verify row by row):
    -- C_A, ID_X, PAY_X, MEDIA_X, BC_X, TRUE, ACC_REF_P, TEXT_X
    -- C_B, ID_Y, PAY_Y, MEDIA_Y, BC_Y, FALSE, ACC_REF_Q, TEXT_Y
    -- C_D, ID_X, PAY_X, MEDIA_X, BC_X, TRUE, ACC_REF_P, TEXT_X

    SELECT record_count FROM `PROJECT_ID.DATASET_ID.job_table`
    WHERE job_kennung = 'TRANSFORM_JOB_006' AND error_code = 0; -- Should return 3
    ```

---

### Test Case 7: Data Transformation Correctness - Empty Source Tables

*   **Purpose**: Verify that the job handles cases where one or more source tables are empty, resulting in no output, and the job status reflects this.
*   **Setup**:
    1.  Clear all relevant tables.
    2.  Make `sof$ta_inv_assign` empty. Populate `sof$ta_inv_def` and `sof$ta_acc_ref` with data (to ensure the join correctly filters out everything).
    3.  Ensure `sof$ta_inv_acc` is empty.

    ```python
    # Python (pytest) code snippet
    client = bigquery.Client(project=PROJECT_ID)
    _clear_tables(client)

    inv_assign_data = [] # Empty
    inv_def_data = [("ID_E", "PAY_E", "MEDIA_E", "BC_E", True, "AR_E", "TEXT_E")]
    acc_ref_data = [("AR_E", "ACC_REF_E")]
    _insert_sof_data(client, inv_assign_data, inv_def_data, acc_ref_data)

    job_kennung = "EMPTY_SOURCE_JOB_007"
    eintrags_nr = "ENTRY_007"
    ```
*   **Action**:
    1.  Call `my_project.my_dataset.r_ausd_vertrag_control` with valid parameters.

    ```python
    # Python (pytest) code snippet
    success, error_msg = _call_control_procedure(client, job_kennung, eintrags_nr)
    assert success # Expecting success
    ```
*   **Pass/Fail Criterion**:
    1.  The procedure should complete successfully.
    2.  Query `sof$ta_inv_acc`: It should remain empty.
    3.  Query `job_table`: The entry for this job should show `record_count = 0` and successful completion (`error_code = 0`).

    ```sql
    -- SQL Assertions
    SELECT COUNT(*) FROM `PROJECT_ID.SOF_DATASET_ID.sof$ta_inv_acc`; -- Should be 0
    SELECT record_count FROM `PROJECT_ID.DATASET_ID.job_table`
    WHERE job_kennung = 'EMPTY_SOURCE_JOB_007' AND error_code = 0; -- Should return 0
    ```

---

### Test Case 8: Data Transformation Correctness - Truncate Behavior

*   **Purpose**: Verify that the `d_ausd_v_ta_inv_acc` procedure correctly truncates the target table before inserting new data, ensuring idempotency for the data load.
*   **Setup**:
    1.  Clear `job_table` and `error_log`.
    2.  Populate `sof$ta_inv_acc` with some initial "old" data.
    3.  Populate source tables with "new" data that should replace the old data.

    ```python
    # Python (pytest) code snippet
    client = bigquery.Client(project=PROJECT_ID)
    _clear_tables(client) # Clears all, including sof$ta_inv_acc initially

    # Insert "old" data directly into target table
    client.query(f"""
        INSERT INTO `{PROJECT_ID}.{SOF_DATASET_ID}.sof$ta_inv_acc`
        (cntrct_id, inv_definition_id, inv_pay_ty_cv, inv_media_cv, billcycle_id, sales_tax_freed, account_reference, rechn_inh_konfig_text)
        VALUES ('OLD_C1', 'OLD_ID1', 'OLD_PAY1', 'OLD_MEDIA1', 'OLD_BC1', TRUE, 'OLD_ACC_REF1', 'OLD_TEXT1')
    """).result()

    # Populate source tables with "new" data
    inv_assign_data = [("NEW_C1", "NEW_ID1")]
    inv_def_data = [("NEW_ID1", "NEW_PAY1", "NEW_MEDIA1", "NEW_BC1", FALSE, "NEW_AR1", "NEW_TEXT1")]
    acc_ref_data = [("NEW_AR1", "NEW_ACC_REF1")]
    _insert_sof_data(client, inv_assign_data, inv_def_data, acc_ref_data)

    job_kennung = "TRUNCATE_JOB_008"
    eintrags_nr = "ENTRY_008"
    ```
*   **Action**:
    1.  Call `my_project.my_dataset.r_ausd_vertrag_control` with valid parameters.

    ```python
    # Python (pytest) code snippet
    success, error_msg = _call_control_procedure(client, job_kennung, eintrags_nr)
    assert success # Expecting success
    ```
*   **Pass/Fail Criterion**:
    1.  The procedure should complete successfully.
    2.  Query `sof$ta_inv_acc`: It should contain *only* the data derived from the current run's source tables (1 row: `('NEW_C1', 'NEW_ID1', 'NEW_PAY1', 'NEW_MEDIA1', 'NEW_BC1', FALSE, 'NEW_ACC_REF1', 'NEW_TEXT1')`). The "old" data should be gone.
    3.  The `record_count` in `job_table` should reflect the count of the "new" data (1).

    ```sql
    -- SQL Assertions
    SELECT COUNT(*) FROM `PROJECT_ID.SOF_DATASET_ID.sof$ta_inv_acc`; -- Should be 1
    SELECT
        cntrct_id, inv_definition_id, inv_pay_ty_cv, inv_media_cv, billcycle_id, sales_tax_freed, account_reference, rechn_inh_konfig_text
    FROM `PROJECT_ID.SOF_DATASET_ID.sof$ta_inv_acc`;
    -- Expected: NEW_C1, NEW_ID1, NEW_PAY1, NEW_MEDIA1, NEW_BC1, FALSE, NEW_ACC_REF1, NEW_TEXT1

    SELECT record_count FROM `PROJECT_ID.DATASET_ID.job_table`
    WHERE job_kennung = 'TRUNCATE_JOB_008' AND error_code = 0; -- Should return 1
    ```

---

### Test Case 9: Error during Data Transformation (`d_ausd_v_ta_inv_acc` fails)

*   **Purpose**: Verify that if the underlying data transformation procedure (`d_ausd_v_ta_inv_acc`) fails (e.g., due to schema mismatch, data error), the control procedure correctly catches the error, logs it, and updates the `job_table` with an error status.
*   **Setup**:
    1.  Clear all relevant tables.
    2.  Temporarily alter the schema of `sof$ta_inv_acc` to cause an `INSERT` failure. For example, make `cntrct_id` a `NUMERIC` type, which will cause a type mismatch when inserting `STRING` data. (Note: BigQuery doesn't allow direct `ALTER COLUMN TYPE` if data exists, so this might require dropping and recreating the table with a conflicting type for the test, then reverting).
    3.  Populate source tables with valid data for the *original* schema.

    ```python
    # Python (pytest) code snippet - Illustrative, actual BigQuery DDL might be complex
    client = bigquery.Client(project=PROJECT_ID)
    _clear_tables(client)

    # Simulate schema change to cause error:
    # Drop and recreate sof$ta_inv_acc with a conflicting schema for cntrct_id
    client.query(f"DROP TABLE IF EXISTS `{PROJECT_ID}.{SOF_DATASET_ID}.sof$ta_inv_acc`").result()
    client.query(f"""
        CREATE TABLE `{PROJECT_ID}.{SOF_DATASET_ID}.sof$ta_inv_acc` (
            cntrct_id NUMERIC, -- This will cause a type mismatch with STRING source
            inv_definition_id STRING,
            inv_pay_ty_cv STRING,
            inv_media_cv STRING,
            billcycle_id STRING,
            sales_tax_freed BOOL,
            account_reference STRING,
            rechn_inh_konfig_text STRING
        )
    """).result()

    inv_assign_data = [("C_FAIL", "ID_FAIL")]
    inv_def_data = [("ID_FAIL", "PAY_FAIL", "MEDIA_FAIL", "BC_FAIL", TRUE, "AR_FAIL", "TEXT_FAIL")]
    acc_ref_data = [("AR_FAIL", "ACC_REF_FAIL")]
    _insert_sof_data(client, inv_assign_data, inv_def_data, acc_ref_data)

    job_kennung = "FAIL_JOB_009"
    eintrags_nr = "ENTRY_009"
    ```
*   **Action**:
    1.  Call `my_project.my_dataset.r_ausd_vertrag_control` with valid parameters.

    ```python
    # Python (pytest) code snippet
    success, error_msg = _call_control_procedure(client, job_kennung, eintrags_nr)
    assert not success # Expecting failure
    ```
*   **Pass/Fail Criterion**:
    1.  The procedure call should raise an error.
    2.  Query `job_table`: It should contain an entry for the job with `job_kennung = 'FAIL_JOB_009'`, `active_flag = FALSE`, `error_code` not 0 (e.g., -1 or a BigQuery specific error code), and a descriptive `error_message` indicating a type mismatch or similar failure.
    3.  Query `error_log`: It should contain an entry detailing the error from `d_ausd_v_ta_inv_acc`.
    4.  Query `sof$ta_inv_acc`: It should be empty (due to `TRUNCATE` and then `INSERT` failure) or contain partial data if the error occurred mid-insert (less likely with a single `INSERT` statement).

    ```sql
    -- SQL Assertions
    SELECT COUNT(*) FROM `PROJECT_ID.DATASET_ID.job_table`
    WHERE job_kennung = 'FAIL_JOB_009' AND active_flag = FALSE AND error_code <> 0 AND error_message LIKE '%Failed to convert STRING to NUMERIC%'; -- Should be 1

    SELECT COUNT(*) FROM `PROJECT_ID.DATASET_ID.error_log`
    WHERE message LIKE '%Failed to convert STRING to NUMERIC%'; -- Should be 1

    SELECT COUNT(*) FROM `PROJECT_ID.SOF_DATASET_ID.sof$ta_inv_acc`; -- Should be 0
    ```
    *Cleanup*: After this test, `sof$ta_inv_acc` should be dropped and recreated with its correct schema.

---

### Test Case 10: Record Count Accuracy

*   **Purpose**: Verify that the `record_count` stored in `job_table` accurately reflects the number of rows inserted into `sof$ta_inv_acc`. This replaces the `tmpFile` mechanism in the legacy script.
*   **Setup**:
    1.  Clear all relevant tables.
    2.  Populate source tables such that a known number of rows (e.g., 5) will be inserted into `sof$ta_inv_acc`.

    ```python
    # Python (pytest) code snippet
    client = bigquery.Client(project=PROJECT_ID)
    _clear_tables(client)

    inv_assign_data = [("C1", "ID1"), ("C2", "ID2"), ("C3", "ID3"), ("C4", "ID4"), ("C5", "ID5")]
    inv_def_data = [
        ("ID1", "P1", "M1", "B1", True, "AR1", "T1"),
        ("ID2", "P2", "M2", "B2", False, "AR2", "T2"),
        ("ID3", "P3", "M3", "B3", True, "AR3", "T3"),
        ("ID4", "P4", "M4", "B4", False, "AR4", "T4"),
        ("ID5", "P5", "M5", "B5", True, "AR5", "T5")
    ]
    acc_ref_data = [
        ("AR1", "AC1"), ("AR2", "AC2"), ("AR3", "AC3"), ("AR4", "AC4"), ("AR5", "AC5")
    ]
    _insert_sof_data(client, inv_assign_data, inv_def_data, acc_ref_data)

    job_kennung = "COUNT_JOB_010"
    eintrags_nr = "ENTRY_010"
    ```
*   **Action**:
    1.  Call `my_project.my_dataset.r_ausd_vertrag_control` with valid parameters.

    ```python
    # Python (pytest) code snippet
    success, error_msg = _call_control_procedure(client, job_kennung, eintrags_nr)
    assert success # Expecting success
    ```
*   **Pass/Fail Criterion**:
    1.  The procedure should complete successfully.
    2.  Query `sof$ta_inv_acc`: It should contain exactly 5 rows.
    3.  Query `job_table`: The `record_count` in the final `job_table` entry for the job should exactly match 5.

    ```sql
    -- SQL Assertions
    SELECT COUNT(*) FROM `PROJECT_ID.SOF_DATASET_ID.sof$ta_inv_acc`; -- Should be 5
    SELECT record_count FROM `PROJECT_ID.DATASET_ID.job_table`
    WHERE job_kennung = 'COUNT_JOB_010' AND error_code = 0; -- Should return 5
    ```

---

### Test Case 11: External System Replacement - Oracle to BigQuery Native

*   **Purpose**: Confirm that the migration successfully replaced Oracle-specific interactions (SQL*Plus, `DWPA_UTIL_SKRIPT`) with native BigQuery operations. This is a verification of the design's intent for external system replacement.
*   **Setup**: N/A (This is a code review and architectural verification test).
*   **Action**:
    1.  Review the BigQuery Stored Procedure code for `d_ausd_v_ta_inv_acc` and `r_ausd_vertrag_control`.
    2.  Examine the deployment process for these BigQuery components.
*   **Pass/Fail Criterion**:
    1.  The BigQuery stored procedures should only use BigQuery Standard SQL syntax and BigQuery-native features (e.g., `TRUNCATE TABLE`, `INSERT INTO`, `SELECT`, `UPDATE`, `DECLARE`, `SET`, `CALL`, `EXCEPTION WHEN ERROR`, `SIGNAL SQLSTATE`).
    2.  There should be no calls to external systems, shell commands, or Oracle-specific functions/packages (e.g., `SQL*Plus`, `DWPA_UTIL_SKRIPT`).
    3.  The deployment process should involve BigQuery DDL/DML commands or BigQuery Data Transfer Service, not external connectors to Oracle for runtime execution.

---

### Helper Functions for Pytest (Conceptual)

```python
import pytest
from google.cloud import bigquery
import time

# --- Configuration ---
PROJECT_ID = "your-gcp-project-id"
DATASET_ID = "your_dataset_id" # For job_table, error_log, and control SPs
SOF_DATASET_ID = "your_sof_dataset_id" # For source/target tables like sof$ta_inv_acc

# --- BigQuery Client (would be in conftest.py for a real project) ---
@pytest.fixture(scope="module")
def bq_client():
    return bigquery.Client(project=PROJECT_ID)

# --- Helper Functions ---
def _clear_tables(bq_client):
    """Truncates all tables relevant to the tests."""
    tables_to_clear = [
        f"{PROJECT_ID}.{DATASET_ID}.job_table",
        f"{PROJECT_ID}.{DATASET_ID}.error_log",
        f"{PROJECT_ID}.{SOF_DATASET_ID}.sof$ta_inv_assign",
        f"{PROJECT_ID}.{SOF_DATASET_ID}.sof$ta_inv_def",
        f"{PROJECT_ID}.{SOF_DATASET_ID}.sof$ta_acc_ref",
        f"{PROJECT_ID}.{SOF_DATASET_ID}.sof$ta_inv_acc",
    ]
    for table_id in tables_to_clear:
        try:
            bq_client.query(f"TRUNCATE TABLE `{table_id}`").result()
        except Exception as e:
            print(f"Warning: Could not truncate {table_id}. It might not exist or schema is temporary. Error: {e}")

def _insert_job_table_entry(bq_client, job_kennung, eintrags_nr, tab_name, active_flag, error_code=None, error_message=None):
    """Inserts a record into the job_table for testing job control logic."""
    query = f"""
    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.job_table` (job_kennung, eintrags_nr, tab_name, active_flag, created_ts, error_code, error_message)
    VALUES (
        '{job_kennung}',
        '{eintrags_nr}',
        '{tab_name}',
        {active_flag},
        CURRENT_TIMESTAMP(),
        {error_code if error_code is not None else 'NULL'},
        '{error_message if error_message is not None else ''}'
    )
    """
    bq_client.query(query).result()

def _insert_sof_data(bq_client, inv_assign_data, inv_def_data, acc_ref_data):
    """Inserts sample data into the source tables."""
    if inv_assign_data:
        values = ", ".join([f"('{d[0]}', '{d[1]}')" for d in inv_assign_data])
        bq_client.query(f"INSERT INTO `{PROJECT_ID}.{SOF_DATASET_ID}.sof$ta_inv_assign` (cntrct_id, inv_definition_id) VALUES {values}").result()
    if inv_def_data:
        values = ", ".join([f"('{d[0]}', '{d[1]}', '{d[2]}', '{d[3]}', {str(d[4]).upper()}, '{d[5]}', '{d[6]}')" for d in inv_def_data])
        bq_client.query(f"INSERT INTO `{PROJECT_ID}.{SOF_DATASET_ID}.sof$ta_inv_def` (inv_definition_id, inv_pay_ty_cv, inv_media_cv, billcycle_id, sales_tax_freed, acc_ref_id, rechn_inh_konfig_text) VALUES {values}").result()
    if acc_ref_data:
        values = ", ".join([f"('{d[0]}', '{d[1]}')" for d in acc_ref_data])
        bq_client.query(f"INSERT INTO `{PROJECT_ID}.{SOF_DATASET_ID}.sof$ta_acc_ref` (acc_ref_id, account_reference) VALUES {values}").result()

def _call_control_procedure(bq_client, job_kennung, eintrags_nr):
    """Calls the main control stored procedure."""
    # Handle NULL parameters for BigQuery CALL statement
    jk_param = f"'{job_kennung}'" if job_kennung is not None else "NULL"
    en_param = f"'{eintrags_nr}'" if eintrags_nr is not None else "NULL"
    query = f"CALL `{PROJECT_ID}.{DATASET_ID}.r_ausd_vertrag_control`({jk_param}, {en_param})"
    try:
        bq_client.query(query).result()
        return True, None
    except Exception as e:
        return False, str(e)

def _get_table_rows(bq_client, table_id):
    """Fetches all rows from a given table."""
    query = f"SELECT * FROM `{table_id}`"
    return list(bq_client.query(query).result())

def _get_row_count(bq_client, table_id):
    """Fetches the row count for a given table."""
    query = f"SELECT COUNT(*) FROM `{table_id}`"
    return list(bq_client.query(query).result())[0][0]

# Example of how a test function would look in pytest
# @pytest.mark.parametrize("job_kennung, eintrags_nr, expected_error_code", [
#     (None, "ENTRY1", 193),
#     ("JOB1", None, 193),
# ])
# def test_parameter_validation(bq_client, job_kennung, eintrags_nr, expected_error_code):
#     _clear_tables(bq_client)
#     success, error_msg = _call_control_procedure(bq_client, job_kennung, eintrags_nr)
#     assert not success
#     job_table_rows = _get_table_rows(bq_client, f"{PROJECT_ID}.{DATASET_ID}.job_table")
#     assert len(job_table_rows) == 1
#     assert job_table_rows[0].error_code == expected_error_code
#     # ... further assertions
```