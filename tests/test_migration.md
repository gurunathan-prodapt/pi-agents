The migration of `k_ausd_v_ta_cntrct_templ.ksh` to a BigQuery Stored Procedure involves significant changes in orchestration, data transformation, and error handling. The following tests aim to ensure behavioral equivalence and data integrity.

---

## Migration Validation Tests for `k_ausd_v_ta_cntrct_templ.ksh`

### Test Setup Prerequisites

Before running any tests, ensure the following BigQuery resources are created:

1.  **Target Table DDL:**
    ```sql
    CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.ta_cntrct_templ` (
        cntrct_template_id INT64,
        cds_description_id INT64,
        cds_description STRING
    );
    ```

2.  **Audit Table DDL:**
    ```sql
    CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.job_audit` (
        job_id STRING NOT NULL,
        entry_number STRING NOT NULL,
        start_time TIMESTAMP NOT NULL,
        end_time TIMESTAMP,
        status STRING NOT NULL,
        records_processed INT64,
        error_message STRING
    );
    ```

3.  **Mock Source Tables DDL:**
    ```sql
    CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.dwtk_meldungen` (
        job_kennung STRING,
        timecreated TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.cds_ta_cntrct_template` (
        cntrct_template_id INT64,
        cds_description_id INT64,
        insert_at DATE,
        modified_at DATE,
        valid_from DATE,
        valid_to DATE,
        is_production INT64
    );

    CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.cds_ta_care_description` (
        cds_description_id INT64,
        cds_description STRING,
        language INT64
    );
    ```

4.  **BigQuery Stored Procedure:** Ensure the `k_ausd_v_ta_cntrct_templ_sp` procedure is deployed as provided in the `sql/sp/k_ausd_v_ta_cntrct_templ_sp.sql` file.

---

### Test Case 1: Successful Execution - Output Parity & Basic Transformation

**Purpose:** Verify that with valid inputs and typical source data, the BigQuery Stored Procedure (SP) processes data correctly, populates the target table as expected, and logs a successful execution, mirroring the legacy job's intended outcome.

**Setup:**
1.  Clear target and audit tables:
    ```sql
    TRUNCATE TABLE `your_project_id.your_dataset_id.ta_cntrct_templ`;
    TRUNCATE TABLE `your_project_id.your_dataset_id.job_audit`;
    TRUNCATE TABLE `your_project_id.your_dataset_id.dwtk_meldungen`;
    TRUNCATE TABLE `your_project_id.your_dataset_id.cds_ta_cntrct_template`;
    TRUNCATE TABLE `your_project_id.your_dataset_id.cds_ta_care_description`;
    ```
2.  Populate mock source tables with test data:
    ```sql
    INSERT INTO `your_project_id.your_dataset_id.dwtk_meldungen` (job_kennung, timecreated) VALUES
    ('OTHER_JOB', '2023-01-01 10:00:00 UTC'),
    ('BERT_DROP_TEMP_TABLE', '2023-01-15 12:30:00 UTC'), -- This will set v_current_date to 2023-01-15
    ('BERT_DROP_TEMP_TABLE', '2023-01-10 09:00:00 UTC');

    INSERT INTO `your_project_id.your_dataset_id.cds_ta_cntrct_template` (cntrct_template_id, cds_description_id, insert_at, modified_at, valid_from, valid_to, is_production) VALUES
    (101, 1, '2023-01-01', NULL, '2023-01-01', NULL, 1), -- Should be included (valid_to NULL)
    (102, 2, '2023-01-10', '2023-01-20', '2023-01-05', NULL, 1), -- Should be included (modified_at > v_current_date)
    (103, 3, '2023-01-15', NULL, '2023-01-15', '2023-01-25', 1), -- Should be included (valid_to > v_current_date)
    (104, 4, '2023-01-16', NULL, '2023-01-10', NULL, 1), -- Should NOT be included (insert_at > v_current_date)
    (105, 5, '2023-01-01', '2023-01-10', '2023-01-01', NULL, 1), -- Should NOT be included (modified_at <= v_current_date)
    (106, 6, '2023-01-01', NULL, '2023-01-01', '2023-01-10', 1), -- Should NOT be included (valid_to <= v_current_date)
    (107, 7, '2023-01-01', NULL, '2023-01-01', NULL, 0), -- Should NOT be included (is_production = 0)
    (108, 8, '2023-01-01', NULL, '2023-01-01', NULL, 1); -- Should be included

    INSERT INTO `your_project_id.your_dataset_id.cds_ta_care_description` (cds_description_id, cds_description, language) VALUES
    (1, 'Description A', 1),
    (2, 'Description B', 1),
    (3, 'Description C', 1),
    (8, 'Description H', 1),
    (99, 'Description X', 2); -- Should NOT be included (language = 2)
    ```

**Action:**
Execute the BigQuery Stored Procedure:
```sql
CALL `your_project_id.your_dataset_id.k_ausd_v_ta_cntrct_templ_sp`('TEST_JOB_001', 'ENTRY_001');
```

**Pass/Fail Criterion:**
1.  The `ta_cntrct_templ` table contains exactly 4 rows with the following data:
    ```
    cntrct_template_id | cds_description_id | cds_description
    -------------------|--------------------|----------------
    101                | 1                  | Description A
    102                | 2                  | Description B
    103                | 3                  | Description C
    108                | 8                  | Description H
    ```
2.  The `job_audit` table contains one entry for `job_id = 'TEST_JOB_001'` and `entry_number = 'ENTRY_001'` with:
    *   `status = 'SUCCESS'`
    *   `records_processed = 4`
    *   `error_message IS NULL`
    *   `start_time` and `end_time` are populated and `end_time > start_time`.

**Pytest Assertion Example:**
```python
import pytest
from google.cloud import bigquery

def test_successful_execution(bq_client, project_id, dataset_id):
    # Setup (clear and insert data) - omitted for brevity, assume done via SQL
    # ...

    # Action
    query = f"CALL `{project_id}.{dataset_id}.k_ausd_v_ta_cntrct_templ_sp`('TEST_JOB_001', 'ENTRY_001');"
    bq_client.query(query).result()

    # Assertions for ta_cntrct_templ
    result_df = bq_client.query(f"SELECT cntrct_template_id, cds_description_id, cds_description FROM `{project_id}.{dataset_id}.ta_cntrct_templ` ORDER BY cntrct_template_id").to_dataframe()
    expected_data = [
        (101, 1, 'Description A'),
        (102, 2, 'Description B'),
        (103, 3, 'Description C'),
        (108, 8, 'Description H'),
    ]
    assert len(result_df) == len(expected_data)
    for i, row in result_df.iterrows():
        assert (row['cntrct_template_id'], row['cds_description_id'], row['cds_description']) == expected_data[i]

    # Assertions for job_audit
    audit_df = bq_client.query(f"SELECT status, records_processed, error_message FROM `{project_id}.{dataset_id}.job_audit` WHERE job_id = 'TEST_JOB_001' AND entry_number = 'ENTRY_001'").to_dataframe()
    assert len(audit_df) == 1
    assert audit_df.iloc[0]['status'] == 'SUCCESS'
    assert audit_df.iloc[0]['records_processed'] == 4
    assert audit_df.iloc[0]['error_message'] is None
```

---

### Test Case 2: Parameter Validation - Missing `p_JobKennung`

**Purpose:** Verify that the SP correctly handles missing required parameters, raising an error and logging the failure, similar to how the legacy script would exit with an error code.

**Setup:**
1.  Clear the `job_audit` table.
    ```sql
    TRUNCATE TABLE `your_project_id.your_dataset_id.job_audit`;
    ```

**Action:**
Attempt to execute the BigQuery Stored Procedure with `p_JobKennung` as `NULL` or an empty string.
```sql
-- Using NULL
CALL `your_project_id.your_dataset_id.k_ausd_v_ta_cntrct_templ_sp`(NULL, 'ENTRY_002');

-- Or using empty string (if allowed by the calling mechanism)
-- CALL `your_project_id.your_dataset_id.k_ausd_v_ta_cntrct_templ_sp`('', 'ENTRY_002');
```

**Pass/Fail Criterion:**
1.  The `CALL` statement fails with an error message containing "FEHLER: JobKennung (j) ist ein notwendiges Argument."
2.  The `job_audit` table contains one entry for `entry_number = 'ENTRY_002'` (or whatever `p_EintragsNr` was passed) with:
    *   `status = 'FAILED'`
    *   `error_message` containing "FEHLER: JobKennung (j) ist ein notwendiges Argument."
    *   `records_processed IS NULL` or `0`.
    *   `start_time` and `end_time` are populated.

**Pytest Assertion Example:**
```python
import pytest
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest

def test_missing_jobkennung_parameter(bq_client, project_id, dataset_id):
    # Setup
    bq_client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_audit`").result()

    # Action & Assertion
    with pytest.raises(BadRequest) as excinfo:
        bq_client.query(f"CALL `{project_id}.{dataset_id}.k_ausd_v_ta_cntrct_templ_sp`(NULL, 'ENTRY_002');").result()

    assert "FEHLER: JobKennung (j) ist ein notwendiges Argument." in str(excinfo.value)

    # Verify audit log for failure
    audit_df = bq_client.query(f"SELECT status, error_message FROM `{project_id}.{dataset_id}.job_audit` WHERE entry_number = 'ENTRY_002'").to_dataframe()
    assert len(audit_df) == 1
    assert audit_df.iloc[0]['status'] == 'FAILED'
    assert "FEHLER: JobKennung (j) ist ein notwendiges Argument." in audit_df.iloc[0]['error_message']
```

---

### Test Case 3: Parameter Validation - Missing `p_EintragsNr`

**Purpose:** Verify that the SP correctly handles missing required parameters, raising an error and logging the failure.

**Setup:**
1.  Clear the `job_audit` table.
    ```sql
    TRUNCATE TABLE `your_project_id.your_dataset_id.job_audit`;
    ```

**Action:**
Attempt to execute the BigQuery Stored Procedure with `p_EintragsNr` as `NULL` or an empty string.
```sql
-- Using NULL
CALL `your_project_id.your_dataset_id.k_ausd_v_ta_cntrct_templ_sp`('TEST_JOB_003', NULL);

-- Or using empty string
-- CALL `your_project_id.your_dataset_id.k_ausd_v_ta_cntrct_templ_sp`('TEST_JOB_003', '');
```

**Pass/Fail Criterion:**
1.  The `CALL` statement fails with an error message containing "FEHLER: EintragsNr (f) ist ein notwendiges Argument."
2.  The `job_audit` table contains one entry for `job_id = 'TEST_JOB_003'` with:
    *   `status = 'FAILED'`
    *   `error_message` containing "FEHLER: EintragsNr (f) ist ein notwendiges Argument."
    *   `records_processed IS NULL` or `0`.
    *   `start_time` and `end_time` are populated.

---

### Test Case 4: Transformation Correctness - Date Filtering Edge Cases

**Purpose:** Validate the complex date filtering logic (`insert_at`, `modified_at`, `valid_from`, `valid_to`) including `NULL` handling, which is critical for data selection.

**Setup:**
1.  Clear target and audit tables, and source tables.
2.  Populate `dwtk_meldungen` to set `v_current_date` to `2023-01-15`.
    ```sql
    TRUNCATE TABLE `your_project_id.your_dataset_id.dwtk_meldungen`;
    INSERT INTO `your_project_id.your_dataset_id.dwtk_meldungen` (job_kennung, timecreated) VALUES
    ('BERT_DROP_TEMP_TABLE', '2023-01-15 12:00:00 UTC');
    ```
3.  Populate `cds_ta_cntrct_template` and `cds_ta_care_description` with specific date scenarios:
    ```sql
    TRUNCATE TABLE `your_project_id.your_dataset_id.cds_ta_cntrct_template`;
    TRUNCATE TABLE `your_project_id.your_dataset_id.cds_ta_care_description`;

    INSERT INTO `your_project_id.your_dataset_id.cds_ta_cntrct_template` (cntrct_template_id, cds_description_id, insert_at, modified_at, valid_from, valid_to, is_production) VALUES
    (201, 10, '2023-01-15', NULL, '2023-01-15', NULL, 1), -- Included: all conditions met, NULLs handled
    (202, 11, '2023-01-14', '2023-01-16', '2023-01-10', '2023-01-20', 1), -- Included: modified_at > v_current_date, valid_to > v_current_date
    (203, 12, '2023-01-16', NULL, '2023-01-10', NULL, 1), -- Excluded: insert_at > v_current_date
    (204, 13, '2023-01-10', '2023-01-15', '2023-01-05', NULL, 1), -- Excluded: modified_at <= v_current_date
    (205, 14, '2023-01-10', NULL, '2023-01-16', NULL, 1), -- Excluded: valid_from > v_current_date
    (206, 15, '2023-01-10', NULL, '2023-01-05', '2023-01-15', 1), -- Excluded: valid_to <= v_current_date
    (207, 16, '2023-01-10', NULL, '2023-01-05', NULL, 0); -- Excluded: is_production = 0

    INSERT INTO `your_project_id.your_dataset_id.cds_ta_care_description` (cds_description_id, cds_description, language) VALUES
    (10, 'Desc 201', 1),
    (11, 'Desc 202', 1),
    (12, 'Desc 203', 1),
    (13, 'Desc 204', 1),
    (14, 'Desc 205', 1),
    (15, 'Desc 206', 1),
    (16, 'Desc 207', 1);
    ```

**Action:**
Execute the BigQuery Stored Procedure:
```sql
CALL `your_project_id.your_dataset_id.k_ausd_v_ta_cntrct_templ_sp`('TEST_JOB_004', 'ENTRY_004');
```

**Pass/Fail Criterion:**
1.  The `ta_cntrct_templ` table contains exactly 2 rows:
    ```
    cntrct_template_id | cds_description_id | cds_description
    -------------------|--------------------|----------------
    201                | 10                 | Desc 201
    202                | 11                 | Desc 202
    ```
2.  The `job_audit` table contains one entry for `job_id = 'TEST_JOB_004'` and `entry_number = 'ENTRY_004'` with:
    *   `status = 'SUCCESS'`
    *   `records_processed = 2`

---

### Test Case 5: Transformation Correctness - Join and Filter Mismatches

**Purpose:** Verify that rows are correctly excluded if they don't meet join conditions or specific filter criteria (`is_production`, `language`).

**Setup:**
1.  Clear target and audit tables, and source tables.
2.  Populate `dwtk_meldungen` to set `v_current_date` to `2023-01-10`.
    ```sql
    TRUNCATE TABLE `your_project_id.your_dataset_id.dwtk_meldungen`;
    INSERT INTO `your_project_id.your_dataset_id.dwtk_meldungen` (job_kennung, timecreated) VALUES
    ('BERT_DROP_TEMP_TABLE', '2023-01-10 08:00:00 UTC');
    ```
3.  Populate `cds_ta_cntrct_template` and `cds_ta_care_description` with join/filter mismatch data:
    ```sql
    TRUNCATE TABLE `your_project_id.your_dataset_id.cds_ta_cntrct_template`;
    TRUNCATE TABLE `your_project_id.your_dataset_id.cds_ta_care_description`;

    INSERT INTO `your_project_id.your_dataset_id.cds_ta_cntrct_template` (cntrct_template_id, cds_description_id, insert_at, modified_at, valid_from, valid_to, is_production) VALUES
    (301, 100, '2023-01-05', NULL, '2023-01-01', NULL, 1), -- Included (matches desc 100, prod=1, lang=1)
    (302, 101, '2023-01-05', NULL, '2023-01-01', NULL, 1), -- Excluded (no matching cds_description_id in cds_ta_care_description)
    (303, 102, '2023-01-05', NULL, '2023-01-01', NULL, 0), -- Excluded (is_production = 0)
    (304, 103, '2023-01-05', NULL, '2023-01-01', NULL, 1); -- Included (matches desc 103, prod=1, lang=1)

    INSERT INTO `your_project_id.your_dataset_id.cds_ta_care_description` (cds_description_id, cds_description, language) VALUES
    (100, 'Valid Desc 1', 1),
    (102, 'Invalid Lang Desc', 2), -- Language mismatch
    (103, 'Valid Desc 2', 1);
    ```

**Action:**
Execute the BigQuery Stored Procedure:
```sql
CALL `your_project_id.your_dataset_id.k_ausd_v_ta_cntrct_templ_sp`('TEST_JOB_005', 'ENTRY_005');
```

**Pass/Fail Criterion:**
1.  The `ta_cntrct_templ` table contains exactly 2 rows:
    ```
    cntrct_template_id | cds_description_id | cds_description
    -------------------|--------------------|----------------
    301                | 100                | Valid Desc 1
    304                | 103                | Valid Desc 2
    ```
2.  The `job_audit` table contains one entry for `job_id = 'TEST_JOB_005'` and `entry_number = 'ENTRY_005'` with:
    *   `status = 'SUCCESS'`
    *   `records_processed = 2`

---

### Test Case 6: Empty Source Tables - Data Quality / Row Count

**Purpose:** Verify that the job handles scenarios where source tables are empty, resulting in an empty target table and a `records_processed` count of 0.

**Setup:**
1.  Clear all tables:
    ```sql
    TRUNCATE TABLE `your_project_id.your_dataset_id.ta_cntrct_templ`;
    TRUNCATE TABLE `your_project_id.your_dataset_id.job_audit`;
    TRUNCATE TABLE `your_project_id.your_dataset_id.dwtk_meldungen`;
    TRUNCATE TABLE `your_project_id.your_dataset_id.cds_ta_cntrct_template`;
    TRUNCATE TABLE `your_project_id.your_dataset_id.cds_ta_care_description`;
    ```
2.  (Optional) Insert a `dwtk_meldungen` row to ensure `v_current_date` is set, even if other sources are empty.
    ```sql
    INSERT INTO `your_project_id.your_dataset_id.dwtk_meldungen` (job_kennung, timecreated) VALUES
    ('BERT_DROP_TEMP_TABLE', '2023-01-01 00:00:00 UTC');
    ```

**Action:**
Execute the BigQuery Stored Procedure:
```sql
CALL `your_project_id.your_dataset_id.k_ausd_v_ta_cntrct_templ_sp`('TEST_JOB_006', 'ENTRY_006');
```

**Pass/Fail Criterion:**
1.  The `ta_cntrct_templ` table is empty (contains 0 rows).
2.  The `job_audit` table contains one entry for `job_id = 'TEST_JOB_006'` and `entry_number = 'ENTRY_006'` with:
    *   `status = 'SUCCESS'`
    *   `records_processed = 0`
    *   `error_message IS NULL`

---

### Test Case 7: Idempotency - Repeated Execution

**Purpose:** Verify that running the job multiple times with the same parameters results in the same final state of the `ta_cntrct_templ` table and distinct audit entries. This confirms the `TRUNCATE` behavior.

**Setup:**
1.  Clear all tables.
2.  Populate source tables with the same data as in Test Case 1.
    ```sql
    TRUNCATE TABLE `your_project_id.your_dataset_id.ta_cntrct_templ`;
    TRUNCATE TABLE `your_project_id.your_dataset_id.job_audit`;
    TRUNCATE TABLE `your_project_id.your_dataset_id.dwtk_meldungen`;
    TRUNCATE TABLE `your_project_id.your_dataset_id.cds_ta_cntrct_template`;
    TRUNCATE TABLE `your_project_id.your_dataset_id.cds_ta_care_description`;

    INSERT INTO `your_project_id.your_dataset_id.dwtk_meldungen` (job_kennung, timecreated) VALUES
    ('BERT_DROP_TEMP_TABLE', '2023-01-15 12:30:00 UTC');

    INSERT INTO `your_project_id.your_dataset_id.cds_ta_cntrct_template` (cntrct_template_id, cds_description_id, insert_at, modified_at, valid_from, valid_to, is_production) VALUES
    (101, 1, '2023-01-01', NULL, '2023-01-01', NULL, 1),
    (102, 2, '2023-01-10', '2023-01-20', '2023-01-05', NULL, 1),
    (103, 3, '2023-01-15', NULL, '2023-01-15', '2023-01-25', 1),
    (108, 8, '2023-01-01', NULL, '2023-01-01', NULL, 1);

    INSERT INTO `your_project_id.your_dataset_id.cds_ta_care_description` (cds_description_id, cds_description, language) VALUES
    (1, 'Description A', 1),
    (2, 'Description B', 1),
    (3, 'Description C', 1),
    (8, 'Description H', 1);
    ```

**Action:**
Execute the BigQuery Stored Procedure twice with the same parameters:
```sql
CALL `your_project_id.your_dataset_id.k_ausd_v_ta_cntrct_templ_sp`('TEST_JOB_007', 'ENTRY_007');
CALL `your_project_id.your_dataset_id.k_ausd_v_ta_cntrct_templ_sp`('TEST_JOB_007', 'ENTRY_007');
```

**Pass/Fail Criterion:**
1.  The `ta_cntrct_templ` table contains the exact same 4 rows as in Test Case 1 after both executions. The content should not be duplicated or altered.
2.  The `job_audit` table contains two distinct entries for `job_id = 'TEST_JOB_007'` and `entry_number = 'ENTRY_007'`, both with:
    *   `status = 'SUCCESS'`
    *   `records_processed = 4`
    *   Distinct `start_time` and `end_time` values.

---

### Test Case 8: External System Replacement - `dwtk_meldungen` Date Derivation

**Purpose:** Verify that the `v_current_date` is correctly derived from `dwtk_meldungen` as specified in the design, replacing the implicit date handling or external file reads of the legacy system.

**Setup:**
1.  Clear all tables.
2.  Populate `cds_ta_cntrct_template` and `cds_ta_care_description` with data that would result in 1 row if `v_current_date` is `2023-01-05`, and 0 rows if `v_current_date` is `2023-01-04`.
    ```sql
    TRUNCATE TABLE `your_project_id.your_dataset_id.ta_cntrct_templ`;
    TRUNCATE TABLE `your_project_id.your_dataset_id.job_audit`;
    TRUNCATE TABLE `your_project_id.your_dataset_id.dwtk_meldungen`;
    TRUNCATE TABLE `your_project_id.your_dataset_id.cds_ta_cntrct_template`;
    TRUNCATE TABLE `your_project_id.your_dataset_id.cds_ta_care_description`;

    INSERT INTO `your_project_id.your_dataset_id.cds_ta_cntrct_template` (cntrct_template_id, cds_description_id, insert_at, modified_at, valid_from, valid_to, is_production) VALUES
    (401, 200, '2023-01-05', NULL, '2023-01-05', NULL, 1);

    INSERT INTO `your_project_id.your_dataset_id.cds_ta_care_description` (cds_description_id, cds_description, language) VALUES
    (200, 'Date Test Desc', 1);
    ```
3.  Populate `dwtk_meldungen` to ensure `MAX(timecreated)` for `BERT_DROP_TEMP_TABLE` results in `2023-01-04`.
    ```sql
    INSERT INTO `your_project_id.your_dataset_id.dwtk_meldungen` (job_kennung, timecreated) VALUES
    ('OTHER_JOB', '2023-01-10 00:00:00 UTC'),
    ('BERT_DROP_TEMP_TABLE', '2023-01-04 15:00:00 UTC'),
    ('BERT_DROP_TEMP_TABLE', '2023-01-03 10:00:00 UTC');
    ```

**Action:**
Execute the BigQuery Stored Procedure:
```sql
CALL `your_project_id.your_dataset_id.k_ausd_v_ta_cntrct_templ_sp`('TEST_JOB_008', 'ENTRY_008');
```

**Pass/Fail Criterion:**
1.  The `ta_cntrct_templ` table is empty (0 rows), because `v_current_date` (2023-01-04) is less than `insert_at` and `valid_from` (2023-01-05) for the only potential row.
2.  The `job_audit` table contains one entry for `job_id = 'TEST_JOB_008'` and `entry_number = 'ENTRY_008'` with:
    *   `status = 'SUCCESS'`
    *   `records_processed = 0`

---

### Test Case 9: Error Handling - Simulated SQL Script Failure

**Purpose:** Verify that if the core SQL transformation (`d_ausd_v_ta_cntrct_templ_bq.sql` equivalent) fails, the SP catches the error, logs it in `job_audit`, and reports a failure status. This replaces the `if [ ! $ErrNr -eq 0 ]` and `DWMSG_MeldeFehler` logic.

**Setup:**
1.  Clear all tables.
2.  Populate `dwtk_meldungen` and `cds_ta_care_description` with minimal data.
    ```sql
    TRUNCATE TABLE `your_project_id.your_dataset_id.ta_cntrct_templ`;
    TRUNCATE TABLE `your_project_id.your_dataset_id.job_audit`;
    TRUNCATE TABLE `your_project_id.your_dataset_id.dwtk_meldungen`;
    TRUNCATE TABLE `your_project_id.your_dataset_id.cds_ta_cntrct_template`;
    TRUNCATE TABLE `your_project_id.your_dataset_id.cds_ta_care_description`;

    INSERT INTO `your_project_id.your_dataset_id.dwtk_meldungen` (job_kennung, timecreated) VALUES
    ('BERT_DROP_TEMP_TABLE', '2023-01-01 00:00:00 UTC');
    INSERT INTO `your_project_id.your_dataset_id.cds_ta_care_description` (cds_description_id, cds_description, language) VALUES
    (1, 'Desc', 1);
    ```
3.  **Simulate failure:** Temporarily rename or drop `cds_ta_cntrct_template` table, or modify the SP's inlined SQL to reference a non-existent column/table. For this example, we'll assume a temporary modification to the SP's inlined SQL (or a separate test SP) to cause an error, e.g., by referencing `non_existent_table`.
    *   *Self-correction:* Since the SP is provided, I cannot directly modify it for a test. A more realistic test would involve creating a *separate* SP that calls the main SP, but before doing so, it drops one of the source tables. Or, for a unit test, mock the `EXECUTE IMMEDIATE` call to raise an error. For a functional test, dropping a table is the most direct way.

**Action:**
1.  Drop a critical source table:
    ```sql
    DROP TABLE `your_project_id.your_dataset_id.cds_ta_cntrct_template`;
    ```
2.  Execute the BigQuery Stored Procedure:
    ```sql
    CALL `your_project_id.your_dataset_id.k_ausd_v_ta_cntrct_templ_sp`('TEST_JOB_009', 'ENTRY_009');
    ```
3.  (Optional) Recreate the dropped table for subsequent tests.
    ```sql
    CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.cds_ta_cntrct_template` (
        cntrct_template_id INT64,
        cds_description_id INT64,
        insert_at DATE,
        modified_at DATE,
        valid_from DATE,
        valid_to DATE,
        is_production INT64
    );
    ```

**Pass/Fail Criterion:**
1.  The `CALL` statement fails with an error message indicating a table not found (e.g., "Not found: Table your_project_id.your_dataset_id.cds_ta_cntrct_template").
2.  The `job_audit` table contains one entry for `job_id = 'TEST_JOB_009'` and `entry_number = 'ENTRY_009'` with:
    *   `status = 'FAILED'`
    *   `error_message` containing the BigQuery error message (e.g., "Not found: Table...").
    *   `records_processed IS NULL` or `0`.
    *   `start_time` and `end_time` are populated.
3.  The `ta_cntrct_templ` table remains empty (or in its state before the failed execution, as `TRUNCATE` would have happened but `INSERT` failed).

---