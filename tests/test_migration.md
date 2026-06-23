As a senior data-migration QA engineer, I've analyzed the migration design and the provided source code for `r_ausd_v_ta_inv_assign.ksh`. The migration involves replatforming a KornShell/Oracle SQL workflow to a BigQuery Stored Procedure.

The following test cases are designed to ensure behavioral equivalence, transformation correctness, proper handling of external system replacements, and data quality/schema integrity in the migrated BigQuery solution.

---

## Migration Validation Tests for `project.dataset.r_ausd_v_ta_inv_assign`

**Assumptions for Test Execution:**
*   A BigQuery project (`project`) and dataset (`dataset`) are configured.
*   All DDLs for `sof_ta_inv_assign`, `cds_ta_inv_assignment`, `dwtk_meldungen`, and `job_log` have been applied.
*   The BigQuery Stored Procedure `project.dataset.r_ausd_v_ta_inv_assign` has been deployed.
*   The test environment has programmatic access to BigQuery (e.g., via Python `google-cloud-bigquery` client library).
*   Helper functions (`truncate_table`, `insert_data`, `get_table_row_count`, `get_table_data`, `call_bq_stored_procedure`, `execute_bq_query`) are available to interact with BigQuery.

---

### Test Case 1: Happy Path - Full Data Load and Output Parity

*   **Purpose:** Verify that the BigQuery Stored Procedure successfully processes a typical set of input data, applies all filters correctly, and produces the expected output in `sof_ta_inv_assign`, matching the legacy system's behavior. Also, verify successful logging.
*   **Setup:**
    1.  Truncate `project.dataset.sof_ta_inv_assign`, `project.dataset.cds_ta_inv_assignment`, `project.dataset.dwtk_meldungen`, and `project.dataset.job_log`.
    2.  Insert a `v_datum` record into `project.dataset.dwtk_meldungen`:
        ```sql
        INSERT INTO project.dataset.dwtk_meldungen (message_id, job_kennung, timecreated, message_text)
        VALUES ('msg1', 'BERT_DROP_TEMP_TABLE', '2023-01-15 10:00:00 UTC', 'Cutoff date for processing');
        ```
    3.  Insert a diverse set of records into `project.dataset.cds_ta_inv_assignment` that cover all filtering conditions (some should pass, some should fail):
        *   Record 1 (Pass): `insert_at` <= `v_datum`, `modified_at` > `v_datum`, `valid_from` <= `v_datum`, `valid_to` > `v_datum`, `is_production` = 1
        *   Record 2 (Pass): `insert_at` <= `v_datum`, `modified_at` IS NULL, `valid_from` <= `v_datum`, `valid_to` IS NULL, `is_production` = 1
        *   Record 3 (Fail - `insert_at` too late): `insert_at` > `v_datum`, `modified_at` IS NULL, `valid_from` <= `v_datum`, `valid_to` IS NULL, `is_production` = 1
        *   Record 4 (Fail - `modified_at` too early): `insert_at` <= `v_datum`, `modified_at` <= `v_datum`, `valid_from` <= `v_datum`, `valid_to` IS NULL, `is_production` = 1
        *   Record 5 (Fail - `valid_from` too late): `insert_at` <= `v_datum`, `modified_at` IS NULL, `valid_from` > `v_datum`, `valid_to` IS NULL, `is_production` = 1
        *   Record 6 (Fail - `valid_to` too early): `insert_at` <= `v_datum`, `modified_at` IS NULL, `valid_from` <= `v_datum`, `valid_to` <= `v_datum`, `is_production` = 1
        *   Record 7 (Fail - `is_production` = 0): `insert_at` <= `v_datum`, `modified_at` IS NULL, `valid_from` <= `v_datum`, `valid_to` IS NULL, `is_production` = 0
        *   Record 8 (Pass): `insert_at` = `v_datum`, `modified_at` = '2023-01-16 00:00:00 UTC', `valid_from` = `v_datum`, `valid_to` = '2023-01-16 00:00:00 UTC', `is_production` = 1
*   **Action:** Execute the BigQuery Stored Procedure:
    ```python
    call_bq_stored_procedure('project.dataset.r_ausd_v_ta_inv_assign', 'BERT_V_TA_INV_ASSIGN', 12345)
    ```
*   **Pass/Fail Criterion:**
    1.  **Output Parity:** The `project.dataset.sof_ta_inv_assign` table contains exactly 3 records (Record 1, Record 2, Record 8) with data identical to the source `cds_ta_inv_assignment` for those records.
    2.  **Row Count:** `get_table_row_count('project.dataset.sof_ta_inv_assign')` returns 3.
    3.  **Logging:** `project.dataset.job_log` contains one entry for `job_name = 'r_ausd_v_ta_inv_assign'` with `status = 'SUCCESS'` and `record_count = 3`.
    ```python
    def test_happy_path_full_load(bq_client):
        truncate_table(bq_client, 'project.dataset.sof_ta_inv_assign')
        truncate_table(bq_client, 'project.dataset.cds_ta_inv_assignment')
        truncate_table(bq_client, 'project.dataset.dwtk_meldungen')
        truncate_table(bq_client, 'project.dataset.job_log')

        v_datum_str = '2023-01-15 10:00:00 UTC'
        insert_data(bq_client, 'project.dataset.dwtk_meldungen', [
            {'message_id': 'msg1', 'job_kennung': 'BERT_DROP_TEMP_TABLE', 'timecreated': v_datum_str, 'message_text': 'Cutoff date'}
        ])

        source_data = [
            # Pass: Record 1
            {'assignment_id': 'A001', 'insert_at': '2023-01-10 00:00:00 UTC', 'modified_at': '2023-01-16 00:00:00 UTC', 'valid_from': '2023-01-01 00:00:00 UTC', 'valid_to': '2023-01-31 00:00:00 UTC', 'is_production': 1, 'some_value': 'Val1'},
            # Pass: Record 2 (NULL handling)
            {'assignment_id': 'A002', 'insert_at': '2023-01-10 00:00:00 UTC', 'modified_at': None, 'valid_from': '2023-01-01 00:00:00 UTC', 'valid_to': None, 'is_production': 1, 'some_value': 'Val2'},
            # Fail: Record 3 (insert_at too late)
            {'assignment_id': 'A003', 'insert_at': '2023-01-16 00:00:00 UTC', 'modified_at': None, 'valid_from': '2023-01-01 00:00:00 UTC', 'valid_to': None, 'is_production': 1, 'some_value': 'Val3'},
            # Fail: Record 4 (modified_at too early)
            {'assignment_id': 'A004', 'insert_at': '2023-01-10 00:00:00 UTC', 'modified_at': '2023-01-14 00:00:00 UTC', 'valid_from': '2023-01-01 00:00:00 UTC', 'valid_to': None, 'is_production': 1, 'some_value': 'Val4'},
            # Fail: Record 5 (valid_from too late)
            {'assignment_id': 'A005', 'insert_at': '2023-01-10 00:00:00 UTC', 'modified_at': None, 'valid_from': '2023-01-16 00:00:00 UTC', 'valid_to': None, 'is_production': 1, 'some_value': 'Val5'},
            # Fail: Record 6 (valid_to too early)
            {'assignment_id': 'A006', 'insert_at': '2023-01-10 00:00:00 UTC', 'modified_at': None, 'valid_from': '2023-01-01 00:00:00 UTC', 'valid_to': '2023-01-14 00:00:00 UTC', 'is_production': 1, 'some_value': 'Val6'},
            # Fail: Record 7 (is_production = 0)
            {'assignment_id': 'A007', 'insert_at': '2023-01-10 00:00:00 UTC', 'modified_at': None, 'valid_from': '2023-01-01 00:00:00 UTC', 'valid_to': None, 'is_production': 0, 'some_value': 'Val7'},
            # Pass: Record 8 (boundary conditions for v_datum)
            {'assignment_id': 'A008', 'insert_at': '2023-01-15 10:00:00 UTC', 'modified_at': '2023-01-15 10:00:01 UTC', 'valid_from': '2023-01-15 10:00:00 UTC', 'valid_to': '2023-01-15 10:00:01 UTC', 'is_production': 1, 'some_value': 'Val8'},
        ]
        insert_data(bq_client, 'project.dataset.cds_ta_inv_assignment', source_data)

        call_bq_stored_procedure(bq_client, 'project.dataset.r_ausd_v_ta_inv_assign', 'BERT_V_TA_INV_ASSIGN', 12345)

        # Assertions
        target_records = get_table_data(bq_client, 'project.dataset.sof_ta_inv_assign', order_by_col='assignment_id')
        assert len(target_records) == 3
        assert target_records[0]['assignment_id'] == 'A001'
        assert target_records[1]['assignment_id'] == 'A002'
        assert target_records[2]['assignment_id'] == 'A008'

        log_entries = get_table_data(bq_client, 'project.dataset.job_log')
        assert len(log_entries) == 2 # Initial RUNNING + final SUCCESS
        success_log = next((log for log in log_entries if log['status'] == 'SUCCESS'), None)
        assert success_log is not None
        assert success_log['job_name'] == 'r_ausd_v_ta_inv_assign'
        assert success_log['job_kennung'] == 'BERT_V_TA_INV_ASSIGN'
        assert success_log['eintrags_nr'] == 12345
        assert success_log['record_count'] == 3
        assert success_log['error_message'] is None
    ```

### Test Case 2: Transformation Correctness - `v_datum` Derivation Edge Cases

*   **Purpose:** Verify the correct derivation of `v_datum` from `dwtk_meldungen`, especially when multiple relevant entries exist.
*   **Setup:**
    1.  Truncate `project.dataset.sof_ta_inv_assign`, `project.dataset.cds_ta_inv_assignment`, `project.dataset.dwtk_meldungen`, and `project.dataset.job_log`.
    2.  Insert multiple `BERT_DROP_TEMP_TABLE` records into `project.dataset.dwtk_meldungen` with varying `timecreated` values:
        ```sql
        INSERT INTO project.dataset.dwtk_meldungen (message_id, job_kennung, timecreated, message_text) VALUES
        ('msg_old', 'BERT_DROP_TEMP_TABLE', '2023-01-01 00:00:00 UTC', 'Old cutoff'),
        ('msg_new', 'BERT_DROP_TEMP_TABLE', '2023-01-20 12:00:00 UTC', 'New cutoff'),
        ('msg_other', 'OTHER_JOB', '2023-01-25 00:00:00 UTC', 'Irrelevant cutoff');
        ```
    3.  Insert records into `project.dataset.cds_ta_inv_assignment` such that some would pass with the "new cutoff" date (`2023-01-20 12:00:00 UTC`) but not with the "old cutoff" date (`2023-01-01 00:00:00 UTC`).
        *   Record 1 (Pass with new, Fail with old): `insert_at` = '2023-01-10 00:00:00 UTC', `is_production` = 1
        *   Record 2 (Pass with new, Fail with old): `insert_at` = '2023-01-19 00:00:00 UTC', `is_production` = 1
        *   Record 3 (Pass with new and old): `insert_at` = '2022-12-01 00:00:00 UTC', `is_production` = 1
*   **Action:** Execute the BigQuery Stored Procedure.
*   **Pass/Fail Criterion:**
    1.  The `project.dataset.sof_ta_inv_assign` table contains records that are filtered based on `v_datum = '2023-01-20 12:00:00 UTC'` (i.e., 3 records should be inserted).
    2.  The `job_log` entry shows `status = 'SUCCESS'` and `record_count = 3`.
    ```python
    def test_v_datum_derivation_correctness(bq_client):
        truncate_table(bq_client, 'project.dataset.sof_ta_inv_assign')
        truncate_table(bq_client, 'project.dataset.cds_ta_inv_assignment')
        truncate_table(bq_client, 'project.dataset.dwtk_meldungen')
        truncate_table(bq_client, 'project.dataset.job_log')

        insert_data(bq_client, 'project.dataset.dwtk_meldungen', [
            {'message_id': 'msg_old', 'job_kennung': 'BERT_DROP_TEMP_TABLE', 'timecreated': '2023-01-01 00:00:00 UTC', 'message_text': 'Old cutoff'},
            {'message_id': 'msg_new', 'job_kennung': 'BERT_DROP_TEMP_TABLE', 'timecreated': '2023-01-20 12:00:00 UTC', 'message_text': 'New cutoff'},
            {'message_id': 'msg_other', 'job_kennung': 'OTHER_JOB', 'timecreated': '2023-01-25 00:00:00 UTC', 'message_text': 'Irrelevant cutoff'}
        ])

        source_data = [
            {'assignment_id': 'B001', 'insert_at': '2023-01-10 00:00:00 UTC', 'modified_at': None, 'valid_from': '2023-01-01 00:00:00 UTC', 'valid_to': None, 'is_production': 1, 'some_value': 'ValB1'},
            {'assignment_id': 'B002', 'insert_at': '2023-01-19 00:00:00 UTC', 'modified_at': None, 'valid_from': '2023-01-01 00:00:00 UTC', 'valid_to': None, 'is_production': 1, 'some_value': 'ValB2'},
            {'assignment_id': 'B003', 'insert_at': '2022-12-01 00:00:00 UTC', 'modified_at': None, 'valid_from': '2022-11-01 00:00:00 UTC', 'valid_to': None, 'is_production': 1, 'some_value': 'ValB3'},
            {'assignment_id': 'B004', 'insert_at': '2023-01-21 00:00:00 UTC', 'modified_at': None, 'valid_from': '2023-01-01 00:00:00 UTC', 'valid_to': None, 'is_production': 1, 'some_value': 'ValB4'} # Should fail
        ]
        insert_data(bq_client, 'project.dataset.cds_ta_inv_assignment', source_data)

        call_bq_stored_procedure(bq_client, 'project.dataset.r_ausd_v_ta_inv_assign', 'BERT_V_TA_INV_ASSIGN', 54321)

        target_records = get_table_data(bq_client, 'project.dataset.sof_ta_inv_assign')
        assert len(target_records) == 3
        assert {r['assignment_id'] for r in target_records} == {'B001', 'B002', 'B003'}

        log_entries = get_table_data(bq_client, 'project.dataset.job_log')
        success_log = next((log for log in log_entries if log['status'] == 'SUCCESS'), None)
        assert success_log['record_count'] == 3
    ```

### Test Case 3: Error Handling - Missing `v_datum`

*   **Purpose:** Verify that the job fails gracefully and logs an error if `v_datum` cannot be determined (e.g., `dwtk_meldungen` is empty or has no matching `job_kennung`).
*   **Setup:**
    1.  Truncate `project.dataset.sof_ta_inv_assign`, `project.dataset.cds_ta_inv_assignment`, `project.dataset.dwtk_meldungen`, and `project.dataset.job_log`.
    2.  Insert an irrelevant record into `project.dataset.dwtk_meldungen` (or leave it empty):
        ```sql
        INSERT INTO project.dataset.dwtk_meldungen (message_id, job_kennung, timecreated, message_text)
        VALUES ('msg_irrelevant', 'ANOTHER_JOB', '2023-01-01 00:00:00 UTC', 'No BERT_DROP_TEMP_TABLE entry');
        ```
    3.  Insert some records into `project.dataset.cds_ta_inv_assignment` (these should not be processed).
*   **Action:** Execute the BigQuery Stored Procedure.
*   **Pass/Fail Criterion:**
    1.  The stored procedure execution raises an error (e.g., `google.api_core.exceptions.BadRequest` or similar, depending on client library).
    2.  `project.dataset.sof_ta_inv_assign` remains empty.
    3.  `project.dataset.job_log` contains an entry with `status = 'FAILED'` and an `error_message` indicating that `v_datum` could not be determined.
    ```python
    import pytest
    from google.cloud import bigquery

    def test_error_missing_v_datum(bq_client):
        truncate_table(bq_client, 'project.dataset.sof_ta_inv_assign')
        truncate_table(bq_client, 'project.dataset.cds_ta_inv_assignment')
        truncate_table(bq_client, 'project.dataset.dwtk_meldungen')
        truncate_table(bq_client, 'project.dataset.job_log')

        # Only insert an irrelevant entry, so BERT_DROP_TEMP_TABLE is missing
        insert_data(bq_client, 'project.dataset.dwtk_meldungen', [
            {'message_id': 'msg_irrelevant', 'job_kennung': 'ANOTHER_JOB', 'timecreated': '2023-01-01 00:00:00 UTC', 'message_text': 'No BERT_DROP_TEMP_TABLE entry'}
        ])
        insert_data(bq_client, 'project.dataset.cds_ta_inv_assignment', [
            {'assignment_id': 'C001', 'insert_at': '2023-01-01 00:00:00 UTC', 'modified_at': None, 'valid_from': '2023-01-01 00:00:00 UTC', 'valid_to': None, 'is_production': 1, 'some_value': 'ValC1'}
        ])

        with pytest.raises(bigquery.exceptions.BadRequest) as excinfo:
            call_bq_stored_procedure(bq_client, 'project.dataset.r_ausd_v_ta_inv_assign', 'BERT_V_TA_INV_ASSIGN', 67890)

        assert "Cutoff date (v_datum) could not be determined" in str(excinfo.value)

        assert get_table_row_count(bq_client, 'project.dataset.sof_ta_inv_assign') == 0

        log_entries = get_table_data(bq_client, 'project.dataset.job_log')
        failed_log = next((log for log in log_entries if log['status'] == 'FAILED'), None)
        assert failed_log is not None
        assert failed_log['job_name'] == 'r_ausd_v_ta_inv_assign'
        assert failed_log['status'] == 'FAILED'
        assert "Cutoff date (v_datum) could not be determined" in failed_log['error_message']
        assert failed_log['record_count'] == 0 # No records processed
    ```

### Test Case 4: External System Replacements - Empty Source Table

*   **Purpose:** Verify the job handles an empty `cds_ta_inv_assignment` table gracefully, resulting in an empty target table and correct logging. This simulates a scenario where the upstream Oracle system (Carmen DB) provides no data.
*   **Setup:**
    1.  Truncate `project.dataset.sof_ta_inv_assign`, `project.dataset.cds_ta_inv_assignment`, `project.dataset.dwtk_meldungen`, and `project.dataset.job_log`.
    2.  Insert a valid `v_datum` record into `project.dataset.dwtk_meldungen`:
        ```sql
        INSERT INTO project.dataset.dwtk_meldungen (message_id, job_kennung, timecreated, message_text)
        VALUES ('msg1', 'BERT_DROP_TEMP_TABLE', '2023-01-15 10:00:00 UTC', 'Cutoff date for processing');
        ```
    3.  Ensure `project.dataset.cds_ta_inv_assignment` is empty.
*   **Action:** Execute the BigQuery Stored Procedure.
*   **Pass/Fail Criterion:**
    1.  `project.dataset.sof_ta_inv_assign` remains empty.
    2.  `project.dataset.job_log` contains an entry with `status = 'SUCCESS'` and `record_count = 0`.
    ```python
    def test_empty_source_table(bq_client):
        truncate_table(bq_client, 'project.dataset.sof_ta_inv_assign')
        truncate_table(bq_client, 'project.dataset.cds_ta_inv_assignment')
        truncate_table(bq_client, 'project.dataset.dwtk_meldungen')
        truncate_table(bq_client, 'project.dataset.job_log')

        insert_data(bq_client, 'project.dataset.dwtk_meldungen', [
            {'message_id': 'msg1', 'job_kennung': 'BERT_DROP_TEMP_TABLE', 'timecreated': '2023-01-15 10:00:00 UTC', 'message_text': 'Cutoff date'}
        ])
        # cds_ta_inv_assignment is intentionally left empty

        call_bq_stored_procedure(bq_client, 'project.dataset.r_ausd_v_ta_inv_assign', 'BERT_V_TA_INV_ASSIGN', 98765)

        assert get_table_row_count(bq_client, 'project.dataset.sof_ta_inv_assign') == 0

        log_entries = get_table_data(bq_client, 'project.dataset.job_log')
        success_log = next((log for log in log_entries if log['status'] == 'SUCCESS'), None)
        assert success_log is not None
        assert success_log['record_count'] == 0
        assert success_log['error_message'] is None
    ```

### Test Case 5: Data Quality - Schema and Type Assertions

*   **Purpose:** Verify that the schema of the target table `sof_ta_inv_assign` matches the expected structure and data types, and that data is inserted without type coercion issues.
*   **Setup:**
    1.  Truncate `project.dataset.sof_ta_inv_assign`, `project.dataset.cds_ta_inv_assignment`, `project.dataset.dwtk_meldungen`, and `project.dataset.job_log`.
    2.  Insert a valid `v_datum` record.
    3.  Insert a single, well-formed record into `project.dataset.cds_ta_inv_assignment`.
*   **Action:** Execute the BigQuery Stored Procedure.
*   **Pass/Fail Criterion:**
    1.  The schema of `project.dataset.sof_ta_inv_assign` matches the DDL provided in the migration document (e.g., `assignment_id` is STRING, `insert_at` is TIMESTAMP, `is_production` is INT64).
    2.  The inserted record in `sof_ta_inv_assign` has the correct data types for each column, and no data loss or unexpected conversions occurred.
    ```python
    def test_data_quality_schema_and_types(bq_client):
        truncate_table(bq_client, 'project.dataset.sof_ta_inv_assign')
        truncate_table(bq_client, 'project.dataset.cds_ta_inv_assignment')
        truncate_table(bq_client, 'project.dataset.dwtk_meldungen')
        truncate_table(bq_client, 'project.dataset.job_log')

        insert_data(bq_client, 'project.dataset.dwtk_meldungen', [
            {'message_id': 'msg1', 'job_kennung': 'BERT_DROP_TEMP_TABLE', 'timecreated': '2023-01-15 10:00:00 UTC', 'message_text': 'Cutoff date'}
        ])

        source_record = {
            'assignment_id': 'D001',
            'insert_at': '2023-01-10 00:00:00 UTC',
            'modified_at': '2023-01-16 00:00:00 UTC',
            'valid_from': '2023-01-01 00:00:00 UTC',
            'valid_to': '2023-01-31 00:00:00 UTC',
            'is_production': 1,
            'some_value': 'TestValue'
        }
        insert_data(bq_client, 'project.dataset.cds_ta_inv_assignment', [source_record])

        call_bq_stored_procedure(bq_client, 'project.dataset.r_ausd_v_ta_inv_assign', 'BERT_V_TA_INV_ASSIGN', 11223)

        target_records = get_table_data(bq_client, 'project.dataset.sof_ta_inv_assign')
        assert len(target_records) == 1
        inserted_record = target_records[0]

        # Verify data types (BigQuery client usually returns Python native types)
        assert isinstance(inserted_record['assignment_id'], str)
        assert isinstance(inserted_record['insert_at'], datetime.datetime) # Assuming datetime objects from BQ client
        assert isinstance(inserted_record['modified_at'], datetime.datetime)
        assert isinstance(inserted_record['valid_from'], datetime.datetime)
        assert isinstance(inserted_record['valid_to'], datetime.datetime)
        assert isinstance(inserted_record['is_production'], int)
        assert isinstance(inserted_record['some_value'], str)

        # Verify values
        assert inserted_record['assignment_id'] == source_record['assignment_id']
        assert inserted_record['is_production'] == source_record['is_production']
        assert inserted_record['some_value'] == source_record['some_value']
        # For timestamps, compare string representations or use timezone-aware comparison
        assert inserted_record['insert_at'].isoformat(timespec='seconds') + 'Z' == source_record['insert_at']
        # ... and so on for other timestamp fields
    ```

### Test Case 6: Parameter Validation - Missing `p_JobKennung`

*   **Purpose:** Verify that the stored procedure correctly handles and logs an error when the mandatory `p_JobKennung` parameter is missing or empty, as per the `IF p_JobKennung IS NULL OR p_JobKennung = ''` check.
*   **Setup:**
    1.  Truncate `project.dataset.job_log`.
    2.  No other tables need specific data for this test.
*   **Action:** Attempt to execute the BigQuery Stored Procedure with `NULL` or empty string for `p_JobKennung`.
    ```python
    # For NULL
    call_bq_stored_procedure('project.dataset.r_ausd_v_ta_inv_assign', None, 123)
    # For empty string
    call_bq_stored_procedure('project.dataset.r_ausd_v_ta_inv_assign', '', 123)
    ```
*   **Pass/Fail Criterion:**
    1.  The stored procedure execution raises an error (e.g., `google.api_core.exceptions.BadRequest`).
    2.  `project.dataset.job_log` contains an entry with `status = 'FAILED'` and an `error_message` indicating "Parameter p_JobKennung cannot be NULL or empty."
    ```python
    import pytest
    from google.cloud import bigquery

    def test_parameter_validation_missing_jobkennung(bq_client):
        truncate_table(bq_client, 'project.dataset.job_log')

        # Test with NULL p_JobKennung
        with pytest.raises(bigquery.exceptions.BadRequest) as excinfo:
            call_bq_stored_procedure(bq_client, 'project.dataset.r_ausd_v_ta_inv_assign', None, 123)
        assert "Parameter p_JobKennung cannot be NULL or empty." in str(excinfo.value)

        log_entries = get_table_data(bq_client, 'project.dataset.job_log')
        failed_log = next((log for log in log_entries if log['status'] == 'FAILED'), None)
        assert failed_log is not None
        assert failed_log['job_name'] == 'r_ausd_v_ta_inv_assign'
        assert failed_log['status'] == 'FAILED'
        assert "Parameter p_JobKennung cannot be NULL or empty." in failed_log['error_message']
        assert failed_log['job_kennung'] is None # Should be NULL if passed as NULL

        truncate_table(bq_client, 'project.dataset.job_log') # Clear for next test

        # Test with empty string p_JobKennung
        with pytest.raises(bigquery.exceptions.BadRequest) as excinfo:
            call_bq_stored_procedure(bq_client, 'project.dataset.r_ausd_v_ta_inv_assign', '', 456)
        assert "Parameter p_JobKennung cannot be NULL or empty." in str(excinfo.value)

        log_entries = get_table_data(bq_client, 'project.dataset.job_log')
        failed_log = next((log for log in log_entries if log['status'] == 'FAILED'), None)
        assert failed_log is not None
        assert failed_log['job_kennung'] == '' # Should be empty string if passed as empty string
    ```

### Test Case 7: Idempotency - Multiple Runs

*   **Purpose:** Verify that running the job multiple times with the same input data produces the same result in the target table, due to the `TRUNCATE` and `INSERT` pattern.
*   **Setup:**
    1.  Truncate `project.dataset.sof_ta_inv_assign`, `project.dataset.cds_ta_inv_assignment`, `project.dataset.dwtk_meldungen`, and `project.dataset.job_log`.
    2.  Insert a valid `v_datum` record.
    3.  Insert a set of records into `project.dataset.cds_ta_inv_assignment` that should result in 2-3 records being inserted into the target.
*   **Action:** Execute the BigQuery Stored Procedure twice consecutively.
*   **Pass/Fail Criterion:**
    1.  After the first run, `project.dataset.sof_ta_inv_assign` contains the expected number of records (e.g., 2).
    2.  After the second run, `project.dataset.sof_ta_inv_assign` still contains the exact same records and the same count (e.g., 2). No duplicates, no additional records.
    3.  `project.dataset.job_log` contains two `SUCCESS` entries, each with the correct `record_count`.
    ```python
    def test_idempotency_multiple_runs(bq_client):
        truncate_table(bq_client, 'project.dataset.sof_ta_inv_assign')
        truncate_table(bq_client, 'project.dataset.cds_ta_inv_assignment')
        truncate_table(bq_client, 'project.dataset.dwtk_meldungen')
        truncate_table(bq_client, 'project.dataset.job_log')

        insert_data(bq_client, 'project.dataset.dwtk_meldungen', [
            {'message_id': 'msg1', 'job_kennung': 'BERT_DROP_TEMP_TABLE', 'timecreated': '2023-01-15 10:00:00 UTC', 'message_text': 'Cutoff date'}
        ])

        source_data = [
            {'assignment_id': 'E001', 'insert_at': '2023-01-10 00:00:00 UTC', 'modified_at': None, 'valid_from': '2023-01-01 00:00:00 UTC', 'valid_to': None, 'is_production': 1, 'some_value': 'ValE1'},
            {'assignment_id': 'E002', 'insert_at': '2023-01-12 00:00:00 UTC', 'modified_at': '2023-01-16 00:00:00 UTC', 'valid_from': '2023-01-05 00:00:00 UTC', 'valid_to': '2023-01-20 00:00:00 UTC', 'is_production': 1, 'some_value': 'ValE2'},
            {'assignment_id': 'E003', 'insert_at': '2023-01-16 00:00:00 UTC', 'modified_at': None, 'valid_from': '2023-01-01 00:00:00 UTC', 'valid_to': None, 'is_production': 1, 'some_value': 'ValE3'} # Should fail
        ]
        insert_data(bq_client, 'project.dataset.cds_ta_inv_assignment', source_data)

        # First run
        call_bq_stored_procedure(bq_client, 'project.dataset.r_ausd_v_ta_inv_assign', 'BERT_V_TA_INV_ASSIGN', 1)
        first_run_target_records = get_table_data(bq_client, 'project.dataset.sof_ta_inv_assign', order_by_col='assignment_id')
        assert len(first_run_target_records) == 2
        assert {r['assignment_id'] for r in first_run_target_records} == {'E001', 'E002'}

        # Second run
        call_bq_stored_procedure(bq_client, 'project.dataset.r_ausd_v_ta_inv_assign', 'BERT_V_TA_INV_ASSIGN', 2)
        second_run_target_records = get_table_data(bq_client, 'project.dataset.sof_ta_inv_assign', order_by_col='assignment_id')
        assert len(second_run_target_records) == 2
        assert {r['assignment_id'] for r in second_run_target_records} == {'E001', 'E002'}
        assert first_run_target_records == second_run_target_records # Ensure exact same data

        log_entries = get_table_data(bq_client, 'project.dataset.job_log', order_by_col='eintrags_nr')
        assert len(log_entries) == 4 # 2 RUNNING + 2 SUCCESS
        success_logs = [log for log in log_entries if log['status'] == 'SUCCESS']
        assert len(success_logs) == 2
        assert success_logs[0]['record_count'] == 2
        assert success_logs[1]['record_count'] == 2
    ```

---

These test cases cover the critical aspects of the migration, from basic functionality and data transformation to error handling and data quality. The use of a testing framework like `pytest` with BigQuery client libraries allows for automated, repeatable validation of the migrated code.