As a senior data-migration QA engineer, I've analyzed the provided migration design and legacy code for `k_ausd_v_ta_cntrct_valid.ksh`. The migration involves re-implementing a KornShell-orchestrated Oracle SQL job as a BigQuery stored procedure, orchestrated by Cloud Composer.

The following test cases are designed to ensure behavioral equivalence, transformation correctness, proper handling of external dependencies, and data quality between the legacy Oracle/KornShell job and the new BigQuery/Cloud Composer implementation.

---

## Migration Validation Tests for `k_ausd_v_ta_cntrct_valid.ksh`

**General Setup for all tests:**
*   BigQuery project, dataset, `isbert_schema`, and `source_dataset` are configured.
*   The DDL for `project.dataset.sof_ta_cntrct_valid` and `project.dataset.job_audit_log` has been executed.
*   The BigQuery stored procedure `project.dataset.r_ausd_vertrag` has been deployed.
*   `bq_client` refers to an initialized BigQuery client in Python test code.
*   Helper functions like `_insert_bq_data(table_id, data)`, `_execute_bq_query(query)`, `_call_bq_stored_procedure(procedure_id, params)` are assumed for brevity in `pytest` examples.
*   `legacy_expected_data` refers to data derived from running the *exact same input data* through the legacy system's logic (or a precise simulation of it) to establish a "golden record" for comparison.

---

### Test Case 1: Basic Functional Equivalence (Output Parity)

*   **Purpose:** Verify that with typical input data, the migrated BigQuery stored procedure produces the exact same output in `project.dataset.sof_ta_cntrct_valid` as the legacy job would. This is the primary output parity check.
*   **Setup:**
    1.  **`project.isbert_schema.dwtk_meldungen`:**
        ```sql
        INSERT INTO `project.isbert_schema.dwtk_meldungen` (job_kennung, timecreated) VALUES
        ('OTHER_JOB', '2023-01-01 10:00:00 UTC'),
        ('BERT_DROP_TEMP_TABLE', '2023-01-10 12:00:00 UTC'), -- This should define v_datum = '2023-01-10'
        ('BERT_DROP_TEMP_TABLE', '2023-01-05 08:00:00 UTC');
        ```
    2.  **`project.source_dataset.cds_ta_cntrct_validity`:**
        ```sql
        INSERT INTO `project.source_dataset.cds_ta_cntrct_validity` (cntrct_validity_id, first_period_id, following_period_id, first_notice_period_id, follow_notice_period_id, insert_at, modified_at) VALUES
        (1, 101, 201, 301, 401, '2023-01-09 00:00:00 UTC', NULL), -- Included: insert_at <= v_datum, modified_at IS NULL
        (2, 102, 202, 302, 402, '2023-01-10 23:59:59 UTC', '2023-01-11 00:00:00 UTC'), -- Included: insert_at <= v_datum, modified_at > v_datum
        (3, 103, 203, 303, 403, '2023-01-08 00:00:00 UTC', '2023-01-09 00:00:00 UTC'), -- Excluded: insert_at <= v_datum, modified_at <= v_datum
        (4, 104, 204, 304, 404, '2023-01-11 00:00:00 UTC', NULL), -- Excluded: insert_at > v_datum
        (5, 105, 205, 305, 405, '2023-01-01 00:00:00 UTC', '2023-01-10 00:00:00 UTC'); -- Excluded: modified_at is not strictly > v_datum (it's equal to DATE(v_datum))
        ```
    3.  **Expected `v_datum`:** '2023-01-10' (from `MAX(DATE(timecreated))` for `BERT_DROP_TEMP_TABLE`).
    4.  **Expected `project.dataset.sof_ta_cntrct_valid` content:**
        ```
        cntrct_validity_id | first_period_id | following_period_id | first_notice_period_id | follow_notice_period_id | bfc_age
        -------------------|-----------------|---------------------|------------------------|-------------------------|------------------------
        1                  | 101             | 201                 | 301                    | 401                     | 2023-01-09 00:00:00 UTC
        2                  | 102             | 202                 | 302                    | 402                     | 2023-01-10 23:59:59 UTC
        ```
*   **Action:**
    1.  Execute the BigQuery stored procedure: `CALL project.dataset.r_ausd_vertrag('TEST_JOB_KENNUNG', 'TEST_ENTRY_NR');`
    2.  Query the contents of `project.dataset.sof_ta_cntrct_valid`.
    3.  Query the `project.dataset.job_audit_log` for the latest entry.
*   **Pass/Fail Criterion:**
    1.  The `project.dataset.sof_ta_cntrct_valid` table contains exactly the two expected rows, with all column values matching the expected content.
    2.  The `job_audit_log` shows `records_loaded = 2` and `status = 'SUCCESS'`.
*   **Runnable Test Code (pytest):**
    ```python
    import pytest
    from google.cloud import bigquery
    from datetime import datetime

    # Assume bq_client, project_id, dataset_id, isbert_schema_id, source_dataset_id are configured
    # and helper functions _insert_bq_data, _execute_bq_query, _call_bq_stored_procedure exist.

    def test_basic_functional_equivalence(bq_client, project_id, dataset_id, isbert_schema_id, source_dataset_id):
        # Clear target and source tables for a clean test run
        _execute_bq_query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.sof_ta_cntrct_valid`")
        _execute_bq_query(f"TRUNCATE TABLE `{project_id}.{isbert_schema_id}.dwtk_meldungen`")
        _execute_bq_query(f"TRUNCATE TABLE `{project_id}.{source_dataset_id}.cds_ta_cntrct_validity`")
        _execute_bq_query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_audit_log`")

        # Setup dwtk_meldungen
        dwtk_data = [
            {'job_kennung': 'OTHER_JOB', 'timecreated': datetime(2023, 1, 1, 10, 0, 0)},
            {'job_kennung': 'BERT_DROP_TEMP_TABLE', 'timecreated': datetime(2023, 1, 10, 12, 0, 0)},
            {'job_kennung': 'BERT_DROP_TEMP_TABLE', 'timecreated': datetime(2023, 1, 5, 8, 0, 0)},
        ]
        _insert_bq_data(f"{project_id}.{isbert_schema_id}.dwtk_meldungen", dwtk_data)

        # Setup cds_ta_cntrct_validity
        cds_data = [
            {'cntrct_validity_id': 1, 'first_period_id': 101, 'following_period_id': 201, 'first_notice_period_id': 301, 'follow_notice_period_id': 401, 'insert_at': datetime(2023, 1, 9, 0, 0, 0), 'modified_at': None},
            {'cntrct_validity_id': 2, 'first_period_id': 102, 'following_period_id': 202, 'first_notice_period_id': 302, 'follow_notice_period_id': 402, 'insert_at': datetime(2023, 1, 10, 23, 59, 59), 'modified_at': datetime(2023, 1, 11, 0, 0, 0)},
            {'cntrct_validity_id': 3, 'first_period_id': 103, 'following_period_id': 203, 'first_notice_period_id': 303, 'follow_notice_period_id': 403, 'insert_at': datetime(2023, 1, 8, 0, 0, 0), 'modified_at': datetime(2023, 1, 9, 0, 0, 0)},
            {'cntrct_validity_id': 4, 'first_period_id': 104, 'following_period_id': 204, 'first_notice_period_id': 304, 'follow_notice_period_id': 404, 'insert_at': datetime(2023, 1, 11, 0, 0, 0), 'modified_at': None},
            {'cntrct_validity_id': 5, 'first_period_id': 105, 'following_period_id': 205, 'first_notice_period_id': 305, 'follow_notice_period_id': 405, 'insert_at': datetime(2023, 1, 1, 0, 0, 0), 'modified_at': datetime(2023, 1, 10, 0, 0, 0)},
        ]
        _insert_bq_data(f"{project_id}.{source_dataset_id}.cds_ta_cntrct_validity", cds_data)

        # Expected output (order doesn't matter for set comparison)
        expected_output = [
            (1, 101, 201, 301, 401, datetime(2023, 1, 9, 0, 0, 0)),
            (2, 102, 202, 302, 402, datetime(2023, 1, 10, 23, 59, 59)),
        ]

        # Action: Call the stored procedure
        _call_bq_stored_procedure(f"{project_id}.{dataset_id}.r_ausd_vertrag",
                                  {'p_JobKennung': 'TEST_JOB_KENNUNG', 'p_EintragsNr': 'TEST_ENTRY_NR'})

        # Assertions
        result_rows = _execute_bq_query(f"SELECT cntrct_validity_id, first_period_id, following_period_id, first_notice_period_id, follow_notice_period_id, bfc_age FROM `{project_id}.{dataset_id}.sof_ta_cntrct_valid` ORDER BY cntrct_validity_id")
        actual_output = [(row.cntrct_validity_id, row.first_period_id, row.following_period_id, row.first_notice_period_id, row.follow_notice_period_id, row.bfc_age) for row in result_rows]

        assert len(actual_output) == len(expected_output)
        assert set(actual_output) == set(expected_output)

        audit_log_rows = _execute_bq_query(f"SELECT records_loaded, status FROM `{project_id}.{dataset_id}.job_audit_log` ORDER BY run_timestamp DESC LIMIT 1")
        assert len(audit_log_rows) == 1
        assert audit_log_rows[0].records_loaded == 2
        assert audit_log_rows[0].status == 'SUCCESS'
    ```

---

### Test Case 2: `v_datum` Derivation Correctness

*   **Purpose:** Ensure the cutoff date `v_datum` is correctly determined by selecting the maximum `DATE(timecreated)` for `job_kennung = 'BERT_DROP_TEMP_TABLE'` from `dwtk_meldungen`.
*   **Setup:**
    1.  **`project.isbert_schema.dwtk_meldungen`:**
        ```sql
        INSERT INTO `project.isbert_schema.dwtk_meldungen` (job_kennung, timecreated) VALUES
        ('OTHER_JOB', '2023-03-01 10:00:00 UTC'),
        ('BERT_DROP_TEMP_TABLE', '2023-02-15 12:00:00 UTC'),
        ('BERT_DROP_TEMP_TABLE', '2023-02-20 08:00:00 UTC'), -- This should be the max date
        ('BERT_DROP_TEMP_TABLE', '2023-02-10 15:00:00 UTC');
        ```
    2.  **`project.source_dataset.cds_ta_cntrct_validity`:** Populate with some data that would be filtered by the derived `v_datum` (e.g., one record with `insert_at` before `2023-02-20` and `modified_at` NULL, and one after).
*   **Action:**
    1.  Execute the BigQuery stored procedure.
    2.  Query the `project.dataset.job_audit_log` for the `run_timestamp` of the latest successful entry.
    3.  Manually calculate the expected `v_datum` from the setup data.
*   **Pass/Fail Criterion:**
    1.  The `job_audit_log` entry for the run shows `status = 'SUCCESS'` and `records_loaded` matching the count based on the *correctly derived* `v_datum` ('2023-02-20').
    2.  (Implicitly) The `sof_ta_cntrct_valid` table contains data consistent with `v_datum` being '2023-02-20'.
*   **Runnable Test Code (pytest):**
    ```python
    def test_v_datum_derivation_correctness(bq_client, project_id, dataset_id, isbert_schema_id, source_dataset_id):
        _execute_bq_query(f"TRUNCATE TABLE `{project_id}.{isbert_schema_id}.dwtk_meldungen`")
        _execute_bq_query(f"TRUNCATE TABLE `{project_id}.{source_dataset_id}.cds_ta_cntrct_validity`")
        _execute_bq_query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.sof_ta_cntrct_valid`")
        _execute_bq_query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_audit_log`")

        dwtk_data = [
            {'job_kennung': 'OTHER_JOB', 'timecreated': datetime(2023, 3, 1, 10, 0, 0)},
            {'job_kennung': 'BERT_DROP_TEMP_TABLE', 'timecreated': datetime(2023, 2, 15, 12, 0, 0)},
            {'job_kennung': 'BERT_DROP_TEMP_TABLE', 'timecreated': datetime(2023, 2, 20, 8, 0, 0)}, # Max date
            {'job_kennung': 'BERT_DROP_TEMP_TABLE', 'timecreated': datetime(2023, 2, 10, 15, 0, 0)},
        ]
        _insert_bq_data(f"{project_id}.{isbert_schema_id}.dwtk_meldungen", dwtk_data)

        # Data for cds_ta_cntrct_validity to verify filtering with expected v_datum '2023-02-20'
        cds_data = [
            {'cntrct_validity_id': 1, 'insert_at': datetime(2023, 2, 19, 0, 0, 0), 'modified_at': None, **_default_contract_cols()}, # Included
            {'cntrct_validity_id': 2, 'insert_at': datetime(2023, 2, 20, 0, 0, 0), 'modified_at': datetime(2023, 2, 21, 0, 0, 0), **_default_contract_cols()}, # Included
            {'cntrct_validity_id': 3, 'insert_at': datetime(2023, 2, 21, 0, 0, 0), 'modified_at': None, **_default_contract_cols()}, # Excluded
            {'cntrct_validity_id': 4, 'insert_at': datetime(2023, 2, 19, 0, 0, 0), 'modified_at': datetime(2023, 2, 19, 0, 0, 0), **_default_contract_cols()}, # Excluded
        ]
        _insert_bq_data(f"{project_id}.{source_dataset_id}.cds_ta_cntrct_validity", cds_data)

        _call_bq_stored_procedure(f"{project_id}.{dataset_id}.r_ausd_vertrag",
                                  {'p_JobKennung': 'VDATUM_TEST', 'p_EintragsNr': 'VDATUM_ENTRY'})

        result_rows = _execute_bq_query(f"SELECT cntrct_validity_id FROM `{project_id}.{dataset_id}.sof_ta_cntrct_valid`")
        assert len(result_rows) == 2 # IDs 1 and 2 should be present

        audit_log_rows = _execute_bq_query(f"SELECT records_loaded, status FROM `{project_id}.{dataset_id}.job_audit_log` ORDER BY run_timestamp DESC LIMIT 1")
        assert audit_log_rows[0].records_loaded == 2
        assert audit_log_rows[0].status == 'SUCCESS'

    def _default_contract_cols():
        return {'first_period_id': 1, 'following_period_id': 2, 'first_notice_period_id': 3, 'follow_notice_period_id': 4}
    ```

---

### Test Case 3: Filtering Logic - `insert_at` and `modified_at` Combinations

*   **Purpose:** Validate the complex filtering logic: `DATE(cv.insert_at) <= v_datum AND (cv.modified_at IS NULL OR DATE(cv.modified_at) > v_datum)`. This covers transformation correctness.
*   **Setup:**
    1.  **`project.isbert_schema.dwtk_meldungen`:** Set `v_datum` to '2023-01-15'.
        ```sql
        INSERT INTO `project.isbert_schema.dwtk_meldungen` (job_kennung, timecreated) VALUES
        ('BERT_DROP_TEMP_TABLE', '2023-01-15 10:00:00 UTC');
        ```
    2.  **`project.source_dataset.cds_ta_cntrct_validity`:** Populate with records covering all filter conditions:
        *   `insert_at <= v_datum`, `modified_at IS NULL` (Expected: INCLUDED)
        *   `insert_at <= v_datum`, `modified_at > v_datum` (Expected: INCLUDED)
        *   `insert_at <= v_datum`, `modified_at = v_datum` (Expected: EXCLUDED, because `modified_at > v_datum` is false)
        *   `insert_at <= v_datum`, `modified_at < v_datum` (Expected: EXCLUDED)
        *   `insert_at > v_datum`, `modified_at IS NULL` (Expected: EXCLUDED)
        *   `insert_at > v_datum`, `modified_at > v_datum` (Expected: EXCLUDED)
*   **Action:**
    1.  Execute the BigQuery stored procedure.
    2.  Query the contents of `project.dataset.sof_ta_cntrct_valid`.
*   **Pass/Fail Criterion:** Only the records matching the "INCLUDED" conditions are present in `sof_ta_cntrct_valid`.
*   **Runnable Test Code (pytest):**
    ```python
    def test_filtering_logic_combinations(bq_client, project_id, dataset_id, isbert_schema_id, source_dataset_id):
        _execute_bq_query(f"TRUNCATE TABLE `{project_id}.{isbert_schema_id}.dwtk_meldungen`")
        _execute_bq_query(f"TRUNCATE TABLE `{project_id}.{source_dataset_id}.cds_ta_cntrct_validity`")
        _execute_bq_query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.sof_ta_cntrct_valid`")
        _execute_bq_query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_audit_log`")

        # Set v_datum to '2023-01-15'
        _insert_bq_data(f"{project_id}.{isbert_schema_id}.dwtk_meldungen",
                        [{'job_kennung': 'BERT_DROP_TEMP_TABLE', 'timecreated': datetime(2023, 1, 15, 10, 0, 0)}])

        cds_data = [
            # INCLUDED cases
            {'cntrct_validity_id': 1, 'insert_at': datetime(2023, 1, 14, 0, 0, 0), 'modified_at': None, **_default_contract_cols()}, # insert_at <= v_datum, modified_at IS NULL
            {'cntrct_validity_id': 2, 'insert_at': datetime(2023, 1, 15, 23, 59, 59), 'modified_at': datetime(2023, 1, 16, 0, 0, 0), **_default_contract_cols()}, # insert_at <= v_datum, modified_at > v_datum

            # EXCLUDED cases
            {'cntrct_validity_id': 3, 'insert_at': datetime(2023, 1, 15, 0, 0, 0), 'modified_at': datetime(2023, 1, 15, 12, 0, 0), **_default_contract_cols()}, # insert_at <= v_datum, modified_at = v_datum (DATE(modified_at) is not > v_datum)
            {'cntrct_validity_id': 4, 'insert_at': datetime(2023, 1, 14, 0, 0, 0), 'modified_at': datetime(2023, 1, 13, 0, 0, 0), **_default_contract_cols()}, # insert_at <= v_datum, modified_at < v_datum
            {'cntrct_validity_id': 5, 'insert_at': datetime(2023, 1, 16, 0, 0, 0), 'modified_at': None, **_default_contract_cols()}, # insert_at > v_datum
            {'cntrct_validity_id': 6, 'insert_at': datetime(2023, 1, 16, 0, 0, 0), 'modified_at': datetime(2023, 1, 17, 0, 0, 0), **_default_contract_cols()}, # insert_at > v_datum
        ]
        _insert_bq_data(f"{project_id}.{source_dataset_id}.cds_ta_cntrct_validity", cds_data)

        expected_ids = {1, 2}

        _call_bq_stored_procedure(f"{project_id}.{dataset_id}.r_ausd_vertrag",
                                  {'p_JobKennung': 'FILTER_TEST', 'p_EintragsNr': 'FILTER_ENTRY'})

        result_rows = _execute_bq_query(f"SELECT cntrct_validity_id FROM `{project_id}.{dataset_id}.sof_ta_cntrct_valid`")
        actual_ids = {row.cntrct_validity_id for row in result_rows}

        assert actual_ids == expected_ids
        audit_log_rows = _execute_bq_query(f"SELECT records_loaded FROM `{project_id}.{dataset_id}.job_audit_log` ORDER BY run_timestamp DESC LIMIT 1")
        assert audit_log_rows[0].records_loaded == len(expected_ids)
    ```

---

### Test Case 4: Column Mapping and Data Type Handling

*   **Purpose:** Verify that `insert_at` (DATETIME) is correctly mapped to `bfc_age` (DATETIME) and other columns retain their values and types without loss of precision or unexpected conversions. This covers transformation correctness.
*   **Setup:**
    1.  **`project.isbert_schema.dwtk_meldungen`:** Set `v_datum` to a date that includes the test data (e.g., '2023-01-01').
    2.  **`project.source_dataset.cds_ta_cntrct_validity`:** Populate with a single record having diverse values for all columns, including specific `DATETIME` values (e.g., with microseconds, different time zones if applicable, though BigQuery `DATETIME` is timezone-agnostic).
        ```sql
        INSERT INTO `project.source_dataset.cds_ta_cntrct_validity` (cntrct_validity_id, first_period_id, following_period_id, first_notice_period_id, follow_notice_period_id, insert_at, modified_at) VALUES
        (1001, 12345, 67890, 11223, 44556, '2022-12-31 23:59:59.123456 UTC', NULL);
        ```
*   **Action:**
    1.  Execute the BigQuery stored procedure.
    2.  Query the contents of `project.dataset.sof_ta_cntrct_valid`.
*   **Pass/Fail Criterion:** The single row in `sof_ta_cntrct_valid` matches the source data exactly for all columns, and `bfc_age` matches `insert_at` including any sub-second precision. Data types in the target table schema should match the expected BigQuery types (e.g., `INT64`, `DATETIME`).
*   **Runnable Test Code (pytest):**
    ```python
    def test_column_mapping_and_data_types(bq_client, project_id, dataset_id, isbert_schema_id, source_dataset_id):
        _execute_bq_query(f"TRUNCATE TABLE `{project_id}.{isbert_schema_id}.dwtk_meldungen`")
        _execute_bq_query(f"TRUNCATE TABLE `{project_id}.{source_dataset_id}.cds_ta_cntrct_validity`")
        _execute_bq_query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.sof_ta_cntrct_valid`")
        _execute_bq_query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_audit_log`")

        _insert_bq_data(f"{project_id}.{isbert_schema_id}.dwtk_meldungen",
                        [{'job_kennung': 'BERT_DROP_TEMP_TABLE', 'timecreated': datetime(2023, 1, 1, 0, 0, 0)}])

        test_insert_at = datetime(2022, 12, 31, 23, 59, 59, 123456)
        cds_data = [
            {'cntrct_validity_id': 1001, 'first_period_id': 12345, 'following_period_id': 67890,
             'first_notice_period_id': 11223, 'follow_notice_period_id': 44556,
             'insert_at': test_insert_at, 'modified_at': None},
        ]
        _insert_bq_data(f"{project_id}.{source_dataset_id}.cds_ta_cntrct_validity", cds_data)

        _call_bq_stored_procedure(f"{project_id}.{dataset_id}.r_ausd_vertrag",
                                  {'p_JobKennung': 'TYPE_TEST', 'p_EintragsNr': 'TYPE_ENTRY'})

        result_rows = _execute_bq_query(f"SELECT * FROM `{project_id}.{dataset_id}.sof_ta_cntrct_valid`")
        assert len(result_rows) == 1
        row = result_rows[0]

        assert row.cntrct_validity_id == 1001
        assert row.first_period_id == 12345
        assert row.following_period_id == 67890
        assert row.first_notice_period_id == 11223
        assert row.follow_notice_period_id == 44556
        assert row.bfc_age == test_insert_at # Verify exact datetime match

        # Schema assertion (conceptual, often done via BigQuery API directly)
        table = bq_client.get_table(f"{project_id}.{dataset_id}.sof_ta_cntrct_valid")
        schema_fields = {field.name: field.field_type for field in table.schema}
        assert schema_fields['cntrct_validity_id'] == 'INT64'
        assert schema_fields['bfc_age'] == 'DATETIME'
    ```

---

### Test Case 5: NULL Handling

*   **Purpose:** Ensure NULL values in source columns are correctly propagated to the target table and that the `modified_at IS NULL` filter condition works as expected. This covers transformation correctness and NULL handling.
*   **Setup:**
    1.  **`project.isbert_schema.dwtk_meldungen`:** Set `v_datum` to '2023-01-01'.
    2.  **`project.source_dataset.cds_ta_cntrct_validity`:** Populate with records including NULLs in various columns, especially `modified_at`.
        ```sql
        INSERT INTO `project.source_dataset.cds_ta_cntrct_validity` (cntrct_validity_id, first_period_id, following_period_id, first_notice_period_id, follow_notice_period_id, insert_at, modified_at) VALUES
        (1, NULL, 201, NULL, 401, '2022-12-31 00:00:00 UTC', NULL), -- Included: modified_at IS NULL
        (2, 102, NULL, 302, NULL, '2022-12-30 00:00:00 UTC', '2023-01-02 00:00:00 UTC'), -- Included: modified_at > v_datum
        (3, 103, 203, 303, 403, '2022-12-29 00:00:00 UTC', '2022-12-30 00:00:00 UTC'); -- Excluded: modified_at <= v_datum
        ```
*   **Action:**
    1.  Execute the BigQuery stored procedure.
    2.  Query the contents of `project.dataset.sof_ta_cntrct_valid`.
*   **Pass/Fail Criterion:**
    1.  The `sof_ta_cntrct_valid` table contains only the two expected records (IDs 1 and 2).
    2.  NULL values in the source columns for these records are correctly represented as NULLs in the corresponding target columns.
*   **Runnable Test Code (pytest):**
    ```python
    def test_null_handling(bq_client, project_id, dataset_id, isbert_schema_id, source_dataset_id):
        _execute_bq_query(f"TRUNCATE TABLE `{project_id}.{isbert_schema_id}.dwtk_meldungen`")
        _execute_bq_query(f"TRUNCATE TABLE `{project_id}.{source_dataset_id}.cds_ta_cntrct_validity`")
        _execute_bq_query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.sof_ta_cntrct_valid`")
        _execute_bq_query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_audit_log`")

        _insert_bq_data(f"{project_id}.{isbert_schema_id}.dwtk_meldungen",
                        [{'job_kennung': 'BERT_DROP_TEMP_TABLE', 'timecreated': datetime(2023, 1, 1, 0, 0, 0)}])

        cds_data = [
            {'cntrct_validity_id': 1, 'first_period_id': None, 'following_period_id': 201,
             'first_notice_period_id': None, 'follow_notice_period_id': 401,
             'insert_at': datetime(2022, 12, 31, 0, 0, 0), 'modified_at': None},
            {'cntrct_validity_id': 2, 'first_period_id': 102, 'following_period_id': None,
             'first_notice_period_id': 302, 'follow_notice_period_id': None,
             'insert_at': datetime(2022, 12, 30, 0, 0, 0), 'modified_at': datetime(2023, 1, 2, 0, 0, 0)},
            {'cntrct_validity_id': 3, 'first_period_id': 103, 'following_period_id': 203,
             'first_notice_period_id': 303, 'follow_notice_period_id': 403,
             'insert_at': datetime(2022, 12, 29, 0, 0, 0), 'modified_at': datetime(2022, 12, 30, 0, 0, 0)},
        ]
        _insert_bq_data(f"{project_id}.{source_dataset_id}.cds_ta_cntrct_validity", cds_data)

        _call_bq_stored_procedure(f"{project_id}.{dataset_id}.r_ausd_vertrag",
                                  {'p_JobKennung': 'NULL_TEST', 'p_EintragsNr': 'NULL_ENTRY'})

        result_rows = _execute_bq_query(f"SELECT cntrct_validity_id, first_period_id, following_period_id, first_notice_period_id, follow_notice_period_id FROM `{project_id}.{dataset_id}.sof_ta_cntrct_valid` ORDER BY cntrct_validity_id")
        assert len(result_rows) == 2

        # Check NULL propagation for ID 1
        row1 = result_rows[0]
        assert row1.cntrct_validity_id == 1
        assert row1.first_period_id is None
        assert row1.following_period_id == 201
        assert row1.first_notice_period_id is None
        assert row1.follow_notice_period_id == 401

        # Check NULL propagation for ID 2
        row2 = result_rows[1]
        assert row2.cntrct_validity_id == 2
        assert row2.first_period_id == 102
        assert row2.following_period_id is None
        assert row2.first_notice_period_id == 302
        assert row2.follow_notice_period_id is None
    ```

---

### Test Case 6: Empty Source Table (`cds_ta_cntrct_validity`)

*   **Purpose:** Verify the job handles an empty primary source table gracefully, resulting in an empty target table and a successful audit log entry with 0 records. This covers data quality and row count assertions.
*   **Setup:**
    1.  **`project.isbert_schema.dwtk_meldungen`:** Set `v_datum` to '2023-01-01'.
    2.  **`project.source_dataset.cds_ta_cntrct_validity`:** Empty.
    3.  **`project.dataset.sof_ta_cntrct_valid`:** Potentially contains old data from previous runs (to test truncation).
*   **Action:**
    1.  Execute the BigQuery stored procedure.
    2.  Query the contents of `project.dataset.sof_ta_cntrct_valid`.
    3.  Query the `project.dataset.job_audit_log` for the latest entry.
*   **Pass/Fail Criterion:**
    1.  `project.dataset.sof_ta_cntrct_valid` is empty.
    2.  `job_audit_log` shows `records_loaded = 0` and `status = 'SUCCESS'`.
*   **Runnable Test Code (pytest):**
    ```python
    def test_empty_source_table(bq_client, project_id, dataset_id, isbert_schema_id, source_dataset_id):
        _execute_bq_query(f"TRUNCATE TABLE `{project_id}.{isbert_schema_id}.dwtk_meldungen`")
        _execute_bq_query(f"TRUNCATE TABLE `{project_id}.{source_dataset_id}.cds_ta_cntrct_validity`")
        _execute_bq_query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.sof_ta_cntrct_valid`")
        _execute_bq_query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_audit_log`")

        # Populate target with some data to ensure truncation works
        _insert_bq_data(f"{project_id}.{dataset_id}.sof_ta_cntrct_valid",
                        [{'cntrct_validity_id': 999, 'bfc_age': datetime(2020,1,1), **_default_contract_cols()}])

        _insert_bq_data(f"{project_id}.{isbert_schema_id}.dwtk_meldungen",
                        [{'job_kennung': 'BERT_DROP_TEMP_TABLE', 'timecreated': datetime(2023, 1, 1, 0, 0, 0)}])

        # cds_ta_cntrct_validity remains empty

        _call_bq_stored_procedure(f"{project_id}.{dataset_id}.r_ausd_vertrag",
                                  {'p_JobKennung': 'EMPTY_SOURCE', 'p_EintragsNr': 'EMPTY_ENTRY'})

        result_rows = _execute_bq_query(f"SELECT * FROM `{project_id}.{dataset_id}.sof_ta_cntrct_valid`")
        assert len(result_rows) == 0

        audit_log_rows = _execute_bq_query(f"SELECT records_loaded, status FROM `{project_id}.{dataset_id}.job_audit_log` ORDER BY run_timestamp DESC LIMIT 1")
        assert audit_log_rows[0].records_loaded == 0
        assert audit_log_rows[0].status == 'SUCCESS'
    ```

---

### Test Case 7: `dwtk_meldungen` Missing `v_datum` Entry (Error Handling)

*   **Purpose:** Verify the stored procedure correctly handles the scenario where `v_datum` cannot be determined from `dwtk_meldungen` (e.g., no `BERT_DROP_TEMP_TABLE` entries), raising an error and logging a failure. This covers external system replacement and error handling.
*   **Setup:**
    1.  **`project.isbert_schema.dwtk_meldungen`:** Contains no entries with `job_kennung = 'BERT_DROP_TEMP_TABLE'`. It might be empty or contain other `job_kennung` values.
    2.  **`project.source_dataset.cds_ta_cntrct_validity`:** Contains some data.
    3.  **`project.dataset.sof_ta_cntrct_valid`:** Contains some initial "old" data.
*   **Action:**
    1.  Attempt to execute the BigQuery stored procedure.
    2.  Query the contents of `project.dataset.sof_ta_cntrct_valid`.
    3.  Query the `project.dataset.job_audit_log` for the latest entry.
*   **Pass/Fail Criterion:**
    1.  The stored procedure execution fails with an error message indicating `v_datum` could not be determined.
    2.  `project.dataset.sof_ta_cntrct_valid` is empty (due to `TRUNCATE` being executed before the `RAISE`).
    3.  `job_audit_log` shows `status = 'FAILED'` and the `message` contains the expected error text.
*   **Runnable Test Code (pytest):**
    ```python
    import pytest
    from google.api_core.exceptions import BadRequest # BigQuery error type

    def test_missing_v_datum_entry_error_handling(bq_client, project_id, dataset_id, isbert_schema_id, source_dataset_id):
        _execute_bq_query(f"TRUNCATE TABLE `{project_id}.{isbert_schema_id}.dwtk_meldungen`")
        _execute_bq_query(f"TRUNCATE TABLE `{project_id}.{source_dataset_id}.cds_ta_cntrct_validity`")
        _execute_bq_query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.sof_ta_cntrct_valid`")
        _execute_bq_query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_audit_log`")

        # Populate target with some data to ensure truncation works before error
        _insert_bq_data(f"{project_id}.{dataset_id}.sof_ta_cntrct_valid",
                        [{'cntrct_validity_id': 999, 'bfc_age': datetime(2020,1,1), **_default_contract_cols()}])

        # dwtk_meldungen contains no 'BERT_DROP_TEMP_TABLE' entries
        _insert_bq_data(f"{project_id}.{isbert_schema_id}.dwtk_meldungen",
                        [{'job_kennung': 'OTHER_JOB', 'timecreated': datetime(2023, 1, 1, 0, 0, 0)}])

        # cds_ta_cntrct_validity has data, but it won't be processed
        _insert_bq_data(f"{project_id}.{source_dataset_id}.cds_ta_cntrct_validity",
                        [{'cntrct_validity_id': 1, 'insert_at': datetime(2022,12,31), 'modified_at': None, **_default_contract_cols()}])

        with pytest.raises(BadRequest) as excinfo:
            _call_bq_stored_procedure(f"{project_id}.{dataset_id}.r_ausd_vertrag",
                                      {'p_JobKennung': 'VDATUM_MISSING', 'p_EintragsNr': 'VDATUM_MISSING_ENTRY'})

        assert "Cutoff date (v_datum) could not be determined" in str(excinfo.value)

        # Verify target table is empty (truncated before error)
        result_rows = _execute_bq_query(f"SELECT * FROM `{project_id}.{dataset_id}.sof_ta_cntrct_valid`")
        assert len(result_rows) == 0

        # Verify audit log entry
        audit_log_rows = _execute_bq_query(f"SELECT status, message FROM `{project_id}.{dataset_id}.job_audit_log` ORDER BY run_timestamp DESC LIMIT 1")
        assert len(audit_log_rows) == 1
        assert audit_log_rows[0].status == 'FAILED'
        assert "Cutoff date (v_datum) could not be determined" in audit_log_rows[0].message
    ```

---

### Test Case 8: Truncate and Load Behavior

*   **Purpose:** Confirm that the target table `sof_ta_cntrct_valid` is always truncated before new data is inserted, ensuring a full refresh behavior as specified. This covers data quality and schema assertions.
*   **Setup:**
    1.  **`project.dataset.sof_ta_cntrct_valid`:** Populate with "old" data (e.g., `cntrct_validity_id = 999`).
    2.  **`project.isbert_schema.dwtk_meldungen`:** Set `v_datum` to '2023-01-01'.
    3.  **`project.source_dataset.cds_ta_cntrct_validity`:** Populate with "new" data (e.g., `cntrct_validity_id = 1, 2`).
*   **Action:**
    1.  Execute the BigQuery stored procedure.
    2.  Query the contents of `project.dataset.sof_ta_cntrct_valid`.
*   **Pass/Fail Criterion:** The `sof_ta_cntrct_valid` table contains *only* the "new" data (IDs 1 and 2), and none of the "old" data (ID 999).
*   **Runnable Test Code (pytest):**
    ```python
    def test_truncate_and_load_behavior(bq_client, project_id, dataset_id, isbert_schema_id, source_dataset_id):
        _execute_bq_query(f"TRUNCATE TABLE `{project_id}.{isbert_schema_id}.dwtk_meldungen`")
        _execute_bq_query(f"TRUNCATE TABLE `{project_id}.{source_dataset_id}.cds_ta_cntrct_validity`")
        _execute_bq_query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.sof_ta_cntrct_valid`")
        _execute_bq_query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_audit_log`")

        # Populate target with old data
        _insert_bq_data(f"{project_id}.{dataset_id}.sof_ta_cntrct_valid",
                        [{'cntrct_validity_id': 999, 'bfc_age': datetime(2020, 1, 1), **_default_contract_cols()}])

        _insert_bq_data(f"{project_id}.{isbert_schema_id}.dwtk_meldungen",
                        [{'job_kennung': 'BERT_DROP_TEMP_TABLE', 'timecreated': datetime(2023, 1, 1, 0, 0, 0)}])

        # Populate source with new data
        cds_data = [
            {'cntrct_validity_id': 1, 'insert_at': datetime(2022, 12, 31, 0, 0, 0), 'modified_at': None, **_default_contract_cols()},
            {'cntrct_validity_id': 2, 'insert_at': datetime(2022, 12, 30, 0, 0, 0), 'modified_at': datetime(2023, 1, 2, 0, 0, 0), **_default_contract_cols()},
        ]
        _insert_bq_data(f"{project_id}.{source_dataset_id}.cds_ta_cntrct_validity", cds_data)

        _call_bq_stored_procedure(f"{project_id}.{dataset_id}.r_ausd_vertrag",
                                  {'p_JobKennung': 'TRUNCATE_TEST', 'p_EintragsNr': 'TRUNCATE_ENTRY'})

        result_rows = _execute_bq_query(f"SELECT cntrct_validity_id FROM `{project_id}.{dataset_id}.sof_ta_cntrct_valid` ORDER BY cntrct_validity_id")
        actual_ids = {row.cntrct_validity_id for row in result_rows}

        assert actual_ids == {1, 2} # Only new data should be present
        assert 999 not in actual_ids # Old data must be gone
    ```

---

### Test Case 9: Orchestration Parameters and Audit Log

*   **Purpose:** Verify that parameters passed from the Airflow DAG (`p_JobKennung`, `p_EintragsNr`) are correctly received by the stored procedure and accurately recorded in the `job_audit_log` table. This covers external system replacements and data quality.
*   **Setup:**
    1.  **Airflow Variables:** Configure `JOB_KENNUNG_PARAM = 'AIRFLOW_JOB_A'` and `EINTRAGS_NR_PARAM = 'AIRFLOW_ENTRY_123'`.
    2.  **Source Data:** Populate `dwtk_meldungen` and `cds_ta_cntrct_validity` to ensure a successful run (e.g., as in Test Case 1).
*   **Action:**
    1.  Trigger the `k_ausd_v_ta_cntrct_valid_bigquery_dag` Airflow DAG.
    2.  After the DAG completes, query the `project.dataset.job_audit_log` for the latest entry.
*   **Pass/Fail Criterion:** The `job_audit_log` contains an entry where `job_kennung` is 'AIRFLOW_JOB_A', `entry_number` is 'AIRFLOW_ENTRY_123', `status` is 'SUCCESS', and `records_loaded` matches the expected count from the source data.
*   **Runnable Test Code (Conceptual Airflow/SQL):**
    ```python
    # This test is primarily an Airflow DAG execution and BigQuery SQL assertion.
    # It would involve deploying the DAG and running it.

    # 1. Ensure Airflow Variables are set:
    #    airflow variables set bq_project_id your-gcp-project-id
    #    airflow variables set bq_dataset_id dataset
    #    airflow variables set job_kennung_param AIRFLOW_JOB_A
    #    airflow variables set eintrags_nr_param AIRFLOW_ENTRY_123

    # 2. Setup BigQuery source data (similar to Test Case 1)
    #    _execute_bq_query(f"TRUNCATE TABLE `{project_id}.{isbert_schema_id}.dwtk_meldungen`")
    #    _execute_bq_query(f"TRUNCATE TABLE `{project_id}.{source_dataset_id}.cds_ta_cntrct_validity`")
    #    _execute_bq_query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.sof_ta_cntrct_valid`")
    #    _execute_bq_query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_audit_log`")
    #    _insert_bq_data(f"{project_id}.{isbert_schema_id}.dwtk_meldungen", [...])
    #    _insert_bq_data(f"{project_id}.{source_dataset_id}.cds_ta_cntrct_validity", [...])
    #    (Expected 2 records to be loaded)

    # 3. Trigger the Airflow DAG (e.g., via Airflow UI or CLI)
    #    airflow dags trigger k_ausd_v_ta_cntrct_valid_bigquery_dag

    # 4. After DAG completes, assert in BigQuery:
    # SQL Assertion:
    """
    SELECT
        job_kennung,
        entry_number,
        records_loaded,
        status
    FROM
        `project.dataset.job_audit_log`
    ORDER BY
        run_timestamp DESC
    LIMIT 1;
    """
    # Expected Result:
    # job_kennung      | entry_number    | records_loaded | status
    # -----------------|-----------------|----------------|--------
    # AIRFLOW_JOB_A    | AIRFLOW_ENTRY_123 | 2              | SUCCESS

    # Python/pytest equivalent for assertion after DAG run:
    # audit_log_rows = _execute_bq_query(f"SELECT job_kennung, entry_number, records_loaded, status FROM `{project_id}.{dataset_id}.job_audit_log` ORDER BY run_timestamp DESC LIMIT 1")
    # assert len(audit_log_rows) == 1
    # assert audit_log_rows[0].job_kennung == 'AIRFLOW_JOB_A'
    # assert audit_log_rows[0].entry_number == 'AIRFLOW_ENTRY_123'
    # assert audit_log_rows[0].records_loaded == 2
    # assert audit_log_rows[0].status == 'SUCCESS'
    ```