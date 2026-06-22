As a senior data-migration QA engineer, I've developed a suite of validation tests for the `k_ausd_adressen.ksh` migration to BigQuery and Airflow. These tests aim to ensure behavioral equivalence, data integrity, and correctness across all specified transformation aspects.

---

## Migration Validation Tests for `k_ausd_adressen.ksh`

### General Test Setup Notes:
*   `PROJECT_ID` and `DATASET` are placeholders for your Google Cloud Project ID and BigQuery dataset where the stored procedure resides (e.g., `your-gcp-project.your_dataset`).
*   `raw`, `staging`, `target`, and `metrics` are assumed to be BigQuery datasets within `PROJECT_ID`.
*   For `pytest` examples, assume a `bq_client` fixture is available, providing an authenticated `google.cloud.bigquery.Client` instance.
*   The `setup_bq_table` and `call_stored_procedure` helper functions (conceptual, not fully implemented here) would facilitate test data setup and execution.
*   **Crucial for Output Parity**: Since the legacy source is unavailable, the "expected output" for all tests must be derived by manually applying the transformation logic described in the design document to the sample input data. This process effectively re-implements the legacy logic to create a baseline for comparison.

---

### Test Case 1: End-to-End Output Parity (Happy Path)

*   **Purpose**: Verify that the migrated job, when executed with typical valid inputs, produces the exact same final data in all target tables as the legacy system would have. This is the primary test for output parity.
*   **Setup**:
    1.  **Populate Raw Tables**: Insert a comprehensive set of sample data into all `raw` BigQuery tables (`PROJECT_ID.raw.cds_ta_bp_ref`, `PROJECT_ID.raw.cds_ta_inv_definition`, `PROJECT_ID.raw.glv_ta_country`, `PROJECT_ID.raw.glv_ta_description`, `PROJECT_ID.raw.bpd_ta_reachability`, `PROJECT_ID.raw.bpd_ta_business_partner`). This data should cover:
        *   Records that satisfy all filtering conditions (date ranges, `is_production=1`, specific `bp_ref_ty`/`address_ref_ty`).
        *   Records that should be excluded by filters.
        *   Data to test all join conditions (matching and non-matching keys).
        *   Data to test `UNION ALL` logic (e.g., for `sof_ta_bp_ref_re`).
        *   Data to test `SUBSTR` and `NULL` handling for `land_sd`.
        *   Data for `sof_ta_e_regulierer` population.
    2.  **Define Parameters**: Choose a `p_stichtag_str` (e.g., `'20230115'`) and other parameters (`p_job_kennung`, `p_eintrags_nr`, `p_wiederanlauf_wert`).
    3.  **Pre-calculate Expected Output**: Based on the sample input data and the transformation logic from the design document, generate the *exact expected rows* for all final target tables: `PROJECT_ID.target.sof_ta_e_reach_gp`, `_re`, `_ev`, `_dn`, `PROJECT_ID.target.sof_ta_e_business_gp`, `_re`, `_ev`, `_dn`, and `PROJECT_ID.target.sof_ta_e_regulierer`.
*   **Action**:
    1.  Trigger the Airflow DAG `k_ausd_adressen_dag` with the chosen parameters. For `p_stichtag_str`, use `ds_nodash` or explicitly set the `execution_date` to match your test date.
*   **Pass/Fail Criterion**:
    1.  The Airflow DAG `k_ausd_adressen_dag` completes successfully.
    2.  The `PROJECT_ID.metrics.job_log` table contains a 'SUCCESS' entry for the executed job, and its `record_count` matches the pre-calculated expected count for `sof_ta_e_regulierer`.
    3.  For each target table, the actual data (row count and content) exactly matches the pre-calculated expected data. This comparison should be order-independent.

    ```python
    import pytest
    from google.cloud import bigquery
    from datetime import datetime, date

    # Assume bq_client, project_id, dataset_id are pytest fixtures
    # Assume helper functions like setup_bq_table, call_stored_procedure are defined

    def test_e2e_output_parity_happy_path(bq_client, project_id, dataset_id):
        # --- Setup: Populate raw tables with sample data ---
        # Example for cds_ta_bp_ref (adjust with full schema and diverse data)
        cds_ta_bp_ref_data = [
            {"bp_id": 1, "reachability_id": 101, "cntrct_cp2_id": 1, "inv_def_invrec_id": 1, "bpr_inst_evnrec_id": 1, "bpr_inst_srvusr_id": 1,
             "insert_at": datetime(2023, 1, 1, 10, 0, 0).isoformat() + "Z", "modified_at": None,
             "valid_from": datetime(2022, 1, 1, 0, 0, 0).isoformat() + "Z", "valid_to": None,
             "is_production": 1, "bp_ref_ty": 4, "address_ref_ty": 6, "inv_def_mopref_id": None, "mop_bp_id": None, "means_of_payment_id": None, "mop_ref_ty": None},
            # Add more data for other bp_ref_ty/address_ref_ty, inv_definition, country, description, reachability, business_partner
            # ...
        ]
        # setup_bq_table(bq_client, f"{project_id}.raw.cds_ta_bp_ref", cds_ta_bp_ref_data, CDS_TA_BP_REF_SCHEMA)
        # ... (setup other raw tables)

        stichtag_str = '20230115'
        job_kennung = 'K_AUSD_ADRESSEN_TEST_HP'
        eintrags_nr = 1

        # --- Action: Execute the stored procedure (simulating Airflow DAG) ---
        # In a real Airflow test, you'd trigger the DAG and wait for its completion.
        # For direct SP testing:
        # call_stored_procedure(bq_client, project_id, dataset_id, "sp_ausd_adressen_main", [
        #     {"name": "p_job_kennung", "parameterType": {"type": "STRING"}, "value": job_kennung},
        #     {"name": "p_eintrags_nr", "parameterType": {"type": "INT64"}, "value": eintrags_nr},
        #     {"name": "p_stichtag_str", "parameterType": {"type": "STRING"}, "value": stichtag_str},
        #     {"name": "p_wiederanlauf_wert", "parameterType": {"type": "STRING"}, "value": "0"}
        # ])

        # --- Assertions ---
        # 1. Check job log for success
        job_log_query = f"""
            SELECT status, record_count, key_date
            FROM `{project_id}.metrics.job_log`
            WHERE job_id = '{job_kennung}' AND entry_number = {eintrags_nr}
            ORDER BY start_timestamp DESC LIMIT 1
        """
        job_log_result = list(bq_client.query(job_log_query).result())
        assert len(job_log_result) == 1
        assert job_log_result[0].status == 'SUCCESS'
        assert job_log_result[0].key_date == date(2023, 1, 15)
        # assert job_log_result[0].record_count == expected_regulierer_count # Requires pre-calculated count

        # 2. Compare target tables with pre-calculated expected data
        # Example for sof_ta_e_reach_gp:
        actual_gp_query = f"SELECT * FROM `{project_id}.target.sof_ta_e_reach_gp` ORDER BY bp_id, reachability_id"
        actual_gp_data = [dict(row) for row in bq_client.query(actual_gp_query).result()]

        expected_gp_data = [
            # Pre-calculated expected rows as dictionaries, e.g.:
            # {"bp_id": 1, "reachability_id": 101, "obj_version": 1, "country_code": "DE", ... "land_sd": "GER"},
            # ...
        ]
        # Sort both lists for order-independent comparison
        assert sorted(actual_gp_data, key=lambda x: (x['bp_id'], x['reachability_id'])) == \
               sorted(expected_gp_data, key=lambda x: (x['bp_id'], x['reachability_id']))

        # Repeat similar assertions for all other target tables:
        # sof_ta_e_reach_re, sof_ta_e_reach_ev, sof_ta_e_reach_dn
        # sof_ta_e_business_gp, sof_ta_e_business_re, sof_ta_e_business_ev, sof_ta_e_business_dn
        # sof_ta_e_regulierer
    ```

---

### Test Case 2: Date Filtering Logic (Transformation Correctness)

*   **Purpose**: Verify that the `insert_at`, `modified_at`, `valid_from`, `valid_to` filters, combined with `is_production = 1`, correctly include or exclude records based on the `p_stichtag_str` parameter.
*   **Setup**:
    1.  **Populate `raw.cds_ta_bp_ref`**: Insert records covering various date scenarios relative to a chosen `p_stichtag_str` (e.g., `'20230115'`):
        *   Record 1: All dates valid, `is_production = 1` (should be included).
        *   Record 2: `insert_at` > `p_stichtag_str` (future insert, should be excluded).
        *   Record 3: `modified_at` is not NULL and <= `p_stichtag_str` (modified before or on stichtag, should be excluded if `modified_at` is the *last* valid point). *Correction: The logic is `modified_at IS NULL OR modified_at > TIMESTAMP(v_datum_date)`. So, if `modified_at` is present and <= `v_datum_date`, it means it was modified *before or on* the stichtag, and thus the record is *not* valid *after* that modification. This means it should be excluded if `modified_at` is present and <= `v_datum_date`. This is a common pattern for "as-of" queries.*
        *   Record 4: `valid_from` > `p_stichtag_str` (future valid_from, should be excluded).
        *   Record 5: `valid_to` is not NULL and <= `p_stichtag_str` (validity ended before or on stichtag, should be excluded).
        *   Record 6: `is_production = 0` (should be excluded).
        *   Record 7: `modified_at` is NULL, `valid_to` is NULL (always valid if `insert_at` and `valid_from` are okay).
    2.  **Clear Staging/Target Tables**: Ensure relevant staging tables are empty before execution.
*   **Action**:
    1.  Execute the `sp_ausd_adressen_main` stored procedure with `p_stichtag_str = '20230115'`.
*   **Pass/Fail Criterion**:
    1.  The row count in `PROJECT_ID.staging.sof_ta_bp_ref_gp` (and other `sof_ta_bp_ref_*` tables) matches the expected count based on the precise filtering logic applied to the input data.
    2.  Specific records are present or absent in `PROJECT_ID.staging.sof_ta_bp_ref_gp` as per the date and `is_production` filters.

    ```sql
    -- Example SQL for setting up raw.cds_ta_bp_ref for this test
    INSERT INTO `PROJECT_ID.raw.cds_ta_bp_ref` (
        bp_id, insert_at, modified_at, valid_from, valid_to, is_production, bp_ref_ty, address_ref_ty,
        reachability_id, cntrct_cp2_id, inv_def_invrec_id, bpr_inst_evnrec_id, bpr_inst_srvusr_id, inv_def_mopref_id, mop_bp_id, means_of_payment_id, mop_ref_ty
    ) VALUES
    (1, TIMESTAMP('2023-01-01'), NULL, TIMESTAMP('2022-01-01'), NULL, 1, 4, 6, 101,1,1,1,1,1,1,1,1), -- Included
    (2, TIMESTAMP('2023-01-16'), NULL, TIMESTAMP('2022-01-01'), NULL, 1, 4, 6, 102,1,1,1,1,1,1,1,1), -- Excluded (insert_at > stichtag)
    (3, TIMESTAMP('2023-01-01'), TIMESTAMP('2023-01-10'), TIMESTAMP('2022-01-01'), NULL, 1, 4, 6, 103,1,1,1,1,1,1,1,1), -- Excluded (modified_at <= stichtag)
    (4, TIMESTAMP('2023-01-01'), NULL, TIMESTAMP('2023-01-16'), NULL, 1, 4, 6, 104,1,1,1,1,1,1,1,1), -- Excluded (valid_from > stichtag)
    (5, TIMESTAMP('2023-01-01'), NULL, TIMESTAMP('2022-01-01'), TIMESTAMP('2023-01-10'), 1, 4, 6, 105,1,1,1,1,1,1,1,1), -- Excluded (valid_to <= stichtag)
    (6, TIMESTAMP('2023-01-01'), NULL, TIMESTAMP('2022-01-01'), NULL, 0, 4, 6, 106,1,1,1,1,1,1,1,1); -- Excluded (is_production = 0)

    -- After SP execution with p_stichtag_str = '20230115'
    SELECT bp_id FROM `PROJECT_ID.staging.sof_ta_bp_ref_gp` ORDER BY bp_id;
    -- Expected result:
    -- bp_id
    -- -----
    -- 1
    ```

---

### Test Case 3: `UNION ALL` Logic and NULL Handling (Transformation Correctness)

*   **Purpose**: Verify that `PROJECT_ID.staging.sof_ta_bp_ref_re` correctly combines data from `cds_ta_bp_ref` and `cds_ta_inv_definition` using `UNION ALL`, including correct column mapping and `NULL` handling for columns not present in both sources.
*   **Setup**:
    1.  **Populate `raw.cds_ta_bp_ref`**: Insert records matching `bp_ref_ty=1` and `address_ref_ty=5`, with valid dates.
    2.  **Populate `raw.cds_ta_inv_definition`**: Insert records matching `rdndant_invrec=0`, with valid dates.
    3.  Ensure some `bp_id`s are unique to each source, and some might overlap (though `UNION ALL` will keep both if all columns differ, or one if identical).
    4.  Set `p_stichtag_str`.
*   **Action**:
    1.  Execute the `sp_ausd_adressen_main` stored procedure.
*   **Pass/Fail Criterion**:
    1.  The row count in `PROJECT_ID.staging.sof_ta_bp_ref_re` is the sum of the rows selected by each `SELECT` statement before the `UNION ALL`.
    2.  Records originating from `cds_ta_bp_ref` have `NULL` values for `inv_def_invrec_id` (if not mapped from `cds_ta_bp_ref` itself).
    3.  Records originating from `cds_ta_inv_definition` have `NULL` values for `cntrct_cp2_id`, `bpr_inst_evnrec_id`, `bpr_inst_srvusr_id` as explicitly set in the generated code.

    ```sql
    -- Example SQL for setting up raw tables
    INSERT INTO `PROJECT_ID.raw.cds_ta_bp_ref` (bp_id, reachability_id, cntrct_cp2_id, inv_def_invrec_id, bpr_inst_evnrec_id, bpr_inst_srvusr_id, insert_at, modified_at, valid_from, valid_to, is_production, bp_ref_ty, address_ref_ty, inv_def_mopref_id, mop_bp_id, means_of_payment_id, mop_ref_ty) VALUES
    (10, 110, 1, 1, 1, 1, TIMESTAMP('2023-01-01'), NULL, TIMESTAMP('2022-01-01'), NULL, 1, 1, 5, NULL, NULL, NULL, NULL),
    (11, 111, 2, 2, 2, 2, TIMESTAMP('2023-01-01'), NULL, TIMESTAMP('2022-01-01'), NULL, 1, 1, 5, NULL, NULL, NULL, NULL);

    INSERT INTO `PROJECT_ID.raw.cds_ta_inv_definition` (rdndnt_cp2_bp_id, rdndnt_cp2_reachability_id, inv_definition_id, insert_at, modified_at, valid_from, valid_to, is_production, rdndant_invrec) VALUES
    (20, 120, 1001, TIMESTAMP('2023-01-01'), NULL, TIMESTAMP('2022-01-01'), NULL, 1, 0),
    (21, 121, 1002, TIMESTAMP('2023-01-01'), NULL, TIMESTAMP('2022-01-01'), NULL, 1, 0);

    -- After SP execution with p_stichtag_str = '20230115'
    SELECT bp_id, reachability_id, cntrct_cp2_id, inv_def_invrec_id, bpr_inst_evnrec_id, bpr_inst_srvusr_id
    FROM `PROJECT_ID.staging.sof_ta_bp_ref_re` ORDER BY bp_id;
    -- Expected result (4 rows):
    -- bp_id | reachability_id | cntrct_cp2_id | inv_def_invrec_id | bpr_inst_evnrec_id | bpr_inst_srvusr_id
    -- ------|-----------------|---------------|-------------------|--------------------|--------------------
    -- 10    | 110             | 1             | 1                 | 1                  | 1
    -- 11    | 111             | 2             | 2                 | 2                  | 2
    -- 20    | 120             | NULL          | 1001              | NULL               | NULL
    -- 21    | 121             | NULL          | 1002              | NULL               | NULL
    ```

---

### Test Case 4: Join Logic and `SUBSTR` Function (Transformation Correctness)

*   **Purpose**: Verify the join conditions between `sof_ta_bp_ref_*`, `sof_ta_reachability`, and `sof_ta_laender_kng` are correct, and that the `land_sd` column is derived correctly using `SUBSTR` and handles `NULL` `short_description` values.
*   **Setup**:
    1.  **Populate `raw.cds_ta_bp_ref`**: Insert data that will populate `sof_ta_bp_ref_gp`.
    2.  **Populate `raw.bpd_ta_reachability`**: Insert data with various `country_code` values, ensuring some match `glv_ta_country` and some do not.
    3.  **Populate `raw.glv_ta_country` and `raw.glv_ta_description`**:
        *   Matching `country_code` and `description_id` for valid countries (e.g., 'DE' -> 'Germany', 'US' -> 'USA').
        *   A country with a `short_description` shorter than 3 characters (e.g., 'AT' -> 'Aut').
        *   A country with a `NULL` `short_description` or no matching `glv_ta_description` entry.
    4.  Set `p_stichtag_str`.
*   **Action**:
    1.  Execute the `sp_ausd_adressen_main` stored procedure.
*   **Pass/Fail Criterion**:
    1.  The row count in `PROJECT_ID.target.sof_ta_e_reach_gp` (and other `_e_reach_*` tables) matches the expected count based on the join logic.
    2.  The `land_sd` column values are correctly derived:
        *   `SUBSTR('Germany', 1, 3)` results in `'Ger'`.
        *   `SUBSTR('USA', 1, 3)` results in `'USA'`.
        *   `SUBSTR('Aut', 1, 3)` results in `'Aut'`.
        *   If `short_description` is `NULL` or no match in `sof_ta_laender_kng` (due to `LEFT JOIN`), `land_sd` is `NULL`.

    ```sql
    -- Example SQL for setting up raw tables (simplified)
    INSERT INTO `PROJECT_ID.raw.cds_ta_bp_ref` (bp_id, reachability_id, insert_at, valid_from, is_production, bp_ref_ty, address_ref_ty, cntrct_cp2_id, inv_def_invrec_id, bpr_inst_evnrec_id, bpr_inst_srvusr_id, inv_def_mopref_id, mop_bp_id, means_of_payment_id, mop_ref_ty) VALUES
    (1, 101, TIMESTAMP('2023-01-01'), TIMESTAMP('2022-01-01'), 1, 4, 6, 1,1,1,1,1,1,1,1),
    (2, 102, TIMESTAMP('2023-01-01'), TIMESTAMP('2022-01-01'), 1, 4, 6, 1,1,1,1,1,1,1,1),
    (3, 103, TIMESTAMP('2023-01-01'), TIMESTAMP('2022-01-01'), 1, 4, 6, 1,1,1,1,1,1,1,1),
    (4, 104, TIMESTAMP('2023-01-01'), TIMESTAMP('2022-01-01'), 1, 4, 6, 1,1,1,1,1,1,1,1);

    INSERT INTO `PROJECT_ID.raw.bpd_ta_reachability` (bp_id, reachability_id, country_code, insert_at, valid_from, is_production, obj_version, for_the_attention_of, address_attachment, address_attachment_org, corp_unit, surname_s, first_name_g, zip_code, city, pobox, street, house_nr, public_area_a, private_area_p, corp_unit_ou1, address_line_1, address_line_2, reachable_from, reachable_thru) VALUES
    (1, 101, 'DE', TIMESTAMP('2023-01-01'), TIMESTAMP('2022-01-01'), 1, 1, 'Attn', 'AddrAtt', 'OrgAtt', 'CorpU', 'Surname', 'Firstname', '12345', 'City', 'Pobox', 'Street', '1A', 'PubA', 'PrivA', 'CorpU1', 'AddrL1', 'AddrL2', TIMESTAMP('2022-01-01'), TIMESTAMP('2024-01-01')),
    (2, 102, 'US', TIMESTAMP('2023-01-01'), TIMESTAMP('2022-01-01'), 1, 1, 'Attn', 'AddrAtt', 'OrgAtt', 'CorpU', 'Surname', 'Firstname', '12345', 'City', 'Pobox', 'Street', '1A', 'PubA', 'PrivA', 'CorpU1', 'AddrL1', 'AddrL2', TIMESTAMP('2022-01-01'), TIMESTAMP('2024-01-01')),
    (3, 103, 'AT', TIMESTAMP('2023-01-01'), TIMESTAMP('2022-01-01'), 1, 1, 'Attn', 'AddrAtt', 'OrgAtt', 'CorpU', 'Surname', 'Firstname', '12345', 'City', 'Pobox', 'Street', '1A', 'PubA', 'PrivA', 'CorpU1', 'AddrL1', 'AddrL2', TIMESTAMP('2022-01-01'), TIMESTAMP('2024-01-01')),
    (4, 104, 'XX', TIMESTAMP('2023-01-01'), TIMESTAMP('2022-01-01'), 1, 1, 'Attn', 'AddrAtt', 'OrgAtt', 'CorpU', 'Surname', 'Firstname', '12345', 'City', 'Pobox', 'Street', '1A', 'PubA', 'PrivA', 'CorpU1', 'AddrL1', 'AddrL2', TIMESTAMP('2022-01-01'), TIMESTAMP('2024-01-01')); -- No matching country

    INSERT INTO `PROJECT_ID.raw.glv_ta_country` (country_code, description_id, valid, parent_country_code, eu_indicator, sap_code, corr_code) VALUES
    ('DE', 100, 1, NULL, 1, 'DE', 'DE'),
    ('US', 101, 1, NULL, 0, 'US', 'US'),
    ('AT', 102, 1, NULL, 1, 'AT', 'AT');

    INSERT INTO `PROJECT_ID.raw.glv_ta_description` (description_id, language, short_description, description, long_description) VALUES
    (100, 'EN', 'Germany', 'Federal Republic of Germany', 'Long description for Germany'),
    (101, 'EN', 'USA', 'United States of America', 'Long description for USA'),
    (102, 'EN', 'Aut', 'Austria', 'Long description for Austria');

    -- After SP execution with p_stichtag_str = '20230115'
    SELECT bp_id, country_code, land_sd FROM `PROJECT_ID.target.sof_ta_e_reach_gp` ORDER BY bp_id;
    -- Expected result:
    -- bp_id | country_code | land_sd
    -- ------|--------------|---------
    -- 1     | DE           | Ger
    -- 2     | US           | USA
    -- 3     | AT           | Aut
    -- 4     | XX           | NULL
    ```

---

### Test Case 5: `DISTINCT` and Intermediate Table Cleanup (Transformation Correctness & Idempotency)

*   **Purpose**: Verify that `sof_ta_bp_ref_*_nodp` tables correctly extract distinct `bp_id`s and that all intermediate staging tables are truncated after the stored procedure completes, ensuring idempotency.
*   **Setup**:
    1.  **Populate `raw.cds_ta_bp_ref`**: Insert records that will result in duplicate `bp_id`s in `PROJECT_ID.staging.sof_ta_bp_ref_gp` (e.g., two different `reachability_id`s for the same `bp_id`).
    2.  Populate other `raw` tables as needed to allow the job to run fully.
    3.  Set `p_stichtag_str`.
*   **Action**:
    1.  Execute the `sp_ausd_adressen_main` stored procedure.
    2.  (Optional for idempotency) Execute the `sp_ausd_adressen_main` stored procedure a second time with the exact same inputs.
*   **Pass/Fail Criterion**:
    1.  The row count in `PROJECT_ID.staging.sof_ta_bp_ref_gp_nodp` (and other `_nodp` tables) matches the count of *distinct* `bp_id`s from its source `sof_ta_bp_ref_gp`.
    2.  After the stored procedure completes, all tables in the `PROJECT_ID.staging` dataset are empty (contain 0 rows).
    3.  If executed twice, the final data in all target tables is identical after both runs, and the `metrics.job_log` shows two successful entries with identical record counts.

    ```sql
    -- Example SQL for setting up raw.cds_ta_bp_ref
    INSERT INTO `PROJECT_ID.raw.cds_ta_bp_ref` (bp_id, reachability_id, insert_at, valid_from, is_production, bp_ref_ty, address_ref_ty, cntrct_cp2_id, inv_def_invrec_id, bpr_inst_evnrec_id, bpr_inst_srvusr_id, inv_def_mopref_id, mop_bp_id, means_of_payment_id, mop_ref_ty) VALUES
    (1, 101, TIMESTAMP('2023-01-01'), TIMESTAMP('2022-01-01'), 1, 4, 6, 1,1,1,1,1,1,1,1),
    (1, 102, TIMESTAMP('2023-01-01'), TIMESTAMP('2022-01-01'), 1, 4, 6, 1,1,1,1,1,1,1,1), -- Duplicate bp_id
    (2, 103, TIMESTAMP('2023-01-01'), TIMESTAMP('2022-01-01'), 1, 4, 6, 1,1,1,1,1,1,1,1);

    -- After SP execution with p_stichtag_str = '20230115'
    -- Check distinct count
    SELECT COUNT(*) FROM `PROJECT_ID.staging.sof_ta_bp_ref_gp_nodp`;
    -- Expected: 2 (for bp_id 1 and 2)

    -- Check cleanup of staging tables
    SELECT COUNT(*) FROM `PROJECT_ID.staging.sof_ta_bp_ref_gp`;
    -- Expected: 0
    SELECT COUNT(*) FROM `PROJECT_ID.staging.sof_ta_reachability`;
    -- Expected: 0
    -- ... (repeat for all staging tables)
    ```

---

### Test Case 6: Invalid `p_stichtag_str` (Error Handling)

*   **Purpose**: Verify that the stored procedure correctly handles invalid date formats for `p_stichtag_str`, logs the error, and prevents further processing.
*   **Setup**:
    1.  Ensure `PROJECT_ID.metrics.job_log` is accessible.
    2.  No specific data needed in `raw` tables, but they should exist.
*   **Action**:
    1.  Execute the `sp_ausd_adressen_main` stored procedure with an invalid `p_stichtag_str` (e.g., `'2023-01-15'`, `'INVALID_DATE'`, `'202301'`).
*   **Pass/Fail Criterion**:
    1.  The stored procedure execution fails and raises an exception (e.g., `google.api_core.exceptions.BadRequest` if called via client library).
    2.  The `PROJECT_ID.metrics.job_log` table contains a 'FAILED' entry for the run, with an `error_message` indicating an invalid date format.
    3.  No data is inserted into any target or staging tables (or they remain empty if the error occurs before any inserts).

    ```python
    import pytest
    from google.cloud import bigquery

    def test_invalid_stichtag_str_error_handling(bq_client, project_id, dataset_id):
        job_kennung = 'INVALID_DATE_TEST'
        eintrags_nr = 2
        invalid_stichtag = '2023-01-15' # Expected YYYYMMDD

        # --- Action: Call SP with invalid date ---
        with pytest.raises(Exception) as excinfo:
            # This assumes call_stored_procedure wraps the BigQuery client call
            # and re-raises exceptions.
            # For direct BigQuery client, it would be client.query(query).result()
            # and the exception would be google.api_core.exceptions.BadRequest
            call_stored_procedure(bq_client, project_id, dataset_id, "sp_ausd_adressen_main", [
                {"name": "p_job_kennung", "parameterType": {"type": "STRING"}, "value": job_kennung},
                {"name": "p_eintrags_nr", "parameterType": {"type": "INT64"}, "value": eintrags_nr},
                {"name": "p_stichtag_str", "parameterType": {"type": "STRING"}, "value": invalid_stichtag},
                {"name": "p_wiederanlauf_wert", "parameterType": {"type": "STRING"}, "value": "0"}
            ])
        assert "Invalid date format for p_stichtag_str" in str(excinfo.value)

        # --- Assertions ---
        # 1. Check job log for failure and error message
        job_log_query = f"""
            SELECT status, error_message, key_date
            FROM `{project_id}.metrics.job_log`
            WHERE job_id = '{job_kennung}' AND entry_number = {eintrags_nr}
            ORDER BY start_timestamp DESC LIMIT 1
        """
        job_log_result = list(bq_client.query(job_log_query).result())
        assert len(job_log_result) == 1
        assert job_log_result[0].status == 'FAILED'
        assert "Invalid date format for p_stichtag_str" in job_log_result[0].error_message
        assert job_log_result[0].key_date is None # Key date should be NULL if parsing failed early

        # 2. Verify no data in target tables
        assert list(bq_client.query(f"SELECT COUNT(*) FROM `{project_id}.target.sof_ta_e_regulierer`").result())[0][0] == 0
        # ... (repeat for all other target tables)
    ```

---

### Test Case 7: Empty Source Tables (Edge Case)

*   **Purpose**: Verify that the job runs successfully and produces empty target tables when all source tables are empty, ensuring graceful handling of no input data.
*   **Setup**:
    1.  Ensure all `raw` BigQuery tables (`cds_ta_bp_ref`, `cds_ta_inv_definition`, etc.) are completely empty.
    2.  Set a valid `p_stichtag_str` (e.g., `'20230115'`).
*   **Action**:
    1.  Execute the `sp_ausd_adressen_main` stored procedure.
*   **Pass/Fail Criterion**:
    1.  The stored procedure completes successfully.
    2.  All target tables (`sof_ta_e_reach_*`, `sof_ta_e_business_*`, `sof_ta_e_regulierer`) are empty (contain 0 rows).
    3.  The `PROJECT_ID.metrics.job_log` table contains a 'SUCCESS' entry with `record_count = 0`.

    ```sql
    -- After ensuring all raw tables are empty and SP execution
    SELECT COUNT(*) FROM `PROJECT_ID.target.sof_ta_e_regulierer`;
    -- Expected: 0
    SELECT COUNT(*) FROM `PROJECT_ID.target.sof_ta_e_reach_gp`;
    -- Expected: 0
    -- ... (repeat for all target tables)

    SELECT status, record_count FROM `PROJECT_ID.metrics.job_log` WHERE job_id = 'K_AUSD_ADRESSEN_TEST_EMPTY' ORDER BY start_timestamp DESC LIMIT 1;
    -- Expected: status = 'SUCCESS', record_count = 0
    ```

---

### Test Case 8: Schema and Data Type Assertions (Data Quality)

*   **Purpose**: Verify that the schemas of all created tables (raw, staging, target, job_log) match the DDLs provided in the migration code, and that data types are correctly preserved or converted (e.g., Oracle `NUMBER` to BigQuery `INT64`, Oracle `DATE`/`TIMESTAMP` to BigQuery `TIMESTAMP`).
*   **Setup**:
    1.  Ensure all DDLs for `raw`, `staging`, `target`, and `metrics` datasets have been executed.
    2.  (Optional) Populate `raw` tables with data that covers various data type edge cases (e.g., max length strings, min/max `INT64` values, various timestamp formats, `NULL` values).
*   **Action**:
    1.  (No specific action for the job itself; this is a static check or run after a successful execution).
*   **Pass/Fail Criterion**:
    1.  Query BigQuery's `INFORMATION_SCHEMA.COLUMNS` view for each table and compare the actual `column_name`, `data_type`, and `is_nullable` properties against the expected DDLs.
    2.  Confirm that `TIMESTAMP` columns correctly store timestamp values, `INT64` for integers, `STRING` for strings, etc.
    3.  Verify that `NULL` values are handled correctly according to the schema (e.g., if a column was `NOT NULL` in Oracle and is intended to be `NOT NULL` in BigQuery, ensure no `NULL`s are inserted).

    ```sql
    -- Example SQL assertion for schema of a target table
    SELECT column_name, data_type, is_nullable
    FROM `PROJECT_ID.target.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'sof_ta_e_reach_gp'
    ORDER BY ordinal_position;
    /* Expected Output (compare against DDL):
    column_name           | data_type | is_nullable
    ----------------------|-----------|------------
    bp_id                 | INT64     | YES
    reachability_id       | INT64     | YES
    obj_version           | INT64     | YES
    country_code          | STRING    | YES
    ...
    land_sd               | STRING    | YES
    */

    -- Example SQL assertion for job_log table schema
    SELECT column_name, data_type, is_nullable
    FROM `PROJECT_ID.metrics.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'job_log'
    ORDER BY ordinal_position;
    /* Expected Output (compare against DDL):
    column_name           | data_type | is_nullable
    ----------------------|-----------|------------
    job_id                | STRING    | YES
    entry_number          | INT64     | YES
    key_date              | DATE      | YES
    restart_value         | STRING    | YES
    start_timestamp       | TIMESTAMP | YES
    end_timestamp         | TIMESTAMP | YES
    status                | STRING    | YES
    error_message         | STRING    | YES
    record_count          | INT64     | YES
    target_table          | STRING    | YES
    */
    ```

---

### Test Case 9: `isbert_schema.dwtk_meldungen` Discrepancy (External System Replacement / Transformation Correctness)

*   **Purpose**: Explicitly test the behavior regarding the `v_datum` derivation. The design document's "Original Flow" mentions retrieving `v_datum` from `isbert_schema.dwtk_meldungen`, but the "Transformation Logic" and generated BigQuery Stored Procedure use `p_stichtag_str` directly. This test confirms the migrated SP's behavior aligns with the `p_stichtag_str` input.
*   **Setup**:
    1.  **Populate `raw.dwtk_meldungen`**: Insert a record with a `timecreated` value (e.g., `TIMESTAMP('2023-01-01')`) that is *different* from the `p_stichtag_str` you will use.
    2.  **Populate `raw.cds_ta_bp_ref`**: Insert records that would be filtered differently if `v_datum` was '2023-01-01' (from `dwtk_meldungen`) vs. '2023-01-15' (from `p_stichtag_str`). For example:
        *   Record A: `insert_at = '2023-01-10'` (valid for `p_stichtag_str='20230115'`, but not if `v_datum='2023-01-01'`).
        *   Record B: `insert_at = '2022-12-25'` (valid for both).
    3.  Set `p_stichtag_str` to `'20230115'`.
*   **Action**:
    1.  Execute the `sp_ausd_adressen_main` stored procedure.
*   **Pass/Fail Criterion**:
    1.  The filtering logic in `PROJECT_ID.staging.sof_ta_bp_ref_gp` (and other tables using `v_datum_date`) uses `'2023-01-15'` as the effective date, *not* `'2023-01-01'` from `dwtk_meldungen`.
    2.  Record A (with `insert_at = '2023-01-10'`) is included in the output (assuming other filters pass).
    3.  The `key_date` in the `PROJECT_ID.metrics.job_log` entry is `DATE('2023-01-15')`.

    ```sql
    -- Example SQL for setting up raw tables
    INSERT INTO `PROJECT_ID.raw.dwtk_meldungen` (timecreated, job_kennung) VALUES
    (TIMESTAMP('2023-01-01'), 'LEGACY_JOB_ID'); -- This should be ignored by the migrated SP

    INSERT INTO `PROJECT_ID.raw.cds_ta_bp_ref` (bp_id, reachability_id, insert_at, modified_at, valid_from, valid_to, is_production, bp_ref_ty, address_ref_ty, cntrct_cp2_id, inv_def_invrec_id, bpr_inst_evnrec_id, bpr_inst_srvusr_id, inv_def_mopref_id, mop_bp_id, means_of_payment_id, mop_ref_ty) VALUES
    (1, 101, TIMESTAMP('2023-01-10'), NULL, TIMESTAMP('2022-01-01'), NULL, 1, 4, 6, 1,1,1,1,1,1,1,1), -- Record A
    (2, 102, TIMESTAMP('2022-12-25'), NULL, TIMESTAMP('2022-01-01'), NULL, 1, 4, 6, 1,1,1,1,1,1,1,1); -- Record B

    -- After SP execution with p_stichtag_str = '20230115'
    SELECT bp_id FROM `PROJECT_ID.staging.sof_ta_bp_ref_gp` ORDER BY bp_id;
    -- Expected result:
    -- bp_id
    -- -----
    -- 1
    -- 2
    -- (Both records should be present, confirming '20230115' was used for filtering)

    SELECT key_date FROM `PROJECT_ID.metrics.job_log` WHERE job_id = 'K_AUSD_ADRESSEN_TEST_DISCREPANCY' ORDER BY start_timestamp DESC LIMIT 1;
    -- Expected: DATE('2023-01-15')
    ```