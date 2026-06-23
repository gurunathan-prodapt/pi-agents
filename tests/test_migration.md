As a senior data-migration QA engineer, I've designed a comprehensive suite of validation tests for the migration of `r_ausd_austausch.ksh` to Google BigQuery. These tests aim to ensure behavioral equivalence across output parity, transformation correctness, external system replacements, and data quality.

---

## Migration Validation Tests: `r_ausd_austausch.ksh` to BigQuery

**General Setup for all Tests:**

*   **Source Data:** A "golden set" of source data, representing a snapshot of the Oracle `sof$ta_p_...` tables, is loaded into BigQuery tables within `project.source_dataset`. This golden set should include typical data, boundary conditions, NULL values, and specific data points designed to trigger various branches of the complex `CASE` and `DECODE` logic.
*   **Legacy Output:** For output parity tests, the legacy `r_ausd_austausch.ksh` job is executed against the same "golden set" of source data in the Oracle environment. The resulting `rpt$...` tables are then extracted and loaded into `project.golden_dataset` in BigQuery (e.g., `project.golden_dataset.legacy_rpt_ta_s_d1_rech_empf`) for direct comparison.
*   **Target State:** Before each test run, the target `project.reporting_dataset.rpt_ta_s_d1_...` tables and the `project.admin_dataset.job_control`, `job_log` tables are cleared or reset to a known baseline state.
*   **BigQuery Stored Procedures:** The migrated BigQuery stored procedures (`BERT_AUSTAUSCH_KSH_SP`, `D_AUSD_AUSTAUSCH_SP`) are deployed in `project.orchestration_dataset`.

---

### Test Case 1: Full Run - Output Parity (Happy Path)

**Purpose:**
To validate that the migrated BigQuery job, when executed for a full refresh (equivalent to `p_wiederanlaufWert=0`), produces identical data in all target `rpt$...` tables as the legacy Oracle job. This is the primary end-to-end behavioral equivalence test.

**Setup:**
1.  Load a comprehensive "golden set" of source data into `project.source_dataset.sof_ta_p_...` tables.
2.  Run the legacy `r_ausd_austausch.ksh` job with `p_wiederanlaufWert=0` and a specific `p_stichtag` (e.g., `20231026`) against the Oracle source data.
3.  Extract the resulting `rpt$...` tables from Oracle and load them into `project.golden_dataset.legacy_rpt_ta_s_d1_...` tables in BigQuery.
4.  Ensure `project.reporting_dataset.rpt_ta_s_d1_...` tables are empty.
5.  Ensure `project.admin_dataset.job_control` and `job_log` tables are empty.

**Action:**
Execute the main BigQuery orchestration stored procedure:
```sql
CALL project.orchestration_dataset.BERT_AUSTAUSCH_KSH_SP(p_stichtag => '2023-10-26', p_wiederanlaufWert => 0);
```

**Pass/Fail Criterion:**
1.  The `job_control` table must contain an entry for this run with `status = 'SUCCEEDED'`.
2.  For each target table (`rpt_ta_s_d1_rech_empf`, `rpt_ta_s_d1_vertrag`, `rpt_ta_s_d1_rech_kunde`, `rpt_ta_s_d1_discount`, `rpt_ta_s_d1_discount_rr`, `rpt_ta_s_d1_vpn`), the data in `project.reporting_dataset` must be identical to the corresponding `project.golden_dataset.legacy_...` table. This includes row counts, column values, and data types.

**Test Code (SQL Assertion):**
```sql
-- Example for rpt_ta_s_d1_rech_empf. Repeat for all rpt$... tables.
SELECT 'Only in BigQuery' AS source, * FROM `project.reporting_dataset.rpt_ta_s_d1_rech_empf`
EXCEPT DISTINCT
SELECT 'Only in Legacy' AS source, * FROM `project.golden_dataset.legacy_rpt_ta_s_d1_rech_empf`

UNION ALL

SELECT 'Only in Legacy' AS source, * FROM `project.golden_dataset.legacy_rpt_ta_s_d1_rech_empf`
EXCEPT DISTINCT
SELECT 'Only in BigQuery' AS source, * FROM `project.reporting_dataset.rpt_ta_s_d1_rech_empf`;

-- Pass if the above query returns 0 rows.
```

---

### Test Case 2: Incremental Run - Output Parity (`p_wiederanlaufWert` Logic)

**Purpose:**
To validate the correct implementation of the `p_wiederanlaufWert` logic, ensuring that only records with `VERTRAG_ID_CARMEN > p_wiederanlaufWert` are processed (inserted/updated) and existing records with `VERTRAG_ID_CARMEN <= p_wiederanlaufWert` are retained or handled as per legacy behavior.

**Setup:**
1.  Load an initial "golden set" of source data into `project.source_dataset.sof_ta_p_...` tables.
2.  Run the legacy `r_ausd_austausch.ksh` job with `p_wiederanlaufWert=0` and a specific `p_stichtag`. Capture the output into `project.golden_dataset.legacy_initial_rpt_...` tables.
3.  Modify the source data:
    *   Add new records to `sof_ta_p_vertrag` with `VERTRAG_ID_CARMEN` values greater than a chosen `p_wiederanlaufWert` (e.g., 1000).
    *   Update some existing records in `sof_ta_p_vertrag` with `VERTRAG_ID_CARMEN` values greater than 1000.
    *   Ensure some records with `VERTRAG_ID_CARMEN <= 1000` remain unchanged.
4.  Run the legacy `r_ausd_austausch.ksh` job with the modified source data, `p_stichtag`, and `p_wiederanlaufWert=1000`. Capture the output into `project.golden_dataset.legacy_incremental_rpt_...` tables.
5.  Pre-populate `project.reporting_dataset.rpt_ta_s_d1_...` tables with the data from `project.golden_dataset.legacy_initial_rpt_...`.
6.  Load the modified source data into `project.source_dataset.sof_ta_p_...`.
7.  Ensure `project.admin_dataset.job_control` and `job_log` tables are empty.

**Action:**
Execute the BigQuery job with the incremental parameter:
```sql
CALL project.orchestration_dataset.BERT_AUSTAUSCH_KSH_SP(p_stichtag => '2023-10-26', p_wiederanlaufWert => 1000);
```

**Pass/Fail Criterion:**
1.  The `job_control` table must contain an entry for this run with `status = 'SUCCEEDED'`.
2.  For each target table, the data in `project.reporting_dataset` must be identical to the corresponding `project.golden_dataset.legacy_incremental_rpt_...` table. This validates that the `MERGE` or `DELETE/INSERT` logic correctly applies the `p_wiederanlaufWert` filter.

**Test Code (Pytest with SQL comparison):**
```python
import pytest
from google.cloud import bigquery

def test_incremental_run_output_parity(bigquery_client):
    stichtag = '2023-10-26'
    wiederanlaufwert = 1000
    
    # Action: Execute the migrated job
    query = f"""
    CALL project.orchestration_dataset.BERT_AUSTAUSCH_KSH_SP(
        p_stichtag => '{stichtag}', 
        p_wiederanlaufWert => {wiederanlaufwert}
    );
    """
    bigquery_client.query(query).result()

    # Pass/Fail: Compare target tables
    target_tables = [
        "rpt_ta_s_d1_rech_empf", "rpt_ta_s_d1_vertrag", "rpt_ta_s_d1_rech_kunde",
        "rpt_ta_s_d1_discount", "rpt_ta_s_d1_discount_rr", "rpt_ta_s_d1_vpn"
    ]
    
    for table_name in target_tables:
        bq_table = f"`project.reporting_dataset.{table_name}`"
        legacy_table = f"`project.golden_dataset.legacy_incremental_{table_name}`"
        
        comparison_query = f"""
        SELECT 'Only in BigQuery' AS source, * FROM {bq_table}
        EXCEPT DISTINCT
        SELECT 'Only in Legacy' AS source, * FROM {legacy_table}

        UNION ALL

        SELECT 'Only in Legacy' AS source, * FROM {legacy_table}
        EXCEPT DISTINCT
        SELECT 'Only in BigQuery' AS source, * FROM {bq_table};
        """
        
        diff_results = bigquery_client.query(comparison_query).result()
        assert diff_results.total_rows == 0, \
            f"Table {table_name} does not match legacy output after incremental run. Differences found."

    # Verify job control status
    job_control_query = f"""
    SELECT status FROM `project.admin_dataset.job_control`
    WHERE stichtag = '{stichtag}' AND wiederanlaufwert = {wiederanlaufwert}
    ORDER BY start_time DESC LIMIT 1;
    """
    job_status = bigquery_client.query(job_control_query).result().to_dataframe()['status'].iloc[0]
    assert job_status == 'SUCCEEDED', "Job control status is not 'SUCCEEDED' for incremental run."
```

---

### Test Case 3: Default Stichtag Handling

**Purpose:**
To verify that when the `p_stichtag` parameter is not explicitly provided, the BigQuery job correctly determines the reference date using BigQuery's native date functions, mimicking the `gestern.ksh` and `r_ausd_austausch.ksh` logic (e.g., `MIN(sysdate, maxladedatum)` or simply `sysdate`).

**Setup:**
1.  Load a "golden set" of source data into `project.source_dataset.sof_ta_p_...` tables.
2.  Run the legacy `r_ausd_austausch.ksh` job *without* the `-s` parameter. Capture the resulting `rpt$...` tables into `project.golden_dataset.legacy_default_date_rpt_...`. Note the `sysdate` used by the legacy job.
3.  Ensure `project.reporting_dataset.rpt_ta_s_d1_...` tables are empty.
4.  Ensure `project.admin_dataset.job_control` and `job_log` tables are empty.

**Action:**
Execute the BigQuery job without providing `p_stichtag` (passing `NULL`):
```sql
CALL project.orchestration_dataset.BERT_AUSTAUSCH_KSH_SP(p_stichtag => NULL, p_wiederanlaufWert => 0);
```

**Pass/Fail Criterion:**
1.  The `job_control` table must contain an entry for this run with `status = 'SUCCEEDED'`.
2.  The `stichtag` recorded in `job_control` must match the date determined by the legacy job (or `CURRENT_DATE()` if `maxladedatum` logic was simplified).
3.  All `rpt$...` tables in `project.reporting_dataset` must be identical to `project.golden_dataset.legacy_default_date_rpt_...`.

**Test Code (SQL Assertion):**
```sql
-- Verify recorded stichtag
SELECT stichtag FROM `project.admin_dataset.job_control`
WHERE wiederanlaufwert = 0 AND status = 'SUCCEEDED'
ORDER BY start_time DESC LIMIT 1;
-- Pass if this date matches the expected default date (e.g., CURRENT_DATE() on the day of execution).

-- Compare target tables (similar to Test Case 1)
-- ... (repeat comparison queries for all rpt$... tables)
```

---

### Test Case 4: Oracle `DECODE` to BigQuery `CASE` Translation

**Purpose:**
To validate the correct translation of Oracle `DECODE` functions to BigQuery `CASE` statements, ensuring equivalent conditional logic and output for all possible input values.

**Setup:**
1.  Load source data into `project.source_dataset.sof_ta_p_...` that specifically includes values designed to exercise every branch of `DECODE` statements found in `d_ausd_austausch.sql`. For example, `VO_KENN` in `sof_ta_p_vertrag` is often used with `DECODE`.
2.  Run the legacy job with this specific source data and capture the output into `project.golden_dataset.legacy_decode_test_rpt_...`.
3.  Ensure `project.reporting_dataset.rpt_ta_s_d1_...` tables are empty.

**Action:**
Execute the BigQuery job:
```sql
CALL project.orchestration_dataset.BERT_AUSTAUSCH_KSH_SP(p_stichtag => '2023-10-26', p_wiederanlaufWert => 0);
```

**Pass/Fail Criterion:**
1.  The `job_control` table must show `status = 'SUCCEEDED'`.
2.  Specific columns in `rpt_ta_s_d1_vertrag` (or other tables where `DECODE` was used, e.g., `VO_KENN`) that are derived from `DECODE` logic must match the corresponding columns in `project.golden_dataset.legacy_decode_test_rpt_...` for all tested input values.

**Test Code (Pytest with targeted column comparison):**
```python
import pytest
from google.cloud import bigquery

def test_decode_translation(bigquery_client):
    stichtag = '2023-10-26'
    
    # Action: Execute the migrated job
    query = f"""
    CALL project.orchestration_dataset.BERT_AUSTAUSCH_KSH_SP(
        p_stichtag => '{stichtag}', 
        p_wiederanlaufWert => 0
    );
    """
    bigquery_client.query(query).result()

    # Pass/Fail: Compare specific columns affected by DECODE
    # Example: VO_KENN in rpt_ta_s_d1_vertrag
    comparison_query = f"""
    SELECT t1.VO_KENN, t2.VO_KENN
    FROM `project.reporting_dataset.rpt_ta_s_d1_vertrag` t1
    JOIN `project.golden_dataset.legacy_decode_test_rpt_ta_s_d1_vertrag` t2
      ON t1.VERTRAG_ID_CARMEN = t2.VERTRAG_ID_CARMEN -- Assuming this is a unique key
    WHERE t1.VO_KENN IS DISTINCT FROM t2.VO_KENN;
    """
    
    diff_results = bigquery_client.query(comparison_query).result()
    assert diff_results.total_rows == 0, \
        "VO_KENN column in rpt_ta_s_d1_vertrag does not match legacy output (DECODE translation issue)."
    
    # Add similar checks for other columns affected by DECODE
```

---

### Test Case 5: Oracle `NVL` to BigQuery `IFNULL`/`COALESCE` Translation

**Purpose:**
To validate the correct translation of Oracle `NVL` functions to BigQuery `IFNULL` or `COALESCE`, ensuring identical NULL handling and default value assignment.

**Setup:**
1.  Load source data into `project.source_dataset.sof_ta_p_...` with explicit `NULL` values in columns where `NVL` was used in the legacy SQL (e.g., `organisationseinheit` in `sof_ta_p_rech_empf`). Also include non-NULL values to ensure `NVL` doesn't incorrectly alter them.
2.  Run the legacy job with this specific source data and capture the output into `project.golden_dataset.legacy_nvl_test_rpt_...`.
3.  Ensure `project.reporting_dataset.rpt_ta_s_d1_...` tables are empty.

**Action:**
Execute the BigQuery job:
```sql
CALL project.orchestration_dataset.BERT_AUSTAUSCH_KSH_SP(p_stichtag => '2023-10-26', p_wiederanlaufWert => 0);
```

**Pass/Fail Criterion:**
1.  The `job_control` table must show `status = 'SUCCEEDED'`.
2.  Specific columns in the target tables (e.g., `ORGANISATIONSEINHEIT` in `rpt_ta_s_d1_rech_empf`) that were affected by `NVL` in the legacy SQL must have identical values (including non-NULL defaults or original non-NULL values) in the BigQuery output compared to `project.golden_dataset.legacy_nvl_test_rpt_...`.

**Test Code (SQL Assertion):**
```sql
-- Example for ORGANISATIONSEINHEIT in rpt_ta_s_d1_rech_empf
SELECT t1.ORGANISATIONSEINHEIT, t2.ORGANISATIONSEINHEIT
FROM `project.reporting_dataset.rpt_ta_s_d1_rech_empf` t1
JOIN `project.golden_dataset.legacy_nvl_test_rpt_ta_s_d1_rech_empf` t2
  ON t1.DWH_KONTO_ID = t2.DWH_KONTO_ID -- Assuming a unique key
WHERE t1.ORGANISATIONSEINHEIT IS DISTINCT FROM t2.ORGANISATIONSEINHEIT;
-- Pass if the above query returns 0 rows.
```

---

### Test Case 6: Oracle `(+)` Outer Join to BigQuery `LEFT JOIN` Translation

**Purpose:**
To validate that all Oracle `(+)` outer join syntax has been correctly translated to BigQuery `LEFT JOIN`s, preserving the outer join behavior, especially when the right side of the join has no matching records.

**Setup:**
1.  Load source data into `project.source_dataset.sof_ta_p_...` where some join conditions will *not* be met on the right side of an outer join (e.g., a `sof_ta_p_vertrag` record with no corresponding `sof_ta_p_dn_nutzer` entry if `dn_nutzer` was outer joined).
2.  Run the legacy job with this specific source data and capture the output into `project.golden_dataset.legacy_outer_join_test_rpt_...`.
3.  Ensure `project.reporting_dataset.rpt_ta_s_d1_...` tables are empty.

**Action:**
Execute the BigQuery job:
```sql
CALL project.orchestration_dataset.BERT_AUSTAUSCH_KSH_SP(p_stichtag => '2023-10-26', p_wiederanlaufWert => 0);
```

**Pass/Fail Criterion:**
1.  The `job_control` table must show `status = 'SUCCEEDED'`.
2.  Columns derived from the right side of `LEFT JOIN`s (originally `(+)`) must correctly contain `NULL`s where no match was found, matching the legacy output. This can be verified by comparing the full target tables.

**Test Code (SQL Assertion):**
```sql
-- Compare the full rpt_ta_s_d1_vertrag table (as it has many joins)
SELECT 'Only in BigQuery' AS source, * FROM `project.reporting_dataset.rpt_ta_s_d1_vertrag`
EXCEPT DISTINCT
SELECT 'Only in Legacy' AS source, * FROM `project.golden_dataset.legacy_outer_join_test_rpt_ta_s_d1_vertrag`

UNION ALL

SELECT 'Only in Legacy' AS source, * FROM `project.golden_dataset.legacy_outer_join_test_rpt_ta_s_d1_vertrag`
EXCEPT DISTINCT
SELECT 'Only in BigQuery' AS source, * FROM `project.reporting_dataset.rpt_ta_s_d1_vertrag`;
-- Pass if the above query returns 0 rows.
```

---

### Test Case 7: Complex `CASE` Logic (e.g., MultiSIM)

**Purpose:**
To validate the translation of complex `CASE` statements, such as those determining MultiSIM status or other intricate business rules, ensuring all branches and conditions are correctly handled.

**Setup:**
1.  Load source data into `project.source_dataset.sof_ta_p_basisprod` that specifically triggers various combinations of `msX_stat`, `msX_iccid`, `msX_e_id`, etc., to test all possible outcomes of the MultiSIM logic.
2.  Run the legacy job with this specific source data and capture the output into `project.golden_dataset.legacy_multisim_test_rpt_...`.
3.  Ensure `project.reporting_dataset.rpt_ta_s_d1_...` tables are empty.

**Action:**
Execute the BigQuery job:
```sql
CALL project.orchestration_dataset.BERT_AUSTAUSCH_KSH_SP(p_stichtag => '2023-10-26', p_wiederanlaufWert => 0);
```

**Pass/Fail Criterion:**
1.  The `job_control` table must show `status = 'SUCCEEDED'`.
2.  The derived columns in `rpt_ta_s_d1_vertrag` (e.g., `MSISDN`, `TWINCARD`, `ICCID`, `E_ID`, `CARD_TYPE_NAME`, `HLR` and their `LINK_` and `MSX_` variants) that depend on this complex `CASE` logic must match the legacy output for all tested scenarios.

**Test Code (Pytest with targeted column comparison):**
```python
import pytest
from google.cloud import bigquery

def test_complex_case_multisim_logic(bigquery_client):
    stichtag = '2023-10-26'
    
    # Action: Execute the migrated job
    query = f"""
    CALL project.orchestration_dataset.BERT_AUSTAUSCH_KSH_SP(
        p_stichtag => '{stichtag}', 
        p_wiederanlaufWert => 0
    );
    """
    bigquery_client.query(query).result()

    # Pass/Fail: Compare specific columns affected by complex MultiSIM CASE logic
    # This is a simplified example; a full test would compare all relevant columns.
    comparison_query = f"""
    SELECT t1.MSISDN, t2.MSISDN, t1.TWINCARD, t2.TWINCARD, t1.ICCID, t2.ICCID
    FROM `project.reporting_dataset.rpt_ta_s_d1_vertrag` t1
    JOIN `project.golden_dataset.legacy_multisim_test_rpt_ta_s_d1_vertrag` t2
      ON t1.VERTRAG_ID_CARMEN = t2.VERTRAG_ID_CARMEN
    WHERE t1.MSISDN IS DISTINCT FROM t2.MSISDN
       OR t1.TWINCARD IS DISTINCT FROM t2.TWINCARD
       OR t1.ICCID IS DISTINCT FROM t2.ICCID;
    """
    
    diff_results = bigquery_client.query(comparison_query).result()
    assert diff_results.total_rows == 0, \
        "MultiSIM related columns in rpt_ta_s_d1_vertrag do not match legacy output (Complex CASE logic issue)."
```

---

### Test Case 8: Temporary Tables/CTEs (`sof_ta_rechdef`, `sof_ta_kd_kto`)

**Purpose:**
To verify that the logic for populating and using temporary tables (now implemented as CTEs or temporary tables in BigQuery) is correctly translated and produces intermediate results that lead to identical final output.

**Setup:**
1.  Load source data into `project.source_dataset.sof_ta_p_...` that would produce specific intermediate results in `sof$ta_rechdef` and `sof$ta_kd_kto` in the legacy system.
2.  Run the legacy job with this specific source data and capture the output for `rpt_ta_s_d1_rech_kunde` into `project.golden_dataset.legacy_temp_table_test_rpt_ta_s_d1_rech_kunde`.
3.  Ensure `project.reporting_dataset.rpt_ta_s_d1_rech_kunde` is empty.

**Action:**
Execute the BigQuery job:
```sql
CALL project.orchestration_dataset.BERT_AUSTAUSCH_KSH_SP(p_stichtag => '2023-10-26', p_wiederanlaufWert => 0);
```

**Pass/Fail Criterion:**
1.  The `job_control` table must show `status = 'SUCCEEDED'`.
2.  The final `rpt_ta_s_d1_rech_kunde` table (which depends on these CTEs) must be identical to `project.golden_dataset.legacy_temp_table_test_rpt_ta_s_d1_rech_kunde`. This indirectly validates the correctness of the CTE logic.

**Test Code (SQL Assertion):**
```sql
-- Compare the rpt_ta_s_d1_rech_kunde table
SELECT 'Only in BigQuery' AS source, * FROM `project.reporting_dataset.rpt_ta_s_d1_rech_kunde`
EXCEPT DISTINCT
SELECT 'Only in Legacy' AS source, * FROM `project.golden_dataset.legacy_temp_table_test_rpt_ta_s_d1_rech_kunde`

UNION ALL

SELECT 'Only in Legacy' AS source, * FROM `project.golden_dataset.legacy_temp_table_test_rpt_ta_s_d1_rech_kunde`
EXCEPT DISTINCT
SELECT 'Only in BigQuery' AS source, * FROM `project.reporting_dataset.rpt_ta_s_d1_rech_kunde`;
-- Pass if the above query returns 0 rows.
```

---

### Test Case 9: Schema and Data Type Validation

**Purpose:**
To verify that the schema of the target BigQuery tables (`rpt$...`) matches the expected schema (column names, data types, nullability) and that no data truncation or unexpected type conversions occur during the migration.

**Setup:**
1.  Load source data with maximum length strings, boundary numbers (min/max for INT64), and various date formats into `project.source_dataset.sof_ta_p_...`.
2.  Define the expected BigQuery DDL for all `rpt$...` tables.
3.  Ensure `project.reporting_dataset.rpt_ta_s_d1_...` tables are empty.

**Action:**
Execute the BigQuery job:
```sql
CALL project.orchestration_dataset.BERT_AUSTAUSCH_KSH_SP(p_stichtag => '2023-10-26', p_wiederanlaufWert => 0);
```

**Pass/Fail Criterion:**
1.  The `job_control` table must show `status = 'SUCCEEDED'`.
2.  For each `rpt$...` table, its BigQuery schema (obtained via `INFORMATION_SCHEMA.COLUMNS`) must match the expected DDL.
3.  No data truncation should be observed in string columns (e.g., `STRASSE` in `rpt_ta_s_d1_rech_empf`).
4.  All data types should be correctly mapped (e.g., Oracle `NUMBER` to BigQuery `INT64`/`BIGNUMERIC`, `DATE` to `DATE`).
5.  No unexpected `NULL` values should appear in columns defined as `NOT NULL` in the target DDL.

**Test Code (Pytest for schema validation):**
```python
import pytest
from google.cloud import bigquery

def test_schema_and_data_type_validation(bigquery_client):
    stichtag = '2023-10-26'
    
    # Action: Execute the migrated job
    query = f"""
    CALL project.orchestration_dataset.BERT_AUSTAUSCH_KSH_SP(
        p_stichtag => '{stichtag}', 
        p_wiederanlaufWert => 0
    );
    """
    bigquery_client.query(query).result()

    expected_schemas = {
        "rpt_ta_s_d1_rech_empf": [
            {"name": "DWH_KONTO_ID", "field_type": "STRING", "mode": "NULLABLE"},
            {"name": "RECHDEF_ID_CARMEN", "field_type": "STRING", "mode": "NULLABLE"},
            # ... define full expected schema for each table
        ],
        # ... define schemas for all other rpt$... tables
    }

    for table_name, expected_schema in expected_schemas.items():
        table_id = f"project.reporting_dataset.{table_name}"
        table = bigquery_client.get_table(table_id)
        
        actual_schema = [{"name": field.name, "field_type": field.field_type, "mode": field.mode} 
                         for field in table.schema]
        
        # Basic check for column count and names
        assert len(actual_schema) == len(expected_schema), \
            f"Schema mismatch for {table_name}: column count differs."
        
        # Detailed field-by-field comparison
        for expected_field in expected_schema:
            matching_field = next((f for f in actual_schema if f["name"] == expected_field["name"]), None)
            assert matching_field is not None, \
                f"Schema mismatch for {table_name}: Column {expected_field['name']} not found."
            assert matching_field["field_type"] == expected_field["field_type"], \
                f"Schema mismatch for {table_name}: Column {expected_field['name']} type mismatch. Expected {expected_field['field_type']}, got {matching_field['field_type']}."
            assert matching_field["mode"] == expected_field["mode"], \
                f"Schema mismatch for {table_name}: Column {expected_field['name']} nullability mismatch. Expected {expected_field['mode']}, got {matching_field['mode']}."

    # Additional check for data truncation (example for a string column)
    # This requires specific test data with max length strings
    truncation_check_query = """
    SELECT COUNT(*) FROM `project.reporting_dataset.rpt_ta_s_d1_rech_empf`
    WHERE LENGTH(STRASSE) > 45; -- Assuming STRASSE has a max length of 45
    """
    truncation_count = bigquery_client.query(truncation_check_query).result().to_dataframe().iloc[0,0]
    assert truncation_count == 0, "Data truncation detected in STRASSE column."
```

---

### Test Case 10: Row Count Validation

**Purpose:**
To verify that the row counts of all target tables in BigQuery exactly match the row counts of the corresponding legacy tables after a full run.

**Setup:**
1.  Load a representative "golden set" of source data.
2.  Run the legacy job and record the exact row counts for all `rpt$...` tables. Store these as `expected_row_counts`.
3.  Ensure `project.reporting_dataset.rpt_ta_s_d1_...` tables are empty.

**Action:**
Execute the BigQuery job:
```sql
CALL project.orchestration_dataset.BERT_AUSTAUSCH_KSH_SP(p_stichtag => '2023-10-26', p_wiederanlaufWert => 0);
```

**Pass/Fail Criterion:**
1.  The `job_control` table must show `status = 'SUCCEEDED'`.
2.  For each `rpt$...` table, its row count in `project.reporting_dataset` must exactly match the `expected_row_counts` recorded from the legacy system.

**Test Code (SQL Assertion):**
```sql
-- Example for rpt_ta_s_d1_rech_empf. Repeat for all rpt$... tables.
SELECT COUNT(*) FROM `project.reporting_dataset.rpt_ta_s_d1_rech_empf`;
-- Pass if this count matches the expected legacy row count.
```

---

### Test Case 11: Error Handling and Logging (Invalid Parameters)

**Purpose:**
To verify that the orchestration procedure correctly handles invalid input parameters (e.g., malformed dates), logs the error, and sets the job status to 'FAILED'.

**Setup:**
1.  Ensure `project.admin_dataset.job_control` and `job_log` tables are empty.

**Action:**
Execute the BigQuery job with an invalid `p_stichtag`:
```sql
-- This call is expected to fail.
CALL project.orchestration_dataset.BERT_AUSTAUSCH_KSH_SP(p_stichtag => 'INVALID_DATE_FORMAT', p_wiederanlaufWert => 0);
```

**Pass/Fail Criterion:**
1.  The stored procedure call must fail or raise an error indicating invalid parameters.
2.  The `job_control` table must contain an entry for this run with `status = 'FAILED'`.
3.  The `job_log` table must contain at least one entry with `level = 'ERROR'` detailing the parameter validation failure.

**Test Code (Pytest for error handling):**
```python
import pytest
from google.cloud import bigquery

def test_invalid_parameter_error_handling(bigquery_client):
    invalid_stichtag = 'INVALID_DATE_FORMAT'
    wiederanlaufwert = 0
    
    # Action: Execute the migrated job with invalid parameters
    query = f"""
    CALL project.orchestration_dataset.BERT_AUSTAUSCH_KSH_SP(
        p_stichtag => '{invalid_stichtag}', 
        p_wiederanlaufWert => {wiederanlaufwert}
    );
    """
    
    # Expect the call to raise an exception
    with pytest.raises(bigquery.exceptions.GoogleAPIError) as excinfo:
        bigquery_client.query(query).result()
    
    assert "Invalid date format" in str(excinfo.value) or "Failed to parse" in str(excinfo.value), \
        "Expected BigQuery error for invalid date format not found."

    # Pass/Fail: Verify job control and log tables
    job_control_query = f"""
    SELECT status, message FROM `project.admin_dataset.job_control`
    WHERE wiederanlaufwert = {wiederanlaufwert}
    ORDER BY start_time DESC LIMIT 1;
    """
    job_control_df = bigquery_client.query(job_control_query).result().to_dataframe()
    assert not job_control_df.empty, "No entry found in job_control table."
    assert job_control_df['status'].iloc[0] == 'FAILED', "Job control status is not 'FAILED'."
    assert "Invalid date format" in job_control_df['message'].iloc[0], \
        "Job control message does not indicate parameter error."

    job_log_query = f"""
    SELECT level, message FROM `project.admin_dataset.job_log`
    WHERE level = 'ERROR'
    ORDER BY log_time DESC LIMIT 1;
    """
    job_log_df = bigquery_client.query(job_log_query).result().to_dataframe()
    assert not job_log_df.empty, "No ERROR entry found in job_log table."
    assert "Invalid date format" in job_log_df['message'].iloc[0], \
        "Job log message does not indicate parameter error."
```

---

### Test Case 12: Error Handling and Logging (Data Transformation Failure)

**Purpose:**
To verify that if an error occurs during the data transformation phase (e.g., a SQL error within `D_AUSD_AUSTAUSCH_SP`), the orchestration procedure catches it, logs it, and marks the overall job as 'FAILED'.

**Setup:**
1.  Load source data that will cause a known SQL error in `D_AUSD_AUSTAUSCH_SP` (e.g., division by zero if such an operation exists, or a data type mismatch if not properly handled). This might require temporarily introducing a fault into `D_AUSD_AUSTAUSCH_SP` for testing purposes.
2.  Ensure `project.admin_dataset.job_control` and `job_log` tables are empty.

**Action:**
Execute the BigQuery job with the fault-inducing data:
```sql
-- This call is expected to fail due to an internal transformation error.
CALL project.orchestration_dataset.BERT_AUSTAUSCH_KSH_SP(p_stichtag => '2023-10-26', p_wiederanlaufWert => 0);
```

**Pass/Fail Criterion:**
1.  The stored procedure call must fail or raise an error.
2.  The `job_control` table must contain an entry for this run with `status = 'FAILED'`.
3.  The `job_log` table must contain at least one entry with `level = 'ERROR'` detailing the transformation failure.

**Test Code (Pytest for error handling):**
```python
import pytest
from google.cloud import bigquery

def test_transformation_failure_error_handling(bigquery_client):
    stichtag = '2023-10-26'
    wiederanlaufwert = 0
    
    # Assume source data is loaded to trigger a specific SQL error in D_AUSD_AUSTAUSCH_SP
    # (e.g., a division by zero if a column is 0 where it shouldn't be)
    
    # Action: Execute the migrated job
    query = f"""
    CALL project.orchestration_dataset.BERT_AUSTAUSCH_KSH_SP(
        p_stichtag => '{stichtag}', 
        p_wiederanlaufWert => {wiederanlaufwert}
    );
    """
    
    # Expect the call to raise an exception
    with pytest.raises(bigquery.exceptions.GoogleAPIError) as excinfo:
        bigquery_client.query(query).result()
    
    assert "division by zero" in str(excinfo.value) or "SQL error" in str(excinfo.value), \
        "Expected BigQuery error for transformation failure not found."

    # Pass/Fail: Verify job control and log tables
    job_control_query = f"""
    SELECT status, message FROM `project.admin_dataset.job_control`
    WHERE stichtag = '{stichtag}' AND wiederanlaufwert = {wiederanlaufwert}
    ORDER BY start_time DESC LIMIT 1;
    """
    job_control_df = bigquery_client.query(job_control_query).result().to_dataframe()
    assert not job_control_df.empty, "No entry found in job_control table."
    assert job_control_df['status'].iloc[0] == 'FAILED', "Job control status is not 'FAILED'."
    assert "Transformation failed" in job_control_df['message'].iloc[0], \
        "Job control message does not indicate transformation error."

    job_log_query = f"""
    SELECT level, message FROM `project.admin_dataset.job_log`
    WHERE level = 'ERROR'
    ORDER BY log_time DESC LIMIT 1;
    """
    job_log_df = bigquery_client.query(job_log_query).result().to_dataframe()
    assert not job_log_df.empty, "No ERROR entry found in job_log table."
    assert "Error during D_AUSD_AUSTAUSCH_SP execution" in job_log_df['message'].iloc[0], \
        "Job log message does not indicate transformation error."
```

---

### Test Case 13: Empty Source Tables

**Purpose:**
To verify that the job gracefully handles scenarios where one or more source tables are completely empty, producing correct (e.g., empty or partially populated) target tables based on the join logic.

**Setup:**
1.  Load source data where one or more `project.source_dataset.sof_ta_p_...` tables are intentionally left empty (e.g., `sof_ta_p_rech_empf` is empty, but others are populated).
2.  Run the legacy job with this setup and capture the output into `project.golden_dataset.legacy_empty_source_rpt_...`.
3.  Ensure `project.reporting_dataset.rpt_ta_s_d1_...` tables are empty.

**Action:**
Execute the BigQuery job:
```sql
CALL project.orchestration_dataset.BERT_AUSTAUSCH_KSH_SP(p_stichtag => '2023-10-26', p_wiederanlaufWert => 0);
```

**Pass/Fail Criterion:**
1.  The `job_control` table must show `status = 'SUCCEEDED'`.
2.  The resulting `rpt$...` tables in `project.reporting_dataset` must have row counts and data consistent with the legacy job's behavior when faced with empty source tables. For tables that are direct copies of an empty source, they should be empty. For tables with outer joins to an empty source, they should contain `NULL`s for the joined columns.

**Test Code (SQL Assertion):**
```sql
-- Example: If sof_ta_p_rech_empf is empty, rpt_ta_s_d1_rech_empf should also be empty.
SELECT COUNT(*) FROM `project.reporting_dataset.rpt_ta_s_d1_rech_empf`;
-- Pass if this count is 0.

-- Compare all target tables against the legacy output for this scenario
-- ... (repeat comparison queries for all rpt$... tables as in Test Case 1)
```

---

### Test Case 14: `TRUNCATE REUSE STORAGE` / `MERGE` vs. `CREATE OR REPLACE` Equivalence

**Purpose:**
To verify that the chosen BigQuery table management strategy (e.g., `MERGE` for upserts or `CREATE OR REPLACE TABLE` for full refreshes) behaves equivalently to the legacy "new table and swap" pattern, ensuring atomicity and data consistency during updates.

**Setup:**
1.  Load initial source data into `project.source_dataset.sof_ta_p_...`.
2.  Run the legacy job with this initial data and capture `legacy_output_tables_initial`.
3.  Modify the source data (add new rows, update existing rows, delete some rows).
4.  Run the legacy job *again* with the modified data (full refresh, `p_wiederanlaufWert=0`) and capture `legacy_output_tables_modified`.
5.  Load the modified source data into `project.source_dataset.sof_ta_p_...`.
6.  Ensure `project.reporting_dataset.rpt_ta_s_d1_...` tables are empty.

**Action:**
Execute the BigQuery job (full refresh):
```sql
CALL project.orchestration_dataset.BERT_AUSTAUSCH_KSH_SP(p_stichtag => '2023-10-26', p_wiederanlaufWert => 0);
```

**Pass/Fail Criterion:**
1.  The `job_control` table must show `status = 'SUCCEEDED'`.
2.  All `rpt$...` tables in `project.reporting_dataset` must be identical to `project.golden_dataset.legacy_output_tables_modified`. This demonstrates that the BigQuery table update strategy correctly reflects all changes from the source, just as the legacy swap pattern would.

**Test Code (SQL Assertion):**
```sql
-- Compare all target tables against the legacy modified output
-- ... (repeat comparison queries for all rpt$... tables as in Test Case 1,
--      but comparing against `legacy_output_tables_modified`)
```

---

### Test Case 15: Unused Code Omission

**Purpose:**
To verify that commented-out or deprecated sections of the legacy `d_ausd_austausch.sql` (e.g., `INSERT INTO SOF$TA_K_BERT_DATENSTAND` or `AL??` blocks) are correctly identified and omitted from the BigQuery migration, reducing code bloat and potential for errors.

**Setup:**
No specific data setup is required. This is primarily a code review and static analysis test.

**Action:**
Manually review the BigQuery stored procedure code (`project.orchestration_dataset.D_AUSD_AUSTAUSCH_SP`) and compare it against the original `d_ausd_austausch.sql` file.

**Pass/Fail Criterion:**
The BigQuery stored procedure code must *not* contain any translated logic corresponding to the identified unused, commented-out, or deprecated sections of the legacy SQL.

**Test Code (Manual Review / Static Analysis):**
```
# Manual review of BigQuery stored procedure code:
# - Search for any BigQuery SQL that appears to translate commented-out Oracle SQL.
# - Confirm that logic related to 'INSERT INTO SOF$TA_K_BERT_DATENSTAND' is absent.
# - Verify that any 'AL??' blocks or similar deprecated logic are not present.

# Example check (conceptual, not runnable BigQuery SQL):
# ASSERT NOT CONTAINS(D_AUSD_AUSTAUSCH_SP_CODE, 'INSERT INTO `project.reporting_dataset.sof_ta_k_bert_datenstand`');
```

---

### Test Case 16: Performance Assessment (Non-Functional)

**Purpose:**
To assess the performance of the migrated BigQuery job against the legacy Oracle job, especially given the complexity of `CASE` statements and joins, and to identify any potential performance regressions or improvements. This addresses a key risk identified in the design document.

**Setup:**
1.  Load a large volume of production-like source data (e.g., 100GB-1TB) into both the Oracle source system and `project.source_dataset` in BigQuery.
2.  Ensure both legacy and migrated environments are configured for optimal performance (e.g., appropriate Oracle indexes/statistics, BigQuery partitioning/clustering).
3.  Record baseline execution times for the legacy job under typical load.

**Action:**
Execute the BigQuery job with `p_wiederanlaufWert=0` and record its total execution time:
```sql
-- Execute and monitor execution time using BigQuery job history or client libraries.
CALL project.orchestration_dataset.BERT_AUSTAUSCH_KSH_SP(p_stichtag => '2023-10-26', p_wiederanlaufWert => 0);
```

**Pass/Fail Criterion:**
The BigQuery job's execution time should be within an acceptable threshold (e.g., +/- 20% of the legacy job's execution time, or ideally, significantly faster). This is a non-functional requirement, and the threshold should be agreed upon with stakeholders.

**Test Code (Conceptual Performance Monitoring):**
```python
import time
from google.cloud import bigquery

def test_performance_assessment(bigquery_client):
    stichtag = '2023-10-26'
    wiederanlaufwert = 0
    
    # Define acceptable performance threshold (e.g., 80% to 120% of legacy time)
    legacy_execution_time_seconds = 3600  # Example: 1 hour for legacy job
    min_acceptable_time = legacy_execution_time_seconds * 0.8
    max_acceptable_time = legacy_execution_time_seconds * 1.2

    start_time = time.time()
    query_job = bigquery_client.query(f"""
        CALL project.orchestration_dataset.BERT_AUSTAUSCH_KSH_SP(
            p_stichtag => '{stichtag}', 
            p_wiederanlaufWert => {wiederanlaufwert}
        );
    """)
    query_job.result() # Wait for the job to complete
    end_time = time.time()
    
    migrated_execution_time_seconds = end_time - start_time
    
    print(f"Legacy execution time: {legacy_execution_time_seconds:.2f} seconds")
    print(f"Migrated execution time: {migrated_execution_time_seconds:.2f} seconds")
    
    assert min_acceptable_time <= migrated_execution_time_seconds <= max_acceptable_time, \
        f"Performance outside acceptable range. Migrated: {migrated_execution_time_seconds:.2f}s, " \
        f"Expected: {min_acceptable_time:.2f}s - {max_acceptable_time:.2f}s."
```