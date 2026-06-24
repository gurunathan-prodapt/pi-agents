As a senior data-migration QA engineer, I've designed a suite of validation tests for the `DW.BERT_AUSD_BP_TA_BCP_ICCID` job migration. These tests aim to ensure the migrated BigQuery solution is behaviourally equivalent to the legacy Oracle/KornShell system, covering output parity, transformation correctness, external system interactions, and data quality.

Given that the legacy source code is unavailable and no generated code is provided, these tests are derived directly from the "MIGRATION DESIGN DOCUMENT". We assume the functionality marked for "retire" in the legacy `d_ausd_bp_ta_bcp_iccid.sql` is still required and has been reimplemented in BigQuery as per the design document's assumption.

---

## Migration Validation Tests: DW.BERT_AUSD_BP_TA_BCP_ICCID

### 1. Output Parity Tests

#### Test Case 1.1: Full Data Parity (Happy Path)

*   **Purpose:** To verify that the migrated BigQuery job produces an identical dataset in the target table (`sof_schema.ta_bcp_iccid_bq`) as the legacy Oracle job (`sof$ta_bcp_iccid`), given the same source data and parameters. This is the primary end-to-end validation.
*   **Setup:**
    1.  **Source Data Preparation:** Create a comprehensive, representative dataset in the Oracle source tables:
        *   `isbert_schema.dwtk_meldungen`: Include multiple entries for `job_kennung = 'BERT_DROP_TEMP_TABLE'` with varying `timecreated` values, and entries for other `job_kennung` values. Ensure at least one `MAX(timecreated)` for `BERT_DROP_TEMP_TABLE` is present.
        *   `sof$ta_bpr_bcp`: Include records with `cntrct_id_ref` values that will match, not match, and have duplicates in `sof$ta_iccid_vertrag`.
        *   `sof$ta_iccid_vertrag`: Include records with `cntrct_id` values that will match, not match, and have duplicates in `sof$ta_bpr_bcp`.
        *   Include `NULL` values in `cntrct_id_ref`, `cntrct_id`, and other selected columns (`bpr_id`, `tn_iccid`, `tn_imsi_hlr`) to test NULL handling.
    2.  **Data Replication:** Extract the exact dataset from the Oracle source tables and load it into their corresponding BigQuery source tables (`isbert_schema.dwtk_meldungen_bq`, `sof_schema.ta_bpr_bcp_bq`, `sof_schema.ta_iccid_vertrag_bq`).
    3.  **Target Table State:** Ensure both `sof$ta_bcp_iccid` (Oracle) and `sof_schema.ta_bcp_iccid_bq` (BigQuery) are empty before execution.
*   **Action:**
    1.  Execute the legacy `DW.BERT_AUSD_BP_TA_BCP_ICCID` job on the Oracle environment.
    2.  Execute the migrated Airflow DAG `dw_bert_ausd_bp_ta_bcp_iccid_dag.py` on the BigQuery environment.
*   **Pass/Fail Criterion:**
    *   The total row count in `sof$ta_bcp_iccid` (Oracle) must be identical to the total row count in `sof_schema.ta_bcp_iccid_bq` (BigQuery).
    *   A deep comparison of the data in both target tables must show exact parity for all columns. This can be achieved by comparing checksums or by performing a full outer join and checking for mismatches.

```sql
-- Example SQL for row count comparison
SELECT
    (SELECT COUNT(*) FROM oracle_schema.SOF$TA_BCP_ICCID) AS oracle_row_count,
    (SELECT COUNT(*) FROM bigquery_project.sof_schema.ta_bcp_iccid_bq) AS bigquery_row_count;

-- Example SQL for data comparison (BigQuery side, assuming Oracle data is loaded into a temp BQ table for comparison)
-- This assumes you've extracted Oracle's final output into a temporary BigQuery table, e.g., `sof_schema.ta_bcp_iccid_oracle_snapshot`
SELECT
    'Mismatch in BigQuery only' AS mismatch_type,
    bq.*
FROM
    bigquery_project.sof_schema.ta_bcp_iccid_bq AS bq
FULL OUTER JOIN
    bigquery_project.sof_schema.ta_bcp_iccid_oracle_snapshot AS ora
ON
    bq.CNTRCT_ID = ora.CNTRCT_ID AND
    bq.BPR_ID = ora.BPR_ID AND
    bq.CNTRCT_ID_REF = ora.CNTRCT_ID_REF AND
    bq.TN_ICCID = ora.TN_ICCID AND
    bq.TN_IMSI_HLR = ora.TN_IMSI_HLR
WHERE
    ora.CNTRCT_ID IS NULL OR bq.CNTRCT_ID IS NULL;

-- Pass if the above query returns 0 rows.
```

### 2. Transformation Correctness Tests

#### Test Case 2.1: `v_datum` Derivation Correctness

*   **Purpose:** To verify that the logic for deriving the `v_datum` (snapshot date) from `dwtk_meldungen` is correctly translated to BigQuery, including the `MAX` aggregation, filtering, `NVL`/`COALESCE`, and date formatting.
*   **Setup:**
    1.  Populate `isbert_schema.dwtk_meldungen` (Oracle) and `isbert_schema.dwtk_meldungen_bq` (BigQuery) with the following scenarios:
        *   Multiple records for `job_kennung = 'BERT_DROP_TEMP_TABLE'` with different `timecreated` values (e.g., '2023-01-01 10:00:00', '2023-01-01 11:00:00', '2023-01-02 09:00:00').
        *   Records for `job_kennung = 'BERT_DROP_TEMP_TABLE'` where `timecreated` is `NULL`.
        *   No records for `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
        *   Records for other `job_kennung` values.
    2.  Ensure `timecreated` column has `TIMESTAMP` data type in both systems.
*   **Action:**
    1.  Execute the Oracle `v_datum` derivation query:
        ```sql
        SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS s_datum
        FROM isbert_schema.dwtk_meldungen m
        WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';
        ```
    2.  Execute the BigQuery `v_datum` derivation query:
        ```sql
        SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101') AS s_datum
        FROM bigquery_project.isbert_schema.dwtk_meldungen_bq AS m
        WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';
        ```
*   **Pass/Fail Criterion:** The `s_datum` value returned by the BigQuery query must exactly match the `s_datum` value returned by the Oracle query for each setup scenario.

#### Test Case 2.2: Join Logic and `DISTINCT` Handling

*   **Purpose:** To verify that the `INNER JOIN` condition (`bp.cntrct_id_ref = ic.cntrct_id`) and the `DISTINCT` clause are correctly applied in BigQuery, producing the same set of unique output rows as Oracle.
*   **Setup:**
    1.  Populate `sof$ta_bpr_bcp` and `sof$ta_iccid_vertrag` (and their BQ counterparts) with data covering:
        *   **Matching Records:** `cntrct_id_ref` in `bpr_bcp` matches `cntrct_id` in `iccid_vertrag`.
        *   **Non-Matching Records:** `cntrct_id_ref` in `bpr_bcp` has no match in `iccid_vertrag`.
        *   **One-to-Many Join:** One `bpr_bcp` record joins to multiple `iccid_vertrag` records.
        *   **Many-to-One Join:** Multiple `bpr_bcp` records join to one `iccid_vertrag` record.
        *   **Duplicate Output Rows (before DISTINCT):** Data structured such that the join would produce identical rows for the selected columns (`CNTRCT_ID, BPR_ID, CNTRCT_ID_REF, TN_ICCID, TN_IMSI_HLR`) if `DISTINCT` were not applied.
        *   **NULL Join Keys:** Records where `bp.cntrct_id_ref` or `ic.cntrct_id` are `NULL`.
*   **Action:**
    1.  Execute the legacy job.
    2.  Execute the migrated job.
*   **Pass/Fail Criterion:**
    *   The set of `(CNTRCT_ID, BPR_ID, CNTRCT_ID_REF, TN_ICCID, TN_IMSI_HLR)` tuples in `sof_schema.ta_bcp_iccid_bq` must be identical to the set in `sof$ta_bcp_iccid` (Oracle).
    *   Specifically, records with `NULL` join keys should be excluded from the output (as it's an inner join).
    *   No duplicate rows (based on all output columns) should exist in either target table.

```sql
-- Example SQL to check for duplicates in BigQuery target table
SELECT
    CNTRCT_ID, BPR_ID, CNTRCT_ID_REF, TN_ICCID, TN_IMSI_HLR,
    COUNT(*) AS row_count
FROM
    bigquery_project.sof_schema.ta_bcp_iccid_bq
GROUP BY
    CNTRCT_ID, BPR_ID, CNTRCT_ID_REF, TN_ICCID, TN_IMSI_HLR
HAVING
    COUNT(*) > 1;

-- Pass if the above query returns 0 rows.
```

#### Test Case 2.3: NULL Handling in Output Columns

*   **Purpose:** To ensure that `NULL` values in the source columns selected for the target table are correctly preserved as `NULL` in the BigQuery target table.
*   **Setup:**
    1.  Populate `sof$ta_bpr_bcp` and `sof$ta_iccid_vertrag` (and their BQ counterparts) with records that will successfully join, but where some of the selected columns (`BPR_ID`, `TN_ICCID`, `TN_IMSI_HLR`) contain `NULL` values.
*   **Action:**
    1.  Execute the legacy job.
    2.  Execute the migrated job.
*   **Pass/Fail Criterion:** For all records in `sof_schema.ta_bcp_iccid_bq`, if a source column was `NULL` in Oracle, the corresponding column in BigQuery must also be `NULL`.

```sql
-- Example SQL to check for NULL parity (assuming Oracle snapshot in BQ)
SELECT
    'Mismatch in NULL handling' AS mismatch_type,
    bq.CNTRCT_ID, bq.BPR_ID, bq.TN_ICCID, bq.TN_IMSI_HLR
FROM
    bigquery_project.sof_schema.ta_bcp_iccid_bq AS bq
JOIN
    bigquery_project.sof_schema.ta_bcp_iccid_oracle_snapshot AS ora
ON
    bq.CNTRCT_ID = ora.CNTRCT_ID AND bq.CNTRCT_ID_REF = ora.CNTRCT_ID_REF
WHERE
    (bq.BPR_ID IS NULL AND ora.BPR_ID IS NOT NULL) OR
    (bq.BPR_ID IS NOT NULL AND ora.BPR_ID IS NULL) OR
    (bq.TN_ICCID IS NULL AND ora.TN_ICCID IS NOT NULL) OR
    (bq.TN_ICCID IS NOT NULL AND ora.TN_ICCID IS NULL) OR
    (bq.TN_IMSI_HLR IS NULL AND ora.TN_IMSI_HLR IS NOT NULL) OR
    (bq.TN_IMSI_HLR IS NOT NULL AND ora.TN_IMSI_HLR IS NULL);

-- Pass if the above query returns 0 rows.
```

### 3. External-System Replacements Tests

#### Test Case 3.1: Airflow DAG Orchestration and Parameter Handling

*   **Purpose:** To verify that the Airflow DAG correctly orchestrates the Python scripts and BigQuery SQL, and that parameters (like `snapshot_date` if passed) are handled correctly through the Python layer to the SQL execution.
*   **Setup:**
    1.  Deploy the Airflow DAG (`dw_bert_ausd_bp_ta_bcp_iccid_dag.py`) and associated Python scripts (`r_ausd_bp_ta_bcp_iccid.py`, `k_ausd_bp_ta_bcp_iccid.py`).
    2.  Configure Airflow connections for BigQuery access.
    3.  Ensure `isbert_schema.dwtk_meldungen_bq` contains data to test `v_datum` derivation when no `snapshot_date` is provided.
*   **Action:**
    1.  **Scenario A (No explicit snapshot date):** Trigger the Airflow DAG without providing a `snapshot_date` parameter.
    2.  **Scenario B (Explicit snapshot date):** Trigger the Airflow DAG, passing a specific `snapshot_date` (e.g., '20230115') as an Airflow DAG run configuration parameter.
    3.  Monitor Airflow task logs for both scenarios.
*   **Pass/Fail Criterion:**
    *   **Scenario A:** The DAG must complete successfully. The BigQuery job should execute, and the `v_datum` used in the BigQuery SQL (which can be inspected from BigQuery job history or by querying the target table if `v_datum` is stored) must match the `MAX(timecreated)` logic from `dwtk_meldungen_bq`.
    *   **Scenario B:** The DAG must complete successfully. The `v_datum` used in the BigQuery SQL must match the explicitly passed `snapshot_date` parameter.
    *   Python script logs should confirm correct parameter parsing and BigQuery client invocation.

#### Test Case 3.2: BigQuery Interaction (Truncate/Insert)

*   **Purpose:** To verify that the migrated Python scripts correctly invoke BigQuery to perform the `TRUNCATE` (or `DELETE`) and `INSERT` operations as specified in the design.
*   **Setup:**
    1.  Populate `sof_schema.ta_bcp_iccid_bq` with some existing "dummy" data (e.g., 5-10 rows) that will be removed by the `TRUNCATE` operation.
    2.  Populate source tables (`ta_bpr_bcp_bq`, `ta_iccid_vertrag_bq`) with data that will result in new rows being inserted.
*   **Action:**
    1.  Execute the migrated Airflow DAG.
    2.  Immediately after the DAG starts, query `sof_schema.ta_bcp_iccid_bq` to observe the `TRUNCATE` effect.
    3.  After the DAG completes, query `sof_schema.ta_bcp_iccid_bq` again.
    4.  Inspect BigQuery job history for the project.
*   **Pass/Fail Criterion:**
    *   **Truncation:** The `sof_schema.ta_bcp_iccid_bq` table must be empty (or have 0 rows if `DELETE` is used) after the truncation step and before the insert step.
    *   **Insertion:** After the DAG completes, the table must contain the expected number of rows and data, matching the results from Test 1.1.
    *   BigQuery job history must show successful `TRUNCATE TABLE` (or `DELETE FROM`) and `INSERT INTO` operations for `sof_schema.ta_bcp_iccid_bq`.

### 4. Data Quality / Row-Count / Schema Assertions

#### Test Case 4.1: Row Count Parity

*   **Purpose:** To confirm that the total number of records processed and loaded into the target table remains consistent between the legacy and migrated systems.
*   **Setup:** Same as Test Case 1.1 (Full Data Parity).
*   **Action:**
    1.  Execute the legacy job.
    2.  Execute the migrated job.
    3.  Query the row counts from both target tables.
*   **Pass/Fail Criterion:** The row count from `sof$ta_bcp_iccid` (Oracle) must be exactly equal to the row count from `sof_schema.ta_bcp_iccid_bq` (BigQuery).

```sql
# Python (using BigQuery client and Oracle client/snapshot)
def test_row_count_parity():
    oracle_count = get_oracle_row_count("sof$ta_bcp_iccid")
    bigquery_count = get_bigquery_row_count("sof_schema.ta_bcp_iccid_bq")
    assert oracle_count == bigquery_count, \
        f"Row count mismatch: Oracle={oracle_count}, BigQuery={bigquery_count}"
```

#### Test Case 4.2: Schema Parity

*   **Purpose:** To verify that the schema of the target table in BigQuery (column names, data types, nullability) is functionally equivalent to the legacy Oracle target table.
*   **Setup:** N/A (schema comparison).
*   **Action:**
    1.  Extract the schema definition for `sof$ta_bcp_iccid` from Oracle (e.g., using `DESCRIBE TABLE` or `ALL_TAB_COLUMNS`).
    2.  Extract the schema definition for `sof_schema.ta_bcp_iccid_bq` from BigQuery (e.g., using `INFORMATION_SCHEMA.COLUMNS`).
*   **Pass/Fail Criterion:**
    *   All column names must match.
    *   The order of columns should ideally match, but at minimum, all columns must be present.
    *   Oracle data types must be correctly mapped to appropriate BigQuery data types (e.g., `NUMBER` to `INT64`/`NUMERIC`, `VARCHAR2` to `STRING`, `DATE`/`TIMESTAMP` to `TIMESTAMP`).
    *   Nullability constraints (e.g., `NOT NULL` in Oracle) should be reflected in BigQuery schema where applicable.

```sql
-- Example SQL for BigQuery schema extraction
SELECT
    column_name,
    data_type,
    is_nullable
FROM
    bigquery_project.sof_schema.INFORMATION_SCHEMA.COLUMNS
WHERE
    table_name = 'ta_bcp_iccid_bq'
ORDER BY
    ordinal_position;

-- Example SQL for Oracle schema extraction (conceptual)
-- SELECT column_name, data_type, nullable FROM ALL_TAB_COLUMNS WHERE owner = 'SOF' AND table_name = 'SOF$TA_BCP_ICCID' ORDER BY column_id;
```

#### Test Case 4.3: Data Quality - No Unexpected NULLs / Duplicates

*   **Purpose:** To assert that critical columns do not contain unexpected `NULL` values and that the `DISTINCT` clause effectively prevents duplicate rows based on the output columns.
*   **Setup:** Same as Test Case 1.1 (Full Data Parity).
*   **Action:**
    1.  Execute the legacy job.
    2.  Execute the migrated job.
    3.  Run data quality checks on the BigQuery target table.
*   **Pass/Fail Criterion:**
    *   **No unexpected NULLs:** Assuming `CNTRCT_ID` is a critical identifier and should not be `NULL` in the output, `SELECT COUNT(*) FROM sof_schema.ta_bcp_iccid_bq WHERE CNTRCT_ID IS NULL` must return 0. Similar checks for other critical columns.
    *   **No duplicates:** `SELECT COUNT(*) FROM (SELECT CNTRCT_ID, BPR_ID, CNTRCT_ID_REF, TN_ICCID, TN_IMSI_HLR FROM sof_schema.ta_bcp_iccid_bq GROUP BY 1,2,3,4,5 HAVING COUNT(*) > 1)` must return 0, confirming the `DISTINCT` clause is working as expected.

```sql
# Python (using BigQuery client)
def test_data_quality_assertions():
    # Check for unexpected NULLs in CNTRCT_ID
    null_cntrct_id_count = get_bigquery_query_result(
        "SELECT COUNT(*) FROM bigquery_project.sof_schema.ta_bcp_iccid_bq WHERE CNTRCT_ID IS NULL"
    )
    assert null_cntrct_id_count == 0, \
        f"Unexpected NULLs found in CNTRCT_ID: {null_cntrct_id_count} rows"

    # Check for duplicate rows based on all output columns
    duplicate_rows_count = get_bigquery_query_result(
        """
        SELECT COUNT(*) FROM (
            SELECT CNTRCT_ID, BPR_ID, CNTRCT_ID_REF, TN_ICCID, TN_IMSI_HLR
            FROM bigquery_project.sof_schema.ta_bcp_iccid_bq
            GROUP BY 1,2,3,4,5
            HAVING COUNT(*) > 1
        )
        """
    )
    assert duplicate_rows_count == 0, \
        f"Duplicate rows found in target table: {duplicate_rows_count} sets of duplicates"

# Helper functions (conceptual)
def get_bigquery_query_result(query):
    # Execute BigQuery query and return scalar result
    pass
```