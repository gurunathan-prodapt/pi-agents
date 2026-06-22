As a senior data-migration QA engineer, I've analyzed the provided legacy Korn Shell script (`k_ausd_bp_ta_bcp_msisdn.ksh`) and the migrated Airflow DAG (`d_ausd_bp_ta_bcp_msisdn.py`).

**Migration Design Interpretation:**

The legacy script acts as a wrapper, handling parameter parsing, validation, and then executing an Oracle SQL script (`d_ausd_bp_ta_bcp_msisdn.sql`) via `sqlplus`. It also captures a record count from a temporary file. The actual data transformation logic within `d_ausd_bp_ta_bcp_msisdn.sql` is *not provided*.

The migrated Airflow DAG, in its current form, appears to be designed as a **metadata logging job** rather than a direct data transformation job. The BigQuery SQL within the DAG:
1.  Creates/appends to a table named `PoolBasisprodukt`.
2.  Inserts the job's input parameters (`job_kennung`, `eintrags_nr`, `stichtag`, `wiederanlauf_wert`, `datum_heute`, `datum_gestern`).
3.  Calculates `records_processed` by performing a `COUNT(1)` on a placeholder `source_table` with a date filter based on `p_Stichtag`.
4.  Adds a `created_at` timestamp.

The comment `-- Replace this source query with the actual BigQuery transformation logic` strongly suggests that the `PoolBasisprodukt` table is currently intended to log job execution details, and the actual data transformation (which `d_ausd_bp_ta_bcp_msisdn.sql` would have performed) is expected to be inserted into the subquery's `SELECT * FROM ...` part at a later stage, or is handled by an upstream process.

Therefore, the validation tests will focus on ensuring the correct capture and logging of parameters, accurate date calculations, and the correct count from the *placeholder* source, rather than validating a full data transformation that is not yet present in the migrated code.

---

## Migration Validation Tests for `d_ausd_bp_ta_bcp_msisdn.py`

### Test Setup Prerequisites

Before running any tests, ensure the following:

1.  **Airflow Environment:** A running Airflow instance with the `d_ausd_bp_ta_bcp_msisdn` DAG deployed.
2.  **GCP Project & BigQuery:** A GCP project configured for BigQuery, with the necessary service account permissions.
3.  **Airflow Variables:** The following Airflow variables are set:
    *   `gcp_project`: Your GCP project ID.
    *   `bq_dataset`: The BigQuery dataset where `PoolBasisprodukt` will be created (e.g., `dw_logs`).
    *   `source_dataset`: The BigQuery dataset containing the mock source data (e.g., `dw_source`).
    *   `bq_location`: BigQuery dataset location (e.g., `EU`).
4.  **Mock Source Table:** Create a mock BigQuery table named `source_table` in `{{ var.value.source_dataset }}`. This table needs at least a `_PARTITIONTIME` pseudo-column (can be simulated with a `DATE` column if not using partitioned tables, but `_PARTITIONTIME` is standard for partitioned tables).
    ```sql
    -- Example DDL for mock source_table
    CREATE TABLE IF NOT EXISTS `your-gcp-project.dw_source.source_table` (
        id INT64,
        value STRING,
        _PARTITIONTIME TIMESTAMP OPTIONS(description="Simulated partition time for filtering")
    )
    PARTITION BY DATE(_PARTITIONTIME);

    -- Example data for mock source_table
    INSERT INTO `your-gcp-project.dw_source.source_table` (_PARTITIONTIME, id, value) VALUES
    ('2023-03-14 00:00:00 UTC', 1, 'data_14_1'),
    ('2023-03-14 00:00:00 UTC', 2, 'data_14_2'),
    ('2023-03-15 00:00:00 UTC', 3, 'data_15_1'),
    ('2023-03-15 00:00:00 UTC', 4, 'data_15_2'),
    ('2023-03-15 00:00:00 UTC', 5, 'data_15_3'),
    ('2023-03-16 00:00:00 UTC', 6, 'data_16_1'),
    ('2023-03-16 00:00:00 UTC', 7, 'data_16_2'),
    ('2023-03-17 00:00:00 UTC', 8, 'data_17_1');
    ```

---

### 1. Output Parity - Parameter Logging

*   **Purpose:** Verify that all input parameters from the legacy KSH script are correctly captured and stored as metadata in the `PoolBasisprodukt` table by the migrated DAG.
*   **Setup:**
    1.  Ensure the mock `source_table` is populated (as per prerequisites).
    2.  Trigger the Airflow DAG `d_ausd_bp_ta_bcp_msisdn` with a specific `dag_run.conf` payload:
        ```json
        {
            "p_JobKennung": "TEST_JOB_A",
            "p_EintragsNr": "ENTRY_001",
            "p_Stichtag": "16032023",
            "p_wiederanlaufWert": 100
        }
        ```
    3.  Let the DAG run to completion.
*   **Action:** Query the `PoolBasisprodukt` table for the most recent entry.
    ```sql
    SELECT
        job_kennung,
        eintrags_nr,
        stichtag,
        wiederanlauf_wert
    FROM
        `{{ var.value.gcp_project }}.{{ var.value.bq_dataset }}.PoolBasisprodukt`
    ORDER BY
        created_at DESC
    LIMIT 1;
    ```
*   **Pass/Fail Criterion:** The queried row's `job_kennung`, `eintrags_nr`, `stichtag`, and `wiederanlauf_wert` columns exactly match the input parameters provided in the `dag_run.conf`.
    *   Expected: `job_kennung='TEST_JOB_A'`, `eintrags_nr='ENTRY_001'`, `stichtag='16032023'`, `wiederanlauf_wert=100`.

### 2. Transformation Correctness - Date Handling (`datum_heute`, `datum_gestern`)

*   **Purpose:** Verify that `datum_heute` and `datum_gestern` are correctly derived based on the DAG's logical date (`ds`), mimicking the `gestern.ksh` behavior.
*   **Setup:**
    1.  Ensure the mock `source_table` is populated.
    2.  Trigger the Airflow DAG `d_ausd_bp_ta_bcp_msisdn` for a specific logical date, e.g., `2023-03-16`. (This is typically done by setting the `execution_date` when manually triggering, or letting it run on its schedule).
    3.  Provide minimal `dag_run.conf` parameters, as `datum_heute` and `datum_gestern` default to `ds` and `macros.ds_add(ds, -1)`.
        ```json
        {
            "p_JobKennung": "DATE_TEST",
            "p_EintragsNr": "DATE_001",
            "p_Stichtag": "16032023"
        }
        ```
    4.  Let the DAG run to completion.
*   **Action:** Query the `PoolBasisprodukt` table for the most recent entry.
    ```sql
    SELECT
        datum_heute,
        datum_gestern
    FROM
        `{{ var.value.gcp_project }}.{{ var.value.bq_dataset }}.PoolBasisprodukt`
    ORDER BY
        created_at DESC
    LIMIT 1;
    ```
*   **Pass/Fail Criterion:** The `datum_heute` column matches the DAG's logical date (`ds`) and `datum_gestern` matches `ds - 1 day`.
    *   If `ds` was `2023-03-16`: Expected: `datum_heute='2023-03-16'`, `datum_gestern='2023-03-15'`.

### 3. Transformation Correctness - `p_wiederanlaufWert` Default

*   **Purpose:** Verify that `p_wiederanlaufWert` correctly defaults to `0` if not provided in the `dag_run.conf`, matching the legacy script's initialization (`if [[ -z "$p_wiederanlaufWert" ]] then p_wiederanlaufWert=0 fi`).
*   **Setup:**
    1.  Ensure the mock `source_table` is populated.
    2.  Trigger the Airflow DAG `d_ausd_bp_ta_bcp_msisdn` *without* providing `p_wiederanlaufWert` in the `dag_run.conf`:
        ```json
        {
            "p_JobKennung": "DEFAULT_TEST",
            "p_EintragsNr": "DEF_001",
            "p_Stichtag": "16032023"
        }
        ```
    3.  Let the DAG run to completion.
*   **Action:** Query the `PoolBasisprodukt` table for the most recent entry.
    ```sql
    SELECT
        wiederanlauf_wert
    FROM
        `{{ var.value.gcp_project }}.{{ var.value.bq_dataset }}.PoolBasisprodukt`
    ORDER BY
        created_at DESC
    LIMIT 1;
    ```
*   **Pass/Fail Criterion:** The `wiederanlauf_wert` column is `0`.
    *   Expected: `wiederanlauf_wert=0`.

### 4. Transformation Correctness - `Stichtag` Filter Logic and `records_processed`

*   **Purpose:** Verify that the BigQuery SQL correctly applies the `DATE(_PARTITIONTIME) BETWEEN DATE_SUB(PARSE_DATE('%d%m%Y', '{p_stichtag}'), INTERVAL 1 DAY) AND PARSE_DATE('%d%m%Y', '{p_stichtag}')` filter to the `source_table` and that `records_processed` accurately reflects the count of filtered records.
*   **Setup:**
    1.  Ensure the mock `source_table` is populated with data across `2023-03-14`, `2023-03-15`, `2023-03-16`, `2023-03-17` as per prerequisites.
        *   `2023-03-14`: 2 records
        *   `2023-03-15`: 3 records
        *   `2023-03-16`: 2 records
        *   `2023-03-17`: 1 record
    2.  Trigger the Airflow DAG `d_ausd_bp_ta_bcp_msisdn` with `p_Stichtag='16032023'`:
        ```json
        {
            "p_JobKennung": "FILTER_TEST",
            "p_EintragsNr": "FIL_001",
            "p_Stichtag": "16032023"
        }
        ```
    3.  Let the DAG run to completion.
*   **Action:** Query the `PoolBasisprodukt` table for the most recent entry.
    ```sql
    SELECT
        records_processed
    FROM
        `{{ var.value.gcp_project }}.{{ var.value.bq_dataset }}.PoolBasisprodukt`
    ORDER BY
        created_at DESC
    LIMIT 1;
    ```
*   **Pass/Fail Criterion:** The `records_processed` column equals the sum of records in `source_table` for `2023-03-15` and `2023-03-16`.
    *   Expected: `records_processed = 3 (from 2023-03-15) + 2 (from 2023-03-16) = 5`.

### 5. Data Quality / Schema Assertions

*   **Purpose:** Verify that the `PoolBasisprodukt` table is created with the expected schema, column names, and data types, and that `created_at` is a valid timestamp.
*   **Setup:**
    1.  Ensure the `PoolBasisprodukt` table has been created by a previous DAG run.
*   **Action:** Query BigQuery's `INFORMATION_SCHEMA.COLUMNS` for the `PoolBasisprodukt` table.
    ```sql
    SELECT
        column_name,
        data_type
    FROM
        `{{ var.value.gcp_project }}.{{ var.value.bq_dataset }}.INFORMATION_SCHEMA.COLUMNS`
    WHERE
        table_name = 'PoolBasisprodukt'
    ORDER BY
        ordinal_position;
    ```
*   **Pass/Fail Criterion:** The schema matches the expected structure:
    *   `job_kennung` (STRING)
    *   `eintrags_nr` (STRING)
    *   `stichtag` (STRING)
    *   `wiederanlauf_wert` (INT64)
    *   `datum_heute` (STRING)
    *   `datum_gestern` (STRING)
    *   `records_processed` (INT64)
    *   `created_at` (TIMESTAMP)
    Additionally, verify that the `created_at` column in a sample row contains a valid timestamp value.

### 6. External System Replacement - Oracle to BigQuery Source Access

*   **Purpose:** Verify that the BigQuery `source_table` is correctly accessed as the data source, effectively replacing the implicit Oracle source of the legacy `d_ausd_bp_ta_bcp_msisdn.sql` script.
*   **Setup:**
    1.  Ensure `var.value.source_dataset` and `var.value.source_table` are correctly configured in Airflow variables and the mock `source_table` exists and is accessible.
    2.  Trigger the Airflow DAG `d_ausd_bp_ta_bcp_msisdn` with any valid parameters.
*   **Action:** Observe the successful execution of the `process_poolbasisprodukt` task (which uses `BigQueryExecuteQueryOperator`). Review Airflow logs for any BigQuery-related errors.
*   **Pass/Fail Criterion:** The `process_poolbasisprodukt` task completes successfully without BigQuery access errors. This is implicitly covered by other tests that verify the data written to `PoolBasisprodukt`, but explicitly confirms the BigQuery source connection.

### 7. Idempotency / `WRITE_APPEND` Behavior

*   **Purpose:** Verify that subsequent runs of the DAG append new records to `PoolBasisprodukt` rather than overwriting existing data, as specified by `write_disposition="WRITE_APPEND"`.
*   **Setup:**
    1.  Ensure the mock `source_table` is populated.
    2.  **First Run:** Trigger the Airflow DAG `d_ausd_bp_ta_bcp_msisdn` with `p_Stichtag='15032023'`:
        ```json
        {
            "p_JobKennung": "APPEND_TEST_1",
            "p_EintragsNr": "APP_001",
            "p_Stichtag": "15032023"
        }
        ```
    3.  Let the first DAG run complete.
    4.  **Second Run:** Trigger the Airflow DAG `d_ausd_bp_ta_bcp_msisdn` again with *different* parameters, e.g., `p_Stichtag='16032023'`:
        ```json
        {
            "p_JobKennung": "APPEND_TEST_2",
            "p_EintragsNr": "APP_002",
            "p_Stichtag": "16032023"
        }
        ```
    5.  Let the second DAG run complete.
*   **Action:** Query the total row count and content of the `PoolBasisprodukt` table.
    ```sql
    SELECT
        COUNT(*) AS total_rows,
        ARRAY_AGG(STRUCT(job_kennung, stichtag, records_processed) ORDER BY created_at) AS entries
    FROM
        `{{ var.value.gcp_project }}.{{ var.value.bq_dataset }}.PoolBasisprodukt`
    WHERE
        job_kennung LIKE 'APPEND_TEST_%';
    ```
*   **Pass/Fail Criterion:** The `PoolBasisprodukt` table contains two distinct entries corresponding to the two DAG runs. The `total_rows` count for `APPEND_TEST_%` jobs should be 2.
    *   Expected `entries` to contain:
        *   `job_kennung='APPEND_TEST_1'`, `stichtag='15032023'`, `records_processed=3` (from `2023-03-14` and `2023-03-15` in mock data)
        *   `job_kennung='APPEND_TEST_2'`, `stichtag='16032023'`, `records_processed=5` (from `2023-03-15` and `2023-03-16` in mock data)

---