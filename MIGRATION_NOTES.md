# Migration Notes: `d_alis_spaufruf_p0.sql`

## 1. Summary
This document details the migration of `d_alis_spaufruf_p0.sql` from a legacy Oracle SQL*Plus environment to Google Cloud Platform (GCP). 

*   **Source Platform:** Oracle SQL*Plus (PL/SQL wrapper script)
*   **Target Platform:** Google BigQuery (GoogleSQL Procedural Language)
*   **Orchestration:** Cloud Composer (Apache Airflow) using `BigQueryInsertJobOperator`
*   **Migration Pattern:** UC4 + KSH + SQL_SIMPLE (Confidence: **High**)

The legacy script served as a dynamic utility wrapper. It imported environment settings from `d_alis_init.sql`, configured SQL*Plus session parameters, executed a dynamic stored procedure passed via the positional parameter `&1`, and managed transactional boundaries (`COMMIT` on success, `ROLLBACK` and exit on failure). In the target architecture, this logic is replaced by a parameterized BigQuery scripting block utilizing dynamic SQL (`EXECUTE IMMEDIATE`) wrapped in a robust transaction block (`BEGIN TRANSACTION ... EXCEPTION ... END`).

---

## 2. Generated Artifacts
The migration process yields the following files:

| File Name | Role / Description |
| :--- | :--- |
| `d_alis_spaufruf_p0.sql` | The translated GoogleSQL script containing the dynamic execution wrapper, sanitization logic, and transaction handling. |
| `d_alis_spaufruf_p0_dag.py` | (Optional/Reference) Airflow DAG template utilizing `BigQueryInsertJobOperator` to pass query parameters (`@target_dataset`, `@procedure_name`, `@arguments`) to the SQL script. |

---

## 3. Key Design Decisions

### 3.1 Elimination of SQL*Plus Session Commands
Oracle-specific session commands (such as `SET SERVEROUTPUT`, `SET ARRAYSIZE`, `SET PAGESIZE`, `ECHO`, `VERIFY`, and `FEEDBACK`) have no functional equivalent in BigQuery and have been completely deprecated.

### 3.2 Transactional Safety & Error Propagation
To replicate the legacy behavior of `WHENEVER OSERROR EXIT FAILURE ROLLBACK`, the BigQuery script uses a nested transaction block:
*   **`BEGIN TRANSACTION` / `COMMIT TRANSACTION`**: Ensures that any DML operations executed within the dynamic procedure are committed atomically.
*   **`EXCEPTION WHEN ERROR THEN ROLLBACK TRANSACTION`**: Catches execution failures, rolls back uncommitted changes, and uses `RAISE USING MESSAGE` to propagate the error back to Cloud Composer, ensuring the Airflow task fails visibly.

### 3.3 SQL Injection Mitigation & Dynamic Execution
Because BigQuery does not allow direct parameterization of identifier names (such as dataset or procedure names) in standard `CALL` statements, dynamic SQL (`EXECUTE IMMEDIATE`) is required. To prevent SQL injection:
*   Input parameters are sanitized using `REGEXP_REPLACE` to strip out any characters that are not alphanumeric or underscores (`[^a-zA-Z0-9_]`).
*   The execution string is safely constructed using the `FORMAT` function.

### 3.4 Support for Optional Arguments
The legacy script was often extended to pass arguments. The migrated script dynamically checks if `@arguments` is provided. If present, it appends them to the `CALL` statement; otherwise, it executes a parameterless call.

---

## 4. Manual Steps Before Go-Live

### 4.1 Schema & Dataset Creation
Ensure that the target BigQuery datasets (corresponding to the legacy schemas) are created in your target GCP project and region.

### 4.2 IAM & Permissions
The Service Account running the Cloud Composer worker nodes (or the identity executing the Airflow DAG) must have the following IAM roles:
*   `roles/bigquery.jobUser` (to run the query job)
*   `roles/bigquery.dataEditor` or `roles/bigquery.admin` (on the target datasets to execute procedures and modify underlying tables)

### 4.3 Connection Strings & Environment Variables
*   Migrate any environment-wide variables or dataset mappings previously defined in the legacy initialization script `d_alis_init.sql` to Airflow Variables or Airflow Environment Variables.
*   Define the target GCP Project ID and default Dataset ID in your Airflow connection settings or DAG configuration.

### 4.4 Scheduling & Parameterization
Configure the Airflow DAG to pass the required query parameters. Example parameter mapping in the `BigQueryInsertJobOperator`:

```python
configuration={
    "query": {
        "query": read_sql_file("d_alis_spaufruf_p0.sql"),
        "useLegacySql": False,
        "parameterMode": "NAMED",
        "queryParameters": [
            {"name": "target_dataset", "parameterType": {"type": "STRING"}, "parameterValue": {"value": "my_target_dataset"}},
            {"name": "procedure_name", "parameterType": {"type": "STRING"}, "parameterValue": {"value": "my_stored_procedure"}},
            {"name": "arguments", "parameterType": {"type": "STRING"}, "parameterValue": {"value": "'arg1', 123, TRUE"}} # Use NULL or empty string if none
        ]
    }
}
```

---

## 5. Known Gaps & Unresolved References

### 5.1 Legacy Initialization Script (`d_alis_init.sql`)
The legacy script `d_alis_init.sql` is not directly migrated. Any global session variables, temporary table structures, or environment configurations defined within it must be handled at the orchestration level (Airflow) or refactored into individual BigQuery procedures.

### 5.2 Complex Argument Datatypes
The current dynamic argument handler (`@arguments`) accepts a single string representing comma-separated values (e.g., `"'value1', 100"`). If target procedures require complex structures, arrays, or `STRUCT` types as input parameters, the dynamic string construction must be manually customized or the target procedures refactored.

---

## 6. Validation

### 6.1 How to Run the Tests
1.  **Deploy a Mock Procedure:** Create a simple test procedure in your target BigQuery dataset:
    ```sql
    CREATE OR REPLACE PROCEDURE `your_dataset.test_proc`(msg STRING)
    BEGIN
      SELECT FORMAT("Success: %s", msg);
    END;
    ```
2.  **Execute the Wrapper:** Run the migrated `d_alis_spaufruf_p0.sql` script in the BigQuery Console, providing the following parameters:
    *   `@target_dataset` = `'your_dataset'`
    *   `@procedure_name` = `'test_proc'`
    *   `@arguments` = `"'Hello World'"`

### 6.2 Definition of "Passing"
*   **Success Case:** The query completes successfully, the mock procedure executes, and no errors are thrown.
*   **Failure Case (Rollback Test):** Attempt to call a non-existent procedure or pass invalid arguments. The script must fail, outputting a structured error message starting with `SP_Execution_Error: ...`, and any transaction changes must be rolled back.

---

## 7. Rollback Procedure
In the event of a deployment failure or unexpected runtime behavior:

1.  **Disable Orchestration:** Pause the newly deployed Airflow DAG calling `d_alis_spaufruf_p0.sql`.
2.  **Revert to Legacy (if hybrid phase):** Route the upstream UC4/KSH jobs back to the legacy Oracle database environment.
3.  **Inspect Transaction Logs:** Query the BigQuery `INFORMATION_SCHEMA.JOBS_BY_PROJECT` to identify the exact query execution ID, error messages, and state of the transaction at the time of failure:
    ```sql
    SELECT query, error_result, state 
    FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT 
    WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
      AND query LIKE '%d_alis_spaufruf_p0%';
    ```