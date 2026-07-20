# Migration Notes: `d_call_sp_template.ksh` to Python

This document outlines the migration details, design decisions, manual setup steps, and validation procedures for transitioning the legacy KornShell script `d_call_sp_template.ksh` to a modern, cloud-native Python implementation targeting Google Cloud BigQuery and Cloud Composer (Airflow).

---

## 1. Summary

The legacy KornShell script `d_call_sp_template.ksh` has been migrated to a native Python 3.11+ script (`d_call_sp_template.py`). 

*   **Source Platform:** On-Premise Data Warehouse running Oracle Database, orchestrated via UC4/Automic Scheduler.
*   **Target Platform:** Google Cloud Platform (GCP), utilizing **BigQuery** for data warehousing and **Cloud Composer (Airflow)** for orchestration.
*   **Key Transformation:** The legacy script wrapped an Oracle SQL\*Plus execution (`sqlplus`) that invoked an external SQL script (`d_call_sp_template.sql`) to run an Oracle Stored Procedure. The migrated Python script replaces this pattern by using the official Google Cloud BigQuery client library to execute a native BigQuery Stored Procedure via IAM-authenticated API calls.

---

## 2. Generated Artifacts

The migration process generated the following file:

| File Path | Role / Description |
| :--- | :--- |
| `ksh_unsupported/d_call_sp_template.py` | The migrated Python utility. It parses command-line arguments, resolves GCP environment variables, initializes the BigQuery client, and executes the parameterized BigQuery stored procedure call safely. |

---

## 3. Key Design Decisions

*   **Native Client Library vs. Subprocess CLI:** Rather than executing the `bq` command-line tool or a legacy SQL client via `subprocess.run()`, we adopted the native `google-cloud-bigquery` Python SDK. This provides robust, structured exception handling, native logging, and eliminates the overhead of shell process forks.
*   **IAM-Based Authentication:** Legacy clear-text credentials (`$user/$pass` sourced from `.dwh_init`) have been completely retired. The Python script relies on Google Application Default Credentials (ADC). When running in Cloud Composer, it automatically inherits the permissions of the Composer Worker Service Account, significantly improving the security posture.
*   **Parameterized Query Execution:** To prevent SQL injection and ensure type safety, the two positional parameters (`fachl_name1` and `fachl_name2`) are passed to the BigQuery stored procedure using `bigquery.ScalarQueryParameter` bindings rather than raw string interpolation.
*   **Environment-Driven Configuration:** Infrastructure-level configurations (GCP Project, Dataset, and Location) are externalized as environment variables (`GCP_PROJECT`, `BQ_DATASET`, `BQ_LOCATION`), allowing the same script to run unmodified across Development, UAT, and Production environments.

---

## 4. Manual Steps Before Go-Live

Before executing the migrated script in a production environment, the following manual setup steps must be completed:

### A. BigQuery Dataset & Stored Procedure Creation
1.  Ensure the target BigQuery dataset (defined by the `BQ_DATASET` environment variable) exists in your target GCP project and region.
2.  **Recreate the Stored Procedure:** Since the original Oracle stored procedure logic was contained in the missing `d_call_sp_template.sql` file, database developers must manually translate the Oracle PL/SQL logic into BigQuery SQL and deploy it.
    *   The procedure must be named `d_call_sp_template`.
    *   It must accept exactly two `STRING` input parameters.
    *   *Example DDL:*
        ```sql
        CREATE OR REPLACE PROCEDURE `your_project.your_dataset.d_call_sp_template`(param1 STRING, param2 STRING)
        BEGIN
          -- Migrated business logic goes here
          SELECT FORMAT("Executing procedure with params: %s, %s", param1, param2);
        END;
        ```

### B. IAM & Permissions
The service account executing the Python script (e.g., the Cloud Composer environment's service account or a local runner service account) must be granted the following IAM roles on the target BigQuery dataset:
*   **BigQuery Job User** (`roles/bigquery.jobUser`) at the project level to run query jobs.
*   **BigQuery Data Editor** (`roles/bigquery.dataEditor`) or a custom role containing `bigquery.routines.get` and execution permissions on the specific dataset to run the stored procedure.

### C. Environment Variables
Ensure the following environment variables are configured in the execution environment (e.g., Airflow Environment Variables, Kubernetes ConfigMaps, or system environment):
*   `GCP_PROJECT`: The target Google Cloud Project ID.
*   `BQ_DATASET`: The target BigQuery dataset containing the stored procedure.
*   `BQ_LOCATION`: The geographic location of the dataset (e.g., `EU` or `US`). Defaults to `EU` if not set.

### D. Scheduling Integration
If orchestrating via Cloud Composer (Airflow), integrate the script using a `BashOperator` or convert the execution to a native `BigQueryInsertJobOperator` pointing to the SQL statement: `CALL your_dataset.d_call_sp_template('val1', 'val2')`.

---

## 5. Known Gaps & Unresolved References

*   **Missing Source SQL (`d_call_sp_template.sql`):** The legacy SQL script containing the actual Oracle stored procedure call and its underlying PL/SQL definition was not provided in the source codebase. 
*   **Redesign (B4) Follow-up:** 
    *   The exact data types of the two parameters (`fachl_name1`, `fachl_name2`) are assumed to be `STRING` (mapped to BigQuery `STRING`). If the migrated BigQuery stored procedure expects different types (e.g., `INT64`, `TIMESTAMP`), the `ScalarQueryParameter` type definitions in `d_call_sp_template.py` must be updated accordingly.
    *   Any session-level configurations previously handled by the legacy `.dwh_init` script (such as timezone settings or default schemas) must be verified and, if necessary, explicitly set within the BigQuery stored procedure body.

---

## 6. Validation

To validate the migration, perform the following test execution:

### A. Execution Command
Run the Python script from a terminal where GCP credentials are authenticated:

```bash
# 1. Set the required environment variables
export GCP_PROJECT="your-gcp-project-id"
export BQ_DATASET="your_target_dataset"
export BQ_LOCATION="EU"

# 2. Execute the script with test arguments
python3 ksh_unsupported/d_call_sp_template.py "test_value_1" "test_value_2"
```

### B. Expected "Passing" Criteria
The execution is considered successful if:
1.  The script exits with status code `0`.
2.  The console output matches the following sequence:
    ```text
    Starting execution of stored procedure `your-gcp-project-id.your_target_dataset.d_call_sp_template`...
    Stored procedure execution completed successfully in BigQuery.
    ```
3.  The Google Cloud Console **BigQuery Job History** shows a successful `QUERY` job with the statement:
    ```sql
    CALL `your-gcp-project-id.your_target_dataset.d_call_sp_template`(@param1, @param2);
    ```
4.  No exceptions or traceback errors (e.g., `GoogleCloudError`, `DefaultCredentialsError`) are printed to `stderr`.

---

## 7. Rollback Procedure

In the event of an unexpected failure or data anomaly during go-live, execute the following rollback steps:

1.  **Halt Orchestration:** Pause the newly deployed Cloud Composer DAG or scheduler task pointing to `d_call_sp_template.py`.
2.  **Revert Scheduler:** Re-enable the legacy UC4/Automic job pointing to the legacy KornShell script `d_call_sp_template.ksh`.
3.  **Verify Legacy Environment:** Ensure that the legacy Oracle database, the `d_call_sp_template.sql` script, and the `.dwh_init` configuration file remain intact and have not been decommissioned.
4.  **Data Reconciliation:** If the BigQuery stored procedure partially executed and modified target tables, run manual cleanup queries in BigQuery to restore the tables to their pre-execution state, or rely on BigQuery Time Travel to restore tables if necessary:
    ```sql
    -- Example of restoring a table to its state 1 hour ago
    CREATE OR REPLACE TABLE `my_project.my_dataset.my_table`
    AS SELECT * FROM `my_project.my_dataset.my_table`
    FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
    ```