# MIGRATION NOTES

**Source Component:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.ksh`  
**Target Platform:** Google Cloud BigQuery  
**Migration Date:** October 2023  

---

## 1. SUMMARY

The legacy KornShell (`KSH`) utility script `h_alis_sqlplus.ksh` has been migrated to a cloud-native architecture on **Google Cloud BigQuery**. 

In the legacy environment, this script defined the `starteSQLSkript` function, which wrapped Oracle SQL\*Plus execution. It performed parameter validation, verified local filesystem script readability, managed shell-level error handling (`set +e`/`set -e`), executed the script using the `${DW_ORAUSER}` connection, and returned the execution status.

To support modern, cloud-native orchestration patterns, the utility has been refactored into two alternative target patterns:
1. **Option A (Python Orchestration Helper):** A Python module designed for Apache Airflow (Cloud Composer) or custom Python runners. It replaces local filesystem checks with Google Cloud Storage (GCS) checks and SQL\*Plus execution with BigQuery API calls.
2. **Option B (BigQuery Native Stored Procedure & Registry):** A pure GoogleSQL implementation using a metadata-driven script registry table and dynamic SQL (`EXECUTE IMMEDIATE`) to execute scripts directly inside BigQuery.

---

## 2. GENERATED ARTIFACTS

The migration process generated the following files:

| Artifact Path | Role / Description |
| :--- | :--- |
| `dags/utils/h_alis_sqlplus.py` | **Python Orchestration Utility (Option A):** Contains Python functions to validate inputs, read SQL files from GCS or local paths, substitute positional parameters, and execute queries via the BigQuery client. |
| `ddl/procedures/starteSQLSkript.sql` | **BigQuery Native Helper (Option B):** DDL script that creates the metadata registry table, the audit logging table, parameter rendering helpers, and the main `starteSQLSkript` stored procedure. |

---

## 3. KEY DESIGN DECISIONS

### Dual-Pattern Architecture (Python vs. Native SQL)
* **Decision:** Provide both a Python module and a BigQuery stored procedure.
* **Reasoning:** This accommodates different enterprise orchestration strategies. Teams using Cloud Composer (Airflow) can leverage the Python module to manage execution state outside the database. Teams preferring database-centric execution can use the stored procedure.

### Metadata-Driven Script Registry
* **Decision:** Replace physical filesystem SQL scripts with a BigQuery table-backed registry (`sql_script_registry`) or GCS buckets.
* **Reasoning:** BigQuery cannot access local application server filesystems. Storing SQL templates in a database table or GCS bucket decouples the execution engine from physical infrastructure and enables dynamic SQL execution.

### Positional Parameter Substitution
* **Decision:** Emulate Oracle SQL\*Plus positional parameters (e.g., `&1`, `&2`) using `${1}`, `${2}` placeholders, resolved via string replacement.
* **Reasoning:** Legacy scripts heavily rely on positional arguments passed via the command line. This substitution mechanism allows migrated SQL scripts to retain their structure without requiring immediate, high-risk refactoring of their internal logic.

### Preservation of Legacy Return Codes
* **Decision:** Explicitly return legacy exit codes (`196` for missing arguments, `201` for script not found, `1` for execution failure, `0` for success).
* **Reasoning:** Upstream orchestrators (such as UC4/Automic) rely on these specific exit codes to determine job success or failure. Preserving them minimizes changes required in the scheduling layer.

---

## 4. MANUAL STEPS BEFORE GO-LIVE

To deploy and configure the migrated utility, complete the following manual steps:

### 1. Schema and Dataset Creation
Ensure the target BigQuery datasets exist in your GCP project:
* `dw_utility_dev` (or `dw_utility_prod`): For utility procedures and the script registry.
* `dw_audit_dev` (or `dw_audit_prod`): For execution audit logs.

### 2. IAM & Permissions
Assign the following IAM roles to the Service Account executing the Airflow DAGs or running the BigQuery jobs:
* **BigQuery:** `roles/bigquery.dataEditor` and `roles/bigquery.jobUser` on the target datasets.
* **Cloud Storage (if using Option A with GCS):** `roles/storage.objectViewer` on the bucket containing the SQL scripts.

### 3. Database Objects Deployment
Execute the DDL script `ddl/procedures/starteSQLSkript.sql` in your target BigQuery environment to create:
* `sql_script_registry` (Table)
* `sql_execution_audit` (Table)
* Helper procedures (`log_execution_event`, `validate_sql_request`, `resolve_sql_script`, `render_sql_parameters`)
* `starteSQLSkript` (Stored Procedure)

### 4. Environment Variables Configuration
Configure the following environment variables in your execution environment (e.g., Airflow Webserver, Cloud Run, or local shell):
* `GCP_PROJECT_ID`: The target GCP project ID.
* `BQ_UTILITY_DATASET`: Dataset where the registry and procedures reside.
* `BQ_LOGGING_DATASET`: Dataset where the audit table resides.
* `GCS_SQL_BUCKET`: GCS bucket name (if storing SQL scripts in GCS).

### 5. Seed the Script Registry
Populate the `sql_script_registry` table with the SQL statements from your migrated scripts. For example:
```sql
INSERT INTO `gcp-is-dw-dev.dw_utility_dev.sql_script_registry` (script_name, script_sql, is_active, last_modified)
VALUES ('my_migrated_report', 'SELECT * FROM `my_project.my_dataset.orders` WHERE status = \'${1}\'', TRUE, CURRENT_TIMESTAMP());
```

---

## 5. KNOWN GAPS & UNRESOLVED REFERENCES

### 1. Oracle Dialect Conversion (Downstream Scripts)
* **Gap:** This utility executes SQL scripts, but it does *not* automatically translate Oracle SQL syntax to BigQuery standard SQL (GoogleSQL).
* **Mitigation:** All downstream `.sql` files called by this utility must be migrated to GoogleSQL before being registered in the `sql_script_registry` or uploaded to GCS.

### 2. Dynamic SQL Limitations
* **Gap:** BigQuery's `EXECUTE IMMEDIATE` statement has limits on query size and complexity. Extremely large, multi-statement legacy scripts may fail to execute dynamically.
* **Mitigation:** Break down exceptionally large legacy scripts into smaller, modular stored procedures, and register them as individual steps.

### 3. Redesign (B4) Recommendation: Named Parameters
* **Gap:** Positional parameter substitution (`${1}`) is prone to ordering errors.
* **Mitigation:** For long-term maintainability, transition from generic positional scripts to strongly-typed, named-parameter stored procedures (e.g., using the generated `starteSQLSkript_named` wrapper as a reference).

---

## 6. VALIDATION

To validate the migration, run the following test cases:

### Test Case 1: Missing Parameters (Expected Return Code: 196)
* **Action:** Call the stored procedure with a `NULL` script name.
  ```sql
  CALL `gcp-is-dw-dev.dw_utility_dev.starteSQLSkript`(1001, NULL, []);
  ```
* **Pass Criteria:** The procedure returns `196` and writes an error log to `sql_execution_audit`.

### Test Case 2: Script Not Found (Expected Return Code: 201)
* **Action:** Call the stored procedure with a non-existent script name.
  ```sql
  CALL `gcp-is-dw-dev.dw_utility_dev.starteSQLSkript`(1002, 'non_existent_script', []);
  ```
* **Pass Criteria:** The procedure returns `201` and writes an error log to `sql_execution_audit`.

### Test Case 3: Successful Execution (Expected Return Code: 0)
* **Action:** Call the stored procedure with a valid registered script and parameters.
  ```sql
  CALL `gcp-is-dw-dev.dw_utility_dev.starteSQLSkript`(1003, 'example_script', ['param_value_1', 'param_value_2']);
  ```
* **Pass Criteria:** The procedure returns `0`, executes the underlying SQL, and writes an `INFO` log to `sql_execution_audit`.

### Test Case 4: Python Unit Tests
* **Action:** Run the Python test suite using `pytest` (with mocked BigQuery/GCS clients).
* **Pass Criteria:** All mock assertions for file reading, parameter substitution, and query execution pass.

---

## 7. ROLLBACK PROCEDURE

If a critical issue is discovered post-go-live, execute the following rollback steps:

1. **Redirect Orchestration:** Revert the calling tasks in your orchestrator (e.g., UC4, Airflow) to execute the legacy KornShell script (`h_alis_sqlplus.ksh`) pointing to the Oracle database.
2. **Disable BigQuery Execution:** If necessary, mark the scripts as inactive in the registry table to prevent accidental execution:
   ```sql
   UPDATE `gcp-is-dw-dev.dw_utility_dev.sql_script_registry`
   SET is_active = FALSE
   WHERE script_name = 'your_script_name';
   ```
3. **Preserve Logs:** Do not delete the `sql_execution_audit` table, as it contains valuable troubleshooting logs leading up to the rollback.