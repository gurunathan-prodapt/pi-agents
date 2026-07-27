# Migration Notes: Shared Files — sql_bqsql_linked_job/isbert/allgemein/is/util/bin

## 1. Summary
This document details the migration of the legacy KornShell utility library `h_alis_sqlplus.ksh` to Python 3. 

* **Source Artifact:** `h_alis_sqlplus.ksh` (KornShell utility library)
* **Target Artifact:** `h_alis_sqlplus.py` (Python 3 script and importable module)
* **Target Platform:** Google Cloud Platform (GCP) / Cloud Composer (Apache Airflow)
* **Primary Database Target:** Google BigQuery (with a legacy Oracle SQL*Plus execution fallback)

The primary purpose of this utility is to validate, log, and execute SQL scripts. In the migrated architecture, it acts as a bridge, allowing legacy SQL scripts to be executed directly against Google BigQuery while maintaining compatibility with existing orchestration patterns and error-reporting frameworks.

---

## 2. Generated Artifacts
The migration process produced a single, unified Python module that preserves the directory structure of the legacy environment (Folder Integrity Rule):

* **File Path:** `sql_bqsql_linked_job/isbert/allgemein/is/util/bin/h_alis_sqlplus.py`
* **Role:** 
  * Provides the core function `starte_sql_skript(p_eintragsnr, p_skript, *args)` for programmatic import by other Python tasks or Airflow DAGs.
  * Provides a Command Line Interface (CLI) via `main()` to allow direct execution from shell-based orchestrators or container entry points.
  * Implements dual-mode execution (BigQuery API vs. legacy Oracle `sqlplus` subprocess).
  * Emulates SQL*Plus positional parameter substitution (`&1`, `&2`) and strips incompatible SQL*Plus formatting directives.

---

## 3. Key Design Decisions

### Dual-Mode Execution (BigQuery & SQL*Plus)
To support a phased migration, the Python script dynamically switches execution modes based on environment variables:
* **BigQuery Mode (Default):** Triggered when `USE_BIGQUERY` is set to `"True"` (or left default) and `DW_ORAUSER` is empty. It executes queries using the native `google-cloud-bigquery` client library.
* **Legacy Fallback Mode:** Triggered when `USE_BIGQUERY` is set to `"False"` or `DW_ORAUSER` is populated. It invokes the local `sqlplus` binary via `subprocess.run`, mimicking the exact behavior of the legacy shell script.

### SQL*Plus Emulation for BigQuery
To minimize the immediate need for manual SQL refactoring (Class B4 Redesign), the Python script performs lightweight preprocessing on SQL files before sending them to BigQuery:
1. **Parameter Substitution:** Replaces positional placeholders (e.g., `&1`, `&&1`, `&2`) with the variadic arguments passed to the function.
2. **Directive Stripping:** Automatically filters out SQL*Plus session configuration commands (e.g., `SET`, `WHENEVER`, `EXIT`, `COLUMN`, `SPOOL`, `PROMPT`) which would otherwise trigger syntax errors in BigQuery.

### Error Handling & Log Preservation
* **German Literal Preservation:** Console outputs (e.g., `"Rufe SQL*PLUS auf mit folgenden Einstellungen"`) have been preserved verbatim to ensure that any legacy log-scraping tools or operators continue to function without modification.
* **Validation Codes:** Legacy exit codes `196` (missing parameters) and `201` (unreadable script file) are strictly maintained.
* **Graceful Error Reporter Fallback:** The script attempts to call the legacy enterprise error reporter `DWMSG_MeldeFehler` via `subprocess`. If the executable is missing (common in local development or clean GCP environments), it catches the `FileNotFoundError`, logs a warning to `stderr`, and continues execution instead of crashing.

---

## 4. Manual Steps Before Go-Live

### Schema & Dataset Creation
Ensure that all BigQuery datasets referenced inside the SQL scripts executed by this utility are pre-created in the target GCP project.

### IAM & Permissions
The service account executing the Python script (e.g., the Cloud Composer worker service account) must be granted the following IAM roles:
* **`roles/bigquery.jobUser`** (to run query jobs)
* **`roles/bigquery.dataViewer`** / **`roles/bigquery.dataEditor`** (on the target datasets, depending on whether the SQL scripts perform DDL/DML)

### Environment Variables
Configure the following environment variables in your execution environment (e.g., Airflow Environment Variables or container environment):

| Variable Name | Expected Value | Description |
| :--- | :--- | :--- |
| `GCP_PROJECT` | `your-gcp-project-id` | The target Google Cloud Project. |
| `BQ_LOCATION` | `EU` or `US` | The geographic location of your BigQuery datasets. |
| `USE_BIGQUERY` | `True` | Set to `True` for BigQuery execution; `False` to force legacy Oracle execution. |
| `DW_ORAUSER` | *[Optional]* | Oracle connection string. Keep empty for BigQuery mode. |

### Secrets Management
If running in legacy fallback mode, do **not** hardcode credentials in `DW_ORAUSER`. Retrieve the connection string dynamically from Google Secret Manager or Airflow Connections at runtime.

### Scheduling & Packaging
Because this is a shared utility library:
1. Ensure the directory containing `h_alis_sqlplus.py` is added to the Python path (`PYTHONPATH`) of your execution environment.
2. If using Cloud Composer, upload this file to the `/dags` or `/plugins` folder structure so it can be imported by parent DAGs.

---

## 5. Known Gaps & Unresolved References

### Unresolved Component: `DWMSG_MeldeFehler`
* **Status:** **SOURCE NOT FOUND**
* **Impact:** The script will log a warning to `stderr` and print the error details to standard error output if this executable is missing.
* **Redesign Recommendation (B4):** Replace the `_dwmsg_melde_fehler` subprocess call with a native Python logging handler that writes structured JSON logs to Google Cloud Logging (Stackdriver), or trigger Airflow alerts/callbacks directly.

### SQL Compatibility (Class B4 Redesign)
* **Status:** **POTENTIAL RUNTIME RISK**
* **Impact:** While basic SQL*Plus directives are stripped, complex Oracle-specific SQL features (e.g., PL/SQL blocks, `MERGE` statements with Oracle-specific syntax, proprietary functions like `DECODE` or `NVL` in complex contexts) cannot be parsed by BigQuery.
* **Mitigation:** Every SQL script passed to this utility must be manually reviewed and refactored to standard BigQuery SQL (BQSQL) before switching `USE_BIGQUERY` to `True`.

### Downstream Migration Dependencies
* **Status:** **PENDING**
* **Impact:** Downstream consumers `DW.BERT_AUSD_V_TA_PERIOD` and `abinitio_rpos_carmen_linked_job/isdwh/abinitio/bin/r_ai_start` are not yet migrated.
* **Mitigation:** Cross-DAG execution links or Airflow sensors must be established once these downstream jobs are migrated to GCP.

---

## 6. Validation

To validate the migration, execute the following test cases in your target environment.

### Test Case 1: Parameter Validation (Error 196)
* **Command:**
  ```bash
  python3 h_alis_sqlplus.py "" ""
  ```
* **Expected Output:**
  * Exit code: `196`
  * Console Output: Logs a warning that `DWMSG_MeldeFehler` was not found (if applicable) and outputs the error details for code `196`.

### Test Case 2: File Readability Validation (Error 201)
* **Command:**
  ```bash
  python3 h_alis_sqlplus.py "1001" "/non_existent_path/script.sql"
  ```
* **Expected Output:**
  * Exit code: `201`
  * Console Output: Outputs the error details for code `201` referencing the invalid path.

### Test Case 3: BigQuery Execution with Parameter Substitution
1. Create a temporary SQL file named `test_query.sql` with the following content:
   ```sql
   -- This is a test query
   SET PAGESIZE 100;
   SELECT '&1' as parameter_one, '&2' as parameter_two;
   ```
2. Execute the script in BigQuery mode:
   ```bash
   export GCP_PROJECT="your-gcp-project-id"
   export BQ_LOCATION="EU"
   export USE_BIGQUERY="True"
   python3 h_alis_sqlplus.py "1002" "test_query.sql" "ValueA" "ValueB"
   ```
* **Expected Output:**
  * Exit code: `0`
  * Console Output:
    ```text
    Rufe SQL*PLUS auf mit folgenden Einstellungen
    Sql*Plus-Skript : test_query.sql
    Skript-Parameter: ValueA ValueB
    ```
  * BigQuery Job History: A successful dry-run/execution of the query:
    ```sql
    -- This is a test query
    SELECT 'ValueA' as parameter_one, 'ValueB' as parameter_two;
    ```

---

## 7. Rollback Procedure

If issues are encountered with BigQuery execution during go-live, follow these steps to roll back to the legacy Oracle execution path:

1. **Toggle Execution Mode:**
   Set the environment variable `USE_BIGQUERY` to `"False"`.
2. **Provide Oracle Credentials:**
   Ensure the `DW_ORAUSER` environment variable is populated with the valid Oracle connection string.
3. **Verify Client Availability:**
   Ensure that the Oracle Instant Client and the `sqlplus` binary are installed and available in the `$PATH` of the execution environment (e.g., on the Cloud Composer worker nodes).
4. **Verify Code Integrity:**
   If a complete code rollback is required, revert the calling orchestrator tasks to source the original `h_alis_sqlplus.ksh` file instead of executing the Python module.