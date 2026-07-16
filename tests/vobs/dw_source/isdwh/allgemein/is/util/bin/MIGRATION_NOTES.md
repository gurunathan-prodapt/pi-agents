# Migration Notes: Shared Files — `vobs/dw_source/isdwh/allgemein/is/util/bin`

These migration notes document the transition of the core Data Warehouse (DWH) logging, parameter validation, and database execution helper libraries from legacy KornShell (`ksh`) and Oracle SQL*Plus to native Google Cloud BigQuery Stored Procedures and User Defined Functions (UDFs).

---

## 1. Summary

The legacy shell scripts and Oracle SQL*Plus wrappers located in `vobs/dw_source/isdwh/allgemein/is/util/bin` have been migrated to **Google Cloud BigQuery**. 

*   **Source Platform**: KornShell (Ksh) scripting, Oracle SQL*Plus, and Oracle PL/SQL packages (`DWPA_MELDUNG`, `DWH$VS_MELDUNG`).
*   **Target Platform**: Google Cloud BigQuery (SQL Procedural Layer).
*   **Migrated Components**:
    *   `f_alis_msgerr.ksh` (Logging, error trapping, and status management).
    *   `h_alis_sqlplus.ksh` (SQL*Plus execution wrappers, connection validation, and session metadata).
    *   `h_alis_parameter.ksh` (Parameter validation, metric code mapping, and date span calculations).

---

## 2. Generated Artifacts

The migration produces three primary SQL files containing BigQuery Stored Procedures and User Defined Functions (UDFs). These files must be deployed to the target GCP project and dataset.

| Target File Path | Type | Role / Description |
| :--- | :--- | :--- |
| `stored_procedures/f_alis_msgerr.sql` | Stored Procedures | Replaces `f_alis_msgerr.ksh`. Implements logging, error trapping (`DWMSG_Fehlerbehandlung`), status updates, and metadata appending. |
| `stored_procedures/h_alis_sqlplus.sql` | Stored Procedures | Replaces `h_alis_sqlplus.ksh`. Implements database connectivity checks (`tryDBConnect`) and dynamic SQL execution wrappers. |
| `stored_procedures/h_alis_parameter.sql` | UDFs & Stored Procedures | Replaces `h_alis_parameter.ksh`. Implements metric/system code lookups, date span calculations, and parameter validation. |

---

## 3. Key Design Decisions

### 3.1. Elimination of Shell Dependencies
*   **Decision**: Convert all shell-level validation and routing logic into native BigQuery Stored Procedures.
*   **Reasoning**: Running shell scripts in a modern cloud data warehouse introduces unnecessary orchestration overhead (e.g., maintaining VM instances or container runtimes just to execute wrapper scripts). Moving this logic to BigQuery allows orchestration tools (like Airflow or Dataform) to invoke validations natively via SQL.

### 3.2. Sequence Generation Replacement
*   **Decision**: Replaced Oracle sequence fetching (`DWMSG_ErmittleNr`) with BigQuery's native `GENERATE_UUID()`.
*   **Reasoning**: BigQuery does not support traditional stateful database sequences. UUIDs provide a globally unique, collision-free tracking identifier suitable for distributed cloud environments.

### 3.3. Encapsulation of Mapping Logic in UDFs
*   **Decision**: Extracted large conditional mapping blocks (e.g., `konvertiereKennzahl` and `konvertiereSDName`) into pure SQL UDFs (`konvertiereKennzahl_lookup`, `konvertiereSDName_lookup`) called by the main procedures.
*   **Reasoning**: Improves code readability, simplifies unit testing, and allows other SQL queries to reuse the mapping logic directly without calling a stored procedure.

### 3.4. Session Metadata and File System Abstraction
*   **Decision**: Oracle's `DBMS_APPLICATION_INFO` and local file writes are abstracted. BigQuery natively tracks execution history in `INFORMATION_SCHEMA.JOBS_BY_*`. File-logging operations (`starteSQLSkriptSilentFile`) are redirected to a structured BigQuery audit table (`dwh.sql_execution_file_logs`).

---

## 4. Manual Steps Before Go-Live

Before deploying the migrated procedures and running jobs, the following setup steps must be completed in the target GCP environment:

### 4.1. Schema & Dataset Creation
Ensure the target dataset exists and contains the logging table migrated from Oracle:
```sql
CREATE SCHEMA IF NOT EXISTS `your_project.your_dataset`;

CREATE TABLE IF NOT EXISTS `your_project.your_dataset.dwh_ta_k_meldungen` (
  entrynr STRING,
  job_kennung STRING,
  programmname STRING,
  log_datei STRING,
  parameter STRING,
  stichtag DATE,
  dateiname STRING,
  status STRING
);

CREATE TABLE IF NOT EXISTS `your_project.your_dataset.sql_execution_file_logs` (
  entrynr STRING,
  script_name STRING,
  directory STRING,
  file_name STRING,
  job_kennung STRING,
  execution_timestamp TIMESTAMP,
  status STRING
);
```

### 4.2. IAM & Permissions
The service account executing these procedures (e.g., the Airflow/Composer worker identity) requires the following IAM roles:
*   `roles/bigquery.dataEditor` on the target dataset.
*   `roles/bigquery.jobUser` on the GCP project.

### 4.3. Connection Strings & Secrets
*   Legacy Oracle connection strings (`user/pass@instance`) are obsolete. 
*   If executing dynamic queries across external databases (e.g., Cloud SQL or Spanner), set up a **BigQuery Connection** resource and grant the service account access.

### 4.4. Scheduling & Environment Variables
Replace legacy environment variables in your orchestrator (e.g., Airflow DAGs) with GCP equivalents:
*   `$DW_DIR_ROOT` $\rightarrow$ Map to a Google Cloud Storage bucket path (e.g., `gs://your-bucket/dwh_root`).
*   `$GCP_PROJECT` $\rightarrow$ Your target Google Cloud Project ID.
*   `$BQ_DATASET` $\rightarrow$ Your target BigQuery dataset (e.g., `dwh`).

---

## 5. Known Gaps & Unresolved References

The following items have been flagged for follow-up or redesign (B4 items):

1.  **Missing Package Procedures**: The procedures invoke downstream package procedures that must be migrated to BigQuery:
    *   `dwpa_meldung__setzestatusok`
    *   `dwpa_meldung__setzestatusabbruch`
    *   `dwpa_meldung__erzeuge_eintrag_p4`
    *   `dwpa_meldung__erzeuge_eintrag_p5`
    *   `dwpa_meldung__fehler`
    *   `dwpa_meldung__setzezusatzinfos`
    *   `dwpa_meldung__logausgabe_info`
    *   `dwh_vs_meldung__logausgabe_debug`
2.  **Downstream Pipeline Integration**: Downstream consumers (`DW.DWH_ABPZ_KKM_AIL_AGENT` and `r_ai_start`) are not yet migrated. Their orchestration wrappers must be updated to call these BigQuery procedures instead of the legacy `.ksh` files.
3.  **Dynamic SQL Execution**: `starteSQLSkript` uses `EXECUTE IMMEDIATE` to run dynamic SQL statements. Ensure that any dynamic SQL passed to this procedure is sanitized to prevent SQL injection risks.

---

## 6. Validation

To validate the deployment, execute the following test cases in the BigQuery console.

### 6.1. Test Case 1: Parameter Validation
Verify that parameter validation correctly flags missing parameters.
```sql
DECLARE v_err_nr INT64 DEFAULT 0;
DECLARE v_err_arg STRING DEFAULT '';

CALL `your_project.your_dataset.pruefeParameterGesetzt`('TestParam', '', v_err_nr, v_err_arg);

-- Expected: v_err_nr = 194, v_err_arg = 'TestParam'
SELECT v_err_nr AS error_number, v_err_arg AS error_argument;
```

### 6.2. Test Case 2: Metric Code Conversion
Verify that metric codes are mapped correctly.
```sql
DECLARE v_kennzahl STRING DEFAULT 'abgang';
DECLARE v_err_nr INT64 DEFAULT 0;
DECLARE v_err_arg STRING DEFAULT '';

CALL `your_project.your_dataset.konvertiereKennzahl`(v_kennzahl, v_err_nr, v_err_arg);

-- Expected: v_kennzahl = 'abg', v_err_nr = 0
SELECT v_kennzahl AS mapped_metric, v_err_nr AS error_number;
```

### 6.3. Test Case 3: Error Trapping & Logging
Verify that the error handler updates the tracking table.
```sql
-- Insert a mock tracking record
INSERT INTO `your_project.your_dataset.dwh_ta_k_meldungen` (entrynr, status)
VALUES ('test-uuid-1234', 'RUNNING');

-- Trigger error handler
CALL `your_project.your_dataset.DWMSG_Fehlerbehandlung`('test-uuid-1234', 500);

-- Expected: Status should be updated to 'ABORTED' (via dwpa_meldung__setzestatusabbruch mock/real)
-- and an error entry logged.
```

---

## 7. Rollback Procedure

In the event of an issue during go-live, follow these steps to roll back:

1.  **Revert Orchestration**: Point your orchestrator (e.g., Airflow, UC4) back to the legacy KornShell scripts (`f_alis_msgerr.ksh`, `h_alis_sqlplus.ksh`, `h_alis_parameter.ksh`).
2.  **Keep BigQuery Assets**: Do not immediately delete the BigQuery stored procedures, as downstream systems may still be referencing them in hybrid environments.
3.  **Audit Logs**: Check the `dwh_ta_k_meldungen` and `sql_execution_file_logs` tables to identify the exact parameters or queries that caused the failure.