# MIGRATION_NOTES.md — Job: `d_ipis_loader.ksh`

This document outlines the migration details, design decisions, manual steps, and validation procedures for transitioning the legacy Oracle SQL\*Loader wrapper utility `d_ipis_loader.ksh` to Google Cloud Platform (GCP) BigQuery.

---

## 1. Summary

The legacy shell script `d_ipis_loader.ksh` has been migrated from an on-premises Oracle environment to **Google Cloud BigQuery**. 

* **Source Component:** `vobs/dw_source/isdwh/import/is/bin/d_ipis_loader.ksh` (Oracle SQL\*Loader wrapper script)
* **Target Component:** `gcp_migration/import/is/stored_procedures/d_ipis_loader.sql` (BigQuery SQL Stored Procedure)
* **Target Platform:** Google Cloud Platform (GCP) / BigQuery

### Functional Overview
The legacy script parsed command-line parameters, validated inputs, generated log/discard/bad file paths, executed Oracle `sqlldr`, and checked for the existence of `.bad` or `.dis` files to determine load success. The migrated architecture replaces local file systems and the SQL\*Loader engine with Google Cloud Storage (GCS) and BigQuery's native `LOAD DATA` DDL statement, wrapped in a robust, transaction-managed stored procedure.

---

## 2. Generated Artifacts

The migration process has generated the following SQL files and structures:

| File Path | Role / Description |
| :--- | :--- |
| `gcp_migration/import/is/stored_procedures/d_ipis_loader.sql` | **Core Migrated Stored Procedure:** Replicates the parameter validation, control-file-to-table mapping, transactional loading, and error handling of the legacy script. |
| **Audit Table Definitions** (Included in SQL) | **`dw_execution_log`**: Captures informational logs and successful load metadata.<br>**`dw_error_log`**: Captures detailed error logs, error codes, and failed SQL statements. |
| **Helper Procedures** (Included in SQL) | **`sp_log_info`**: Reusable procedure to write standard execution logs.<br>**`sp_log_error`**: Reusable procedure to write detailed error logs. |

---

## 3. Key Design Decisions

### Native `LOAD DATA` vs. External Tables
* **Decision:** Used BigQuery's native `LOAD DATA OVERWRITE` statement inside an `EXECUTE IMMEDIATE` block.
* **Reasoning:** This approach mimics the high-performance, direct-loading behavior of SQL\*Loader. It avoids the overhead of maintaining permanent external table definitions for transient source files.

### Transactional Integrity & Error Handling
* **Decision:** Wrapped the dynamic `LOAD DATA` execution inside a BigQuery `BEGIN TRANSACTION ... COMMIT TRANSACTION` block, enclosed by a `BEGIN ... EXCEPTION WHEN ERROR ... END` block.
* **Reasoning:** If a load fails (e.g., due to malformed rows exceeding `max_bad_records`), the transaction is rolled back, preventing partial or corrupted data loads. The exception block catches the failure, assigns the legacy-equivalent error code `200`, and writes the exact system error message (`@@error.message`) and failing statement (`@@error.statement`) to the audit log.

### Decoupled Logging Helpers
* **Decision:** Created standalone logging procedures (`sp_log_info`, `sp_log_error`).
* **Reasoning:** This modularizes the logging logic, keeping the main loader procedure clean and allowing other migrated loader jobs to reuse the same logging infrastructure.

### Trade-offs
* **Strict Schema Enforcement:** By setting `max_bad_records = 0`, any structural mismatch will fail the entire load. This matches the legacy script's behavior of raising an error when a `.bad` file is generated. If a more lenient approach is required in the future, `max_bad_records` can be parameterized.

---

## 4. Manual Steps Before Go-Live

Before executing the migrated stored procedure in a production environment, the following setup steps must be completed:

### 1. Schema & Dataset Creation
Ensure the target dataset exists in your target GCP project:
```sql
CREATE SCHEMA IF NOT EXISTS `your_project.your_dataset`;
```
Deploy the target tables (e.g., `t_customer`, `t_orders`) with schemas matching the structure of the incoming flat files.

### 2. IAM & Permissions
The service account or user executing the stored procedure must have the following IAM roles:
* **BigQuery:** `roles/bigquery.admin` or a combination of `roles/bigquery.dataEditor` and `roles/bigquery.jobUser`.
* **Cloud Storage:** `roles/storage.objectViewer` on the GCS bucket containing the source data files.

### 3. Secrets & Connection Strings
Unlike the legacy Oracle script, no database passwords (`-p`) are passed or stored. Authentication is handled natively via GCP IAM. Ensure that any orchestration tool (e.g., Cloud Composer/Airflow) uses the authorized service account.

### 4. Scheduling & Orchestration
If migrating from Cron or Control-M:
* Replace the shell execution call with a BigQuery job execution call using the `bq` command-line tool, Cloud Composer (Airflow `BigQueryInsertJobOperator`), or Workflows.
* **Example CLI Call:**
  ```bash
  bq query --use_legacy_sql=false \
    "CALL \`your_project.your_dataset.d_ipis_loader\`('customer_ctrl', 'gs://your-bucket/import/customer_data.csv', 'DE', NULL)"
  ```

---

## 5. Known Gaps & Unresolved References

### 1. Control File Mapping (B4 Redesign Item)
* **Gap:** The legacy script accepted a control file path (`-c`). In the migrated stored procedure, a `CASE` statement maps control file name patterns (e.g., `%customer%`, `%orders%`) to hardcoded target tables.
* **Redesign Action:** As new tables are migrated, developers must manually update the `CASE` statement in `d_ipis_loader.sql` to map new control file patterns to their respective BigQuery target tables. Alternatively, a metadata mapping table can be introduced to resolve mappings dynamically.

### 2. Custom CSV Formats
* **Gap:** The `LOAD DATA` statement currently uses a hardcoded semicolon delimiter (`;`), double-quote character (`"`), and skips 1 header row.
* **Redesign Action:** If different source files use different delimiters or formatting options, these parameters should be externalized into a metadata table or passed as additional arguments to the stored procedure.

---

## 6. Validation

To validate the migration, execute the test cases below and verify the output in the logging tables.

### Test Case 1: Successful Load
1. Upload a valid CSV file to your GCS bucket (e.g., `gs://your-bucket/test/customer_valid.csv`).
2. Execute the procedure:
   ```sql
   DECLARE v_err INT64;
   CALL `your_project.your_dataset.d_ipis_loader`('customer_ctrl', 'gs://your-bucket/test/customer_valid.csv', 'EN', v_err);
   SELECT v_err AS return_code;
   ```
3. **Passing Criteria:** 
   * `return_code` is `0`.
   * Data is populated in `your_project.your_dataset.t_customer`.
   * A success entry is written to `your_project.your_dataset.dw_execution_log`.

### Test Case 2: Invalid File / Parsing Failure (Bad Records)
1. Upload a corrupted CSV file containing mismatched columns or invalid data types to GCS.
2. Execute the procedure pointing to the bad file.
3. **Passing Criteria:**
   * `return_code` is `200`.
   * Target table remains unchanged (transaction rolled back).
   * A detailed failure entry is written to `your_project.your_dataset.dw_error_log` containing the BigQuery parser error message.

---

## 7. Rollback Procedure

In the event of an issue during deployment or go-live, follow these rollback steps:

1. **Drop Migrated Procedures:**
   ```sql
   DROP PROCEDURE IF EXISTS `your_project.your_dataset.d_ipis_loader`;
   DROP PROCEDURE IF EXISTS `your_project.your_dataset.sp_log_info`;
   DROP PROCEDURE IF EXISTS `your_project.your_dataset.sp_log_error`;
   ```
2. **Revert Orchestration:**
   Point your scheduling tool (e.g., Control-M, Cron) back to the legacy on-premises execution path running the `d_ipis_loader.ksh` shell script.
3. **Verify Oracle State:**
   Ensure the legacy Oracle database and local file systems are clean and ready to resume processing.