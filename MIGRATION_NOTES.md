# Migration Notes: `f_alis_msgerr.ksh` Utility Library Migration to BigQuery

## 1. Summary

The legacy KornShell utility library `f_alis_msgerr.ksh` has been migrated to **Google BigQuery**. 

In the legacy Oracle-based environment, this library provided standardized error handling, database-backed logging, and status tracking by wrapping dynamic `sqlplus` calls to the Oracle stored procedure package `BERT_MELDUNG`. 

To align with modern cloud-native patterns on Google Cloud Platform (GCP), the utility functions have been redesigned as **BigQuery Stored Procedures**. Execution states, logging headers, and error details are now persisted directly in partitioned and clustered BigQuery tables, while shell-level execution states and traps are shifted to modern orchestration layers (such as Cloud Composer/Apache Airflow or BigQuery Scripting).

---

## 2. Generated Artifacts

The migration produces the following database tables and stored procedures within the target BigQuery dataset (recommended: `is_reporting_ds`):

### DDL Tables
*   **`tables/message_entry_sequence.sql`**
    *   *Role*: Emulates an Oracle sequence. It stores and increments the transaction key (`next_value`) for tracking job runs.
*   **`tables/message_table.sql`**
    *   *Role*: Replaces the core legacy tracking table. Stores execution headers, job names, log file paths, execution statuses (`OPEN`, `OK`, `ABBRUCH`), and stichtag metadata.
*   **`tables/message_errors.sql`**
    *   *Role*: Stores detailed error records, warnings, and fatal exceptions linked to a specific execution header.

### Internal Helper Procedures
*   **`procedures/sp_ensure_sequence_row.sql`**
    *   *Role*: Ensures that the sequence tracking row exists for a given control key, initializing it if missing.
*   **`procedures/sp_next_sequence_value.sql`**
    *   *Role*: Atomically increments and retrieves the next sequential ID from `message_entry_sequence`.
*   **`procedures/sp_update_message_status.sql`**
    *   *Role*: Updates the status field and timestamp of a specific run in `message_table`.
*   **`procedures/sp_insert_message_error.sql`**
    *   *Role*: Inserts a detailed error log entry into `message_errors`.
*   **`procedures/sp_set_message_additional_info.sql`**
    *   *Role*: Updates metadata fields (`stichtag` and `zusatzinfos`) in `message_table`.

### Public Interface Procedures (Direct Legacy Replacements)
*   **`procedures/DWMSG_ErmittleNr.sql`**
    *   *Role*: Replaces `DWMSG_ErmittleNr()`. Retrieves a unique numeric transaction ID.
*   **`procedures/DWMSG_ErzeugeEintrag.sql`**
    *   *Role*: Replaces `DWMSG_ErzeugeEintrag()`. Creates an initial execution record with status `OPEN`.
*   **`procedures/DWMSG_MeldeFehler.sql`**
    *   *Role*: Replaces `DWMSG_MeldeFehler()`. Appends explicit error logs (Fatal, Error, Warning) to the run.
*   **`procedures/DWMSG_SetzeStatusOK.sql`**
    *   *Role*: Replaces `DWMSG_SetzeStatusOK()`. Transitions the job status to `OK`.
*   **`procedures/DWMSG_SetzeStatusAbbruch.sql`**
    *   *Role*: Replaces `DWMSG_SetzeStatusAbbruch()`. Transitions the job status to `ABBRUCH`.
*   **`procedures/DWMSG_Fehlerbehandlung.sql`**
    *   *Role*: Replaces `DWMSG_Fehlerbehandlung()`. Orchestrates the rollback state, logs the caught error code, and marks the status as `ABBRUCH`.
*   **`procedures/DWMSG_Logdateiname.sql`**
    *   *Role*: Replaces `DWMSG_Logdateiname()`. Generates a standardized Cloud Storage log path (`gs://<gcp-project>-prot/...`) instead of a local Unix path.
*   **`procedures/DWMSG_SetzeStichtagInfo.sql`**
    *   *Role*: Replaces `DWMSG_SetzeStichtagInfo()`. Parses and updates the reporting date (`stichtag`) using strict format strings.
*   **`procedures/DWMSG_AppendTimingInfos.sql`**
    *   *Role*: Replaces `DWMSG_AppendTimingInfos()`. Appends runtime execution timestamps to the job's metadata.

---

## 3. Key Design Decisions

### Stored Procedures over Shell Wrappers
*   *Decision*: Migrate the core logic into BigQuery Stored Procedures rather than maintaining complex shell scripts executing `bq query` commands.
*   *Reasoning*: This encapsulates the logging schema and transaction logic within the database layer, reducing network roundtrips and ensuring consistency across different calling applications (e.g., Airflow, Cloud Functions, or legacy shell wrappers).

### Sequence Emulation
*   *Decision*: Emulate Oracle sequences using a single-row tracking table (`message_entry_sequence`) updated via a `MERGE` and `UPDATE` transaction block.
*   *Trade-off*: BigQuery is an analytical database and does not natively support auto-incrementing sequences. While this emulation maintains strict integer-based sequential IDs, it introduces a potential serialization bottleneck if hundreds of jobs attempt to request an ID simultaneously.

### Cloud Storage Log Paths
*   *Decision*: Replace local Unix log directories (`/vobs/.../prot/`) with Google Cloud Storage URIs (`gs://${GCP_PROJECT_ID}-prot/`).
*   *Reasoning*: Local disk storage is ephemeral in cloud execution environments (like GKE or Cloud Run). Cloud Storage provides durable, centralized, and secure log retention.

---

## 4. Manual Steps Before Go-Live

### 1. Schema and Dataset Creation
Ensure the target BigQuery dataset exists in your project. If not, create it:
```bash
bq mk --dataset --location=EU ${GCP_PROJECT_ID}:${BQ_DATASET_ID}
```

### 2. IAM & Permissions
The service account executing the migrated jobs (e.g., Cloud Composer worker service account) must have the following IAM roles:
*   `roles/bigquery.dataEditor` (to write to logging tables)
*   `roles/bigquery.jobUser` (to run stored procedures)
*   `roles/storage.objectCreator` (to write log files to the GCS bucket)

### 3. Cloud Storage Bucket Creation
Create the GCS bucket designated for execution logs:
```bash
gcloud storage buckets create gs://${GCP_PROJECT_ID}-prot --location=EU
```

### 4. Deployment Order
Deploy the generated SQL files in the following strict order to resolve dependencies:
1.  **Tables**:
    *   `tables/message_entry_sequence.sql`
    *   `tables/message_table.sql`
    *   `tables/message_errors.sql`
2.  **Internal Helper Procedures**:
    *   `procedures/sp_ensure_sequence_row.sql`
    *   `procedures/sp_next_sequence_value.sql`
    *   `procedures/sp_update_message_status.sql`
    *   `procedures/sp_insert_message_error.sql`
    *   `procedures/sp_set_message_additional_info.sql`
3.  **Public Interface Procedures**:
    *   `procedures/DWMSG_ErmittleNr.sql`
    *   `procedures/DWMSG_ErzeugeEintrag.sql`
    *   `procedures/DWMSG_MeldeFehler.sql`
    *   `procedures/DWMSG_SetzeStatusOK.sql`
    *   `procedures/DWMSG_SetzeStatusAbbruch.sql`
    *   `procedures/DWMSG_Fehlerbehandlung.sql`
    *   `procedures/DWMSG_Logdateiname.sql`
    *   `procedures/DWMSG_SetzeStichtagInfo.sql`
    *   `procedures/DWMSG_AppendTimingInfos.sql`

---

## 5. Known Gaps & Unresolved References

### 1. Concurrency Bottleneck on Sequence Table (Redesign Item B4)
*   *Description*: The sequence emulation table (`message_entry_sequence`) requires row-level locks during updates. High-concurrency environments will experience execution delays.
*   *Follow-up / Redesign*: It is highly recommended to transition downstream systems to accept UUIDs (`GENERATE_UUID()`) instead of sequential integers. This would allow the complete removal of the sequence table and the `DWMSG_ErmittleNr` bottleneck.

### 2. Shell Trap Integration
*   *Description*: The legacy shell script relied on `trap 'DWMSG_Fehlerbehandlung' ERR` to catch runtime failures. BigQuery stored procedures cannot catch external shell or orchestration failures.
*   *Follow-up*: Downstream orchestration (e.g., Airflow DAGs) must be configured to catch task failures and explicitly invoke `DWMSG_Fehlerbehandlung` via an `on_failure_callback` or a teardown task.

### 3. Date Format Mapping
*   *Description*: Oracle and BigQuery use different date formatting tokens (e.g., Oracle `YYYY-MM-DD HH24:MI:SS` vs. BigQuery `%Y-%m-%d %H:%M:%S`).
*   *Follow-up*: Any downstream job calling `DWMSG_SetzeStichtagInfo` or `DWMSG_AppendTimingInfos` must pass BigQuery-compatible format strings.

---

## 6. Validation

To validate the deployment, run the following verification script in the BigQuery Console. It simulates a complete job execution lifecycle.

### Test Script
```sql
DECLARE v_eintrags_nr INT64;
DECLARE v_log_datei STRING;

-- 1. Retrieve a unique transaction ID
CALL `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.DWMSG_ErmittleNr`(v_eintrags_nr);
SELECT v_eintrags_nr AS debug_eintrags_nr;

-- 2. Generate log filename
CALL `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.DWMSG_Logdateiname`('TEST_JOB', v_eintrags_nr, v_log_datei);
SELECT v_log_datei AS debug_log_datei;

-- 3. Create execution entry
CALL `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.DWMSG_ErzeugeEintrag`(v_eintrags_nr, 'TEST_JOB', 'validation_script.sql', v_log_datei);

-- 4. Add metadata and timing info
CALL `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.DWMSG_SetzeStichtagInfo`(v_eintrags_nr, '2023-10-31', '%Y-%m-%d');
CALL `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.DWMSG_AppendTimingInfos`(v_eintrags_nr, 'Step 1 completed at', '%Y-%m-%d %H:%M:%S');

-- 5. Log a warning
CALL `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.DWMSG_MeldeFehler`(v_eintrags_nr, 'W', 100, 'Non-fatal warning message', NULL);

-- 6. Set status to OK
CALL `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.DWMSG_SetzeStatusOK`(v_eintrags_nr);
```

### What "Passing" Means
1.  The test script executes with **zero errors**.
2.  Querying the tracking tables returns the expected records:
    ```sql
    SELECT * FROM `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.message_table` WHERE eintragsnr = v_eintrags_nr;
    -- Expected: status = 'OK', jobkennung = 'TEST_JOB', stichtag = '2023-10-31'
    
    SELECT * FROM `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.message_errors` WHERE eintragsnr = v_eintrags_nr;
    -- Expected: One row with typ = 'W', fehlernr = 100, zusatz1 = 'Non-fatal warning message'
    ```

---

## 7. Rollback Procedure

In the event of an issue during deployment or validation, execute the following steps to roll back the changes:

1.  **Drop Stored Procedures**:
    ```sql
    DROP PROCEDURE IF EXISTS `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.DWMSG_AppendTimingInfos`;
    DROP PROCEDURE IF EXISTS `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.DWMSG_SetzeStichtagInfo`;
    DROP PROCEDURE IF EXISTS `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.DWMSG_Logdateiname`;
    DROP PROCEDURE IF EXISTS `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.DWMSG_Fehlerbehandlung`;
    DROP PROCEDURE IF EXISTS `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.DWMSG_SetzeStatusAbbruch`;
    DROP PROCEDURE IF EXISTS `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.DWMSG_SetzeStatusOK`;
    DROP PROCEDURE IF EXISTS `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.DWMSG_MeldeFehler`;
    DROP PROCEDURE IF EXISTS `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.DWMSG_ErzeugeEintrag`;
    DROP PROCEDURE IF EXISTS `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.DWMSG_ErmittleNr`;
    DROP PROCEDURE IF EXISTS `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.sp_set_message_additional_info`;
    DROP PROCEDURE IF EXISTS `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.sp_insert_message_error`;
    DROP PROCEDURE IF EXISTS `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.sp_update_message_status`;
    DROP PROCEDURE IF EXISTS `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.sp_next_sequence_value`;
    DROP PROCEDURE IF EXISTS `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.sp_ensure_sequence_row`;
    ```

2.  **Drop Tables** (Warning: This will delete logged execution history. Only run if a complete schema reset is required):
    ```sql
    DROP TABLE IF EXISTS `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.message_errors`;
    DROP TABLE IF EXISTS `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.message_table`;
    DROP TABLE IF EXISTS `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.message_entry_sequence`;
    ```

3.  **Revert Orchestration**:
    Point downstream orchestration tasks back to the legacy shell-based execution path (`f_alis_msgerr.ksh`) and the Oracle database environment.