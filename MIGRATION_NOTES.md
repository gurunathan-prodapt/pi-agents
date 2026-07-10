# Migration Notes: TEST.NEW_HOUSEKEEPING_JOB

This document details the migration of the legacy utility job `TEST.NEW_HOUSEKEEPING_JOB` (originally implemented via the KornShell library `h_alis_meldungen.ksh` and Oracle PL/SQL package `CCR$VS_MELDUNG`) to Google Cloud BigQuery.

---

## 1. Summary

The legacy utility library `h_alis_meldungen.ksh` has been successfully migrated from an **Oracle / KornShell** environment to **Google Cloud BigQuery**. 

* **Source Platform**: Oracle Database (PL/SQL package `CCR$VS_MELDUNG`) & KornShell (Utility Library)
* **Target Platform**: Google Cloud BigQuery (GoogleSQL Stored Procedures & Native Auditing Tables)
* **Migration Strategy**: Re-engineered. The shell-wrapped database operations (logging, execution tracking, metadata updates, and log file name construction) have been ported directly into native BigQuery Stored Procedures. This preserves modularity, keeps execution states database-native, and eliminates high-frequency network roundtrips between external orchestrators and the data warehouse.

---

## 2. Generated Artifacts

The migration process has produced the following database resources and stored procedures within the target dataset (e.g., `project.audit_dataset`):

### 2.1 Database Tables (DDL Schemas)
* **`project.audit_dataset.message_table`**: The main metadata table tracking execution metrics, statuses, and run parameters per script execution.
* **`project.audit_dataset.message_log`**: The granular log table holding debugging, informational, warning, and error messages.
* **`project.audit_dataset.message_id_control`**: A helper sequence-control table used to generate unique tracking IDs.

### 2.2 Stored Procedures
| File / Resource Name | Target Language | Role / Description |
| :--- | :--- | :--- |
| `project.audit_dataset.CCRMSG_ErmittleNr` | GoogleSQL | Generates a unique tracking ID using the sequence control table. |
| `project.audit_dataset.CCRMSG_ErzeugeEintrag` | GoogleSQL | Registers the initial job run entry in the main tracking table. |
| `project.audit_dataset.CCRMSG_SetzeStichtag` | GoogleSQL | Updates the business reporting snapshot date (`Stichtag`) for the run. |
| `project.audit_dataset.CCRMSG_SetzeAnzahl` | GoogleSQL | Logs the count of processed records. |
| `project.audit_dataset.CCRMSG_SetzeDateiname` | GoogleSQL | Logs external source file names referenced in the job. |
| `project.audit_dataset.CCRMSG_SetzeZusatzinfos` | GoogleSQL | Stores unstructured supplemental tracking parameters. |
| `project.audit_dataset.CCRMSG_LogDebug` | GoogleSQL | Inserts a `DEBUG` level log entry into the log table. |
| `project.audit_dataset.CCRMSG_LogInfo` | GoogleSQL | Inserts an `INFO` level log entry into the log table. |
| `project.audit_dataset.CCRMSG_LogWarn` | GoogleSQL | Inserts a `WARN` level log entry into the log table. |
| `project.audit_dataset.CCRMSG_Fehler` | GoogleSQL | Logs an execution error and marks the job status as `FAILED`. |
| `project.audit_dataset.CCRMSG_Fertig` | GoogleSQL | Marks the job status as `SUCCESS` upon successful completion. |
| `project.audit_dataset.CCRMSG_Logdateiname` | GoogleSQL | Constructs a standardized GCS log file path string. |
| `project.audit_dataset.CCRMSG_Logdateiname_Parallel` | GoogleSQL | Constructs a standardized GCS log file path string for parallel runs. |

---

## 3. Key Design Decisions

### 3.1 Database-Native Execution State
* **Decision**: Port the shell utility library directly into BigQuery Stored Procedures rather than externalizing the logic into Cloud Functions or Airflow Python operators.
* **Reasoning**: Keeping the logging state database-native minimizes latency, ensures transactional consistency for status updates, and allows other BigQuery-native processes to log their progress without leaving the database engine.

### 3.2 GCS Path Construction
* **Decision**: Replace local file protocol paths (`CCR_DIR_PROT`) with Google Cloud Storage (GCS) URI formats (`gs://bucket-name/path`).
* **Reasoning**: Cloud-native execution patterns rely on object storage (GCS) rather than local POSIX filesystems for persistent log storage.

### 3.3 Trade-offs: Sequential IDs vs. UUIDs
* **Decision**: Retained a sequence-control table (`message_id_control`) to mimic the legacy sequential integer ID generation.
* **Trade-off**: While sequential IDs are highly readable, they introduce a potential concurrency bottleneck in highly parallel cloud environments. (See Section 5 for recommended redesign).

---

## 4. Manual Steps Before Go-Live

To deploy and configure the migrated utility job, execute the following steps in order:

### 4.1 Schema & Dataset Creation
Create the target dataset and tables in BigQuery. Run the following DDL statements:

```sql
-- Create Dataset (if not exists)
-- CREATE SCHEMA `project.audit_dataset` OPTIONS(location="EU");

-- 1. Create Sequence Control Table
CREATE TABLE IF NOT EXISTS `project.audit_dataset.message_id_control` (
  entry_nr INT64 OPTIONS(description="Auto-incremented sequence ID"),
  created_at TIMESTAMP
)
AS SELECT 1 AS entry_nr, CURRENT_TIMESTAMP() AS created_at; 
-- Note: For real environments, configure an auto-incrementing mechanism or use a row-count proxy.

-- 2. Create Main Metadata Table
CREATE TABLE IF NOT EXISTS `project.audit_dataset.message_table` (
  entry_nr INT64 NOT NULL,
  job_kennung STRING,
  programmname STRING,
  log_datei STRING,
  parameter_string STRING,
  stichtag DATETIME,
  anzahl INT64,
  dateiname STRING,
  zusatzinfos STRING,
  status STRING,
  error_type INT64,
  error_text STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);

-- 3. Create Log Messages Detail Table
CREATE TABLE IF NOT EXISTS `project.audit_dataset.message_log` (
  entry_nr INT64 NOT NULL,
  severity STRING,
  error_type INT64,
  log_text STRING,
  created_at TIMESTAMP
);
```

### 4.2 Deploy Stored Procedures
Execute the SQL scripts provided in the `GENERATED MIGRATION CODE` section of the design document to compile all `CCRMSG_*` procedures inside `project.audit_dataset`.

### 4.3 IAM & Permissions
Ensure that the service accounts executing the ETL jobs or orchestration DAGs have the following IAM roles:
* **`roles/bigquery.dataEditor`** on `project.audit_dataset` (to insert/update log tables).
* **`roles/bigquery.jobUser`** on the project level (to run the stored procedures).

### 4.4 Connection Strings & Secrets
* No database connection strings or passwords are required in the scripts, as authentication is handled natively via Google Cloud IAM.
* Ensure that any external orchestrator (e.g., Cloud Composer) is configured with the correct target Google Cloud Project ID and Dataset ID.

---

## 5. Known Gaps & Unresolved References

### 5.1 Concurrency Bottleneck on ID Generation (B4 Redesign Item)
* **Gap**: The procedure `CCRMSG_ErmittleNr` queries `MAX(entry_nr)` from `message_id_control`. Under high concurrency, multiple parallel jobs might fetch the same ID or experience lock contention.
* **Recommendation**: Redesign the logging schema to use `GENERATE_UUID()` as the primary tracking key instead of sequential integers. This eliminates the need for a sequence control table entirely.

### 5.2 Oracle Date Format Compatibility
* **Gap**: Legacy jobs pass Oracle-style date format strings (e.g., `YYYY-MM-DD HH24:MI:SS`) to `CCRMSG_SetzeStichtag`. BigQuery's `PARSE_DATETIME` requires GoogleSQL format elements (e.g., `%Y-%m-%d %H:%M:%S`).
* **Action Required**: Update upstream orchestration parameters to pass GoogleSQL-compliant format strings.

---

## 6. Validation

To validate the deployment and ensure the procedures are functioning correctly, execute the following integration test script in the BigQuery console:

```sql
DECLARE v_my_id INT64;
DECLARE v_log_file STRING;

-- 1. Test ID Retrieval
CALL `project.audit_dataset.CCRMSG_ErmittleNr`(v_my_id);
ASSERT v_my_id IS NOT NULL AS 'Validation Failed: ID generation returned NULL';

-- 2. Test Entry Creation
CALL `project.audit_dataset.CCRMSG_ErzeugeEintrag`(
  v_my_id, 
  'TEST_JOB', 
  'test_script.sh', 
  'gs://my-bucket/logs/test.log', 
  'param1=true'
);

-- 3. Test Metadata Setters
CALL `project.audit_dataset.CCRMSG_SetzeStichtag`(v_my_id, '2023-10-27 12:00:00', '%Y-%m-%d %H:%M:%S');
CALL `project.audit_dataset.CCRMSG_SetzeAnzahl`(v_my_id, 1500);
CALL `project.audit_dataset.CCRMSG_SetzeDateiname`(v_my_id, 'input_data.csv');
CALL `project.audit_dataset.CCRMSG_SetzeZusatzinfos`(v_my_id, 'Dry-run validation test');

-- 4. Test Log Writing
CALL `project.audit_dataset.CCRMSG_LogInfo`(v_my_id, 'This is an informational test message.');
CALL `project.audit_dataset.CCRMSG_LogWarn`(v_my_id, 'This is a warning test message.');

-- 5. Test Log File Name Construction
CALL `project.audit_dataset.CCRMSG_Logdateiname`('gs://my-bucket/logs', 'TEST_JOB', v_my_id, v_log_file);
ASSERT v_log_file LIKE 'gs://my-bucket/logs/TEST_JOB_%' AS 'Validation Failed: Log file path constructed incorrectly';

-- 6. Test Completion
CALL `project.audit_dataset.CCRMSG_Fertig`(v_my_id);

-- 7. Verify Results
SELECT * FROM `project.audit_dataset.message_table` WHERE entry_nr = v_my_id;
SELECT * FROM `project.audit_dataset.message_log` WHERE entry_nr = v_my_id;
```

### Meaning of "Passing"
The validation test is considered **passing** if:
1. The script runs to completion without throwing any runtime exceptions or assertion failures.
2. A record is successfully created in `message_table` with a status of `SUCCESS`.
3. Two corresponding records (one `INFO`, one `WARN`) are present in `message_log` linked by the generated `entry_nr`.

---

## 7. Rollback Procedure

In the event of an issue during deployment or go-live, execute the following steps to roll back the changes:

1. **Drop Stored Procedures**:
   ```sql
   DROP PROCEDURE IF EXISTS `project.audit_dataset.CCRMSG_ErmittleNr`;
   DROP PROCEDURE IF EXISTS `project.audit_dataset.CCRMSG_ErzeugeEintrag`;
   DROP PROCEDURE IF EXISTS `project.audit_dataset.CCRMSG_SetzeStichtag`;
   DROP PROCEDURE IF EXISTS `project.audit_dataset.CCRMSG_SetzeAnzahl`;
   DROP PROCEDURE IF EXISTS `project.audit_dataset.CCRMSG_SetzeDateiname`;
   DROP PROCEDURE IF EXISTS `project.audit_dataset.CCRMSG_SetzeZusatzinfos`;
   DROP PROCEDURE IF EXISTS `project.audit_dataset.CCRMSG_LogDebug`;
   DROP PROCEDURE IF EXISTS `project.audit_dataset.CCRMSG_LogInfo`;
   DROP PROCEDURE IF EXISTS `project.audit_dataset.CCRMSG_LogWarn`;
   DROP PROCEDURE IF EXISTS `project.audit_dataset.CCRMSG_Fehler`;
   DROP PROCEDURE IF EXISTS `project.audit_dataset.CCRMSG_Fertig`;
   DROP PROCEDURE IF EXISTS `project.audit_dataset.CCRMSG_Logdateiname`;
   DROP PROCEDURE IF EXISTS `project.audit_dataset.CCRMSG_Logdateiname_Parallel`;
   ```

2. **Drop Tables** (Optional - only perform if a clean slate is required and historical logs do not need to be preserved):
   ```sql
   DROP TABLE IF EXISTS `project.audit_dataset.message_log`;
   DROP TABLE IF EXISTS `project.audit_dataset.message_table`;
   DROP TABLE IF EXISTS `project.audit_dataset.message_id_control`;
   ```

3. **Revert Orchestration**:
   Point the calling orchestration workflows (e.g., Airflow DAGs, shell wrappers) back to the legacy Oracle database connection and the original `h_alis_meldungen.ksh` library script.