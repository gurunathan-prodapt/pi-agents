# MIGRATION NOTES

**Source Component:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh`  
**Target Platform:** Google BigQuery / Cloud Orchestration (Cloud Composer / Airflow)

---

## 1. Summary

The legacy KornShell (KSH) utility library `f_alis_msgerr.ksh` (historically sourced as `dwmsg.ksh`) has been migrated to a cloud-native architecture on **Google Cloud Platform (GCP)**. 

### What Was Migrated
* **Legacy Shell Functions:** Shell-based wrappers (`DWMSG_ErmittleNr`, `DWMSG_ErzeugeEintrag`, `DWMSG_MeldeFehler`, etc.) that historically executed Oracle PL/SQL procedures via `sqlplus` command-line interfaces.
* **Database Logic:** Oracle PL/SQL package `BERT_MELDUNG` routines, which managed execution tracking, unique run ID generation, error logging, and status updates.
* **File-Based Logging:** Local file path generation and tracking historically pointing to `$DW_DIR_PROT`.

### Target Platform Architecture
* **Database Layer:** Google BigQuery Stored Procedures and structured audit tables.
* **Orchestration Layer:** Cloud Composer (Apache Airflow) utilizing a custom Python helper module (`dwmsg.py`) to interface with BigQuery logging procedures and manage execution state.
* **Storage Layer:** Google Cloud Storage (GCS) for centralized, durable log file storage.

---

## 2. Generated Artifacts

The migration process has produced three core artifacts, each serving a distinct role in the target architecture:

| # | Target File Path | Language | Role / Description |
|---|---|---|---|
| **1** | `gcp/bigquery/ddl/DWMSG_Tables.sql` | SQL (DDL) | **Schema Definition:** Creates the centralized audit logging tables (`message_table`, `message_error_table`, `message_sequence_table`, `message_timing_table`) in BigQuery. Includes table partitioning by date and clustering to optimize query performance and cost. |
| **2** | `gcp/bigquery/procedures/DWMSG_Procedures.sql` | SQL (Stored Proc) | **Business Logic:** Implements BigQuery stored procedures that replace the legacy Oracle `BERT_MELDUNG` package and KSH helper functions. Handles transactional sequence generation, status updates, and error registration. |
| **3** | `gcp/orchestration/utils/dwmsg.py` | Python | **Orchestration Helper:** A Python module for Cloud Composer (Airflow) DAGs. It provides helper functions to dynamically construct SQL calls to the BigQuery procedures, generate GCS log paths, and handle task-level error trapping. |

---

## 3. Key Design Decisions

### 3.1 Shift from Shell/SQL*Plus to Native BigQuery Scripting
* **Decision:** Completely eliminate KSH shell wrappers, local temporary files (`/tmp/ErmittleNr_*.lst`), and `sqlplus` subprocess execution.
* **Reasoning:** Running shell scripts on VM instances to execute database queries is an anti-pattern in modern cloud data warehouses. Moving the logic directly into BigQuery stored procedures reduces execution latency, removes VM maintenance overhead, and leverages native IAM security.

### 3.2 Transactional Sequence Generation in BigQuery
* **Decision:** Implemented an explicit transaction block (`BEGIN TRANSACTION ... COMMIT TRANSACTION`) on a dedicated sequence table (`message_sequence_table`) to mimic Oracle sequence behavior.
* **Trade-off:** BigQuery is designed for analytical workloads (OLAP) rather than high-frequency transactional updates (OLTP). Under extremely high concurrency, updating a single row in `message_sequence_table` may introduce lock contention. 
* **Alternative Considered:** Using `GENERATE_UUID()` was considered to avoid locking. However, sequential integer IDs were retained to maintain backward compatibility with downstream legacy reporting systems that require numeric sorting.

### 3.3 Partitioning and Clustering Strategy
* **Decision:** Partitioned `message_table`, `message_error_table`, and `message_timing_table` by `DATE(created_at)` and clustered them by `entry_nr`.
* **Reasoning:** Audit tables grow continuously. Partitioning ensures that queries analyzing recent runs do not scan historical data, keeping BigQuery query costs low. Clustering by `entry_nr` ensures rapid lookups when retrieving logs for a specific job run.

### 3.4 Cloud Storage (GCS) for Log Paths
* **Decision:** Replaced local file system paths (`$DW_DIR_PROT`) with GCS URIs (`gs://${protocol_gcs_bucket}/...`).
* **Reasoning:** Cloud Composer workers are ephemeral. Writing logs to local disk would result in data loss upon worker scale-down. GCS provides durable, centralized, and highly available log storage.

---

## 4. Manual Steps Before Go-Live

The following steps must be executed manually in the target environment before deploying any ETL jobs that depend on the migrated logging framework:

### 4.1 Schema and Dataset Creation
1. Create the target BigQuery dataset (e.g., `dw_audit_logging`) in your designated GCP project and region.
2. Execute the DDL script `gcp/bigquery/ddl/DWMSG_Tables.sql` to initialize the tables and seed the sequence table:
   ```bash
   bq query --use_legacy_sql=false < gcp/bigquery/ddl/DWMSG_Tables.sql
   ```

### 4.2 Deploy Stored Procedures
1. Deploy the stored procedures to the newly created dataset:
   ```bash
   bq query --use_legacy_sql=false < gcp/bigquery/procedures/DWMSG_Procedures.sql
   ```

### 4.3 IAM & Permissions
Ensure that the Service Account used by Cloud Composer / Airflow has the following IAM roles:
* **BigQuery Data Editor** (`roles/bigquery.dataEditor`) on the audit dataset.
* **BigQuery Job User** (`roles/bigquery.jobUser`) on the project level.
* **Storage Object Creator** (`roles/storage.objectCreator`) on the GCS protocol log bucket.

### 4.4 Environment Variables & Airflow Connections
Define the following variables in your Airflow environment (or via Airflow Variables):
* `GCP_PROJECT_ID`: The active GCP project ID.
* `AUDIT_DATASET`: The name of the BigQuery logging dataset (e.g., `dw_audit_logging`).
* `PROTOCOL_GCS_BUCKET`: The GCS bucket name for logs (e.g., `gs://dw-prod-protocol-logs`).
* `GCP_CONN_ID`: The Airflow connection ID for Google Cloud (typically `google_cloud_default`).

---

## 5. Known Gaps & Unresolved References

### 5.1 Concurrency Bottlenecks (Redesign Item B4)
* **Gap:** The transactional update on `message_sequence_table` is synchronous. If hundreds of parallel Airflow tasks request an entry number simultaneously, some tasks may experience retries or timeouts due to BigQuery transaction limits.
* **Follow-up Action:** For high-concurrency environments, it is highly recommended to transition downstream systems to accept UUIDs. The Python utility can be updated to bypass `DWMSG_ErmittleNr` and generate UUIDs directly:
  ```python
  import uuid
  entry_nr = uuid.uuid4().int >> 64 # Safe 64-bit integer representation
  ```

### 5.2 Mail and Robomon Notifications
* **Gap:** The legacy KSH script contained comments regarding automated alerting via mail or "Robomon" upon failure. These alerting mechanisms are not implemented within the BigQuery stored procedures.
* **Follow-up Action:** Alerting must be handled at the orchestration layer. Configure Airflow DAGs with `on_failure_callback` functions to send Slack, PagerDuty, or Email notifications when a task fails.

### 5.3 Legacy Shell Script Sourcing
* **Gap:** Any legacy shell scripts still running on on-premises VMs or Compute Engine instances that attempt to source (`. f_alis_msgerr.ksh`) will fail.
* **Follow-up Action:** These legacy scripts must be refactored to call the BigQuery API via the `bq` CLI tool or migrated entirely into Cloud Composer DAGs.

---

## 6. Validation

To validate the migration, execute the following test suite to verify database state transitions and orchestration helper logic.

### 6.1 Database-Level Validation (SQL Test Script)
Execute the following SQL block in the BigQuery console to simulate a complete job lifecycle:

```sql
DECLARE v_entry_nr INT64;
DECLARE v_log_file STRING;

-- 1. Get unique entry number
CALL `dw_audit_logging.DWMSG_ErmittleNr`(v_entry_nr);
SELECT FORMAT('Generated Entry Number: %d', v_entry_nr) AS test_step_1;

-- 2. Generate log filename
CALL `dw_audit_logging.DWMSG_Logdateiname`('TEST_JOB', v_entry_nr, v_log_file);
SELECT FORMAT('Generated Log Path: %s', v_log_file) AS test_step_2;

-- 3. Create entry
CALL `dw_audit_logging.DWMSG_ErzeugeEintrag`(v_entry_nr, 'TEST_JOB', 'validation_script.sql', v_log_file);

-- 4. Append timing info
CALL `dw_audit_logging.DWMSG_AppendTimingInfos`(v_entry_nr, 'Step 1 completed', '%Y-%m-%d %H:%M:%S');

-- 5. Simulate an error
CALL `dw_audit_logging.DWMSG_MeldeFehler`(v_entry_nr, 'E', 5001, 'Validation Test Error', 'Context Info');

-- 6. Set status to OK
CALL `dw_audit_logging.DWMSG_SetzeStatusOK`(v_entry_nr);

-- 7. Verify results
SELECT * FROM `dw_audit_logging.message_table` WHERE entry_nr = v_entry_nr;
SELECT * FROM `dw_audit_logging.message_error_table` WHERE entry_nr = v_entry_nr;
SELECT * FROM `dw_audit_logging.message_timing_table` WHERE entry_nr = v_entry_nr;
```

### 6.2 Definition of "Passing"
The validation is successful if:
1. `v_entry_nr` is a positive integer incremented by exactly `1` from the previous run.
2. `message_table` contains a row for the generated `entry_nr` with `status = 'OK'`.
3. `message_error_table` contains the simulated error record with `fehler_nr = 5001`.
4. `message_timing_table` contains the timing text appended with the correct timestamp format.
5. No query errors or transaction conflicts are thrown during execution.

---

## 7. Rollback Procedure

In the event of a critical failure in the target environment, follow these steps to roll back to the legacy Oracle/KSH infrastructure:

### Step 1: Revert Orchestration Layer
* Disable the migrated Cloud Composer DAGs.
* Re-enable the legacy cron jobs or scheduling tool (e.g., UC4, Automic) that executed the KSH scripts on the legacy application servers.

### Step 2: Restore Environment Variables
* Ensure the legacy environment variables are active on the application servers:
  ```bash
  export DW_DIR_ROOT=/vobs/dw_source/isrpt/isbert/SQL/aktuell
  export DW_DIR_PROT=/path/to/legacy/protocols
  export DW_ORAUSER=username/password@oracle_db
  ```

### Step 3: Verify Oracle DB Connectivity
* Verify that the legacy Oracle database is accepting connections and that the `BERT_MELDUNG` package is compiled and valid:
  ```bash
  sqlplus $DW_ORAUSER @d_al_is_ermittlenr.sql
  ```

### Step 4: Archive BigQuery Audit Data
* To prevent data loss from runs executed on GCP during the migration window, export the BigQuery audit tables to GCS and load them into the legacy Oracle database if necessary:
  ```bash
  bq extract --destination_format=CSV 'dw_audit_logging.message_table' gs://dw-prod-protocol-logs/rollback/message_table_export.csv
  ```