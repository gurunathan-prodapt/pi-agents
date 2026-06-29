# Migration Notes: `ausd_bp_ta_ibcp_ccid`

This document provides comprehensive migration notes for the transition of the legacy batch job `ausd_bp_ta_ibcp_ccid` to a modern, native Google Cloud Platform (GCP) ELT pipeline.

---

## 1. Summary

The legacy batch job `ausd_bp_ta_ibcp_ccid` has been migrated from an on-premise Unix Shell (KSH) and Oracle PL/SQL environment to a native **GCP BigQuery ELT pipeline** orchestrated by **Google Cloud Composer (Apache Airflow)**.

### Scope of Migration
*   **Legacy Source**: Oracle ERP database tables (`GL_CODE_COMBINATIONS` and `IBCP_STAGE_TXN`) containing Chart of Accounts (COA) segment definitions and Intercompany Business Cost Platform (IBCP) billing transactions.
*   **Target Platform**: Google Cloud Platform.
    *   **Data Lake / Staging**: Google Cloud Storage (GCS) and BigQuery Staging (`bq_stage_ta`).
    *   **Data Warehouse / Core**: BigQuery Core Integration (`finance_ta`).
    *   **Orchestration**: Cloud Composer (Apache Airflow).
*   **Business Purpose**: Aggregates, cleanses, maps, and loads Cost Center and GL Code Combination IDs (CCIDs) for Australian Dollar (AUSD) intercompany business billing and transaction accounting.

---

## 2. Generated Artifacts

The migration process has produced the following artifacts, organized by their role in the target architecture:

| Artifact Path | Type | Role / Description |
| :--- | :--- | :--- |
| `src/sql/ddl_staging.sql` | DDL Script | Defines the schema for raw staging tables in BigQuery (`stg_oracle_ccid` and `stg_ibcp_txns`). Includes clustering and partitioning configurations. |
| `src/sql/ddl_core.sql` | DDL Script | Defines the schema for conformed core tables (`dim_ccid_ibcp` and `fact_ibcp_ledger`). Establishes primary keys, descriptions, and optimization parameters. |
| `src/sql/merge_ccid_ibcp.sql` | DML Script | Contains the core transformation and `MERGE` logic. Filters for Australian entities, concatenates account segments, and upserts data into core tables. |
| `src/sql/reconcile_ccid_ibcp.sql` | DML Script | A validation query that compares row counts, distinct keys, and transaction amounts between staging and core tables to identify data discrepancies. |
| `src/dags/ausd_bp_ta_ibcp_ccid_dag.py` | Python (Airflow) | Orchestrates the entire end-to-end pipeline: JDBC extraction to GCS, loading to BigQuery staging, running transformations, and executing reconciliation audits. |

---

## 3. Key Design Decisions

### ELT (Extract-Load-Transform) Architecture
*   **Decision**: Shift from legacy ETL (where transformations occurred during extraction or via PL/SQL procedures) to an ELT pattern.
*   **Rationale**: Leveraging BigQuery's highly parallelized compute engine for transformations (`MERGE` statements) reduces execution time and eliminates the need for intermediate processing servers.

### Parquet Format for GCS Landings
*   **Decision**: Export data from Oracle to GCS as Parquet files instead of CSV.
*   **Rationale**: Parquet preserves schema metadata, supports compression, and prevents common CSV parsing issues (e.g., unescaped commas or newlines in text fields).

### Table Partitioning and Clustering
*   **Decision**: 
    *   `fact_ibcp_ledger` and `stg_ibcp_txns` are partitioned by `txn_date` and clustered by `code_combination_id`.
    *   `dim_ccid_ibcp` is clustered by `code_combination_id`, `entity_code`, and `cost_center_code`.
*   **Rationale**: Partitioning by transaction date optimizes query performance and controls costs for daily ledger analysis. Clustering by CCID and entity codes speeds up downstream join operations.

### Automated Reconciliation Gate
*   **Decision**: Implement a hard-stop Python validation task (`reconcile_and_verify`) at the end of the Airflow DAG.
*   **Rationale**: Financial pipelines require strict data integrity. If row counts, distinct keys, or transaction amounts do not match exactly between staging and core, the DAG halts and alerts the operations team, preventing corrupted downstream reporting.

---

## 4. Manual Steps Before Go-Live

The following setup steps must be completed manually before unpausing the Airflow DAG in production:

### A. Schema & Dataset Creation
1.  Ensure the target BigQuery datasets exist in their respective projects:
    *   Project: `prj-ausd-stage-gcp`, Dataset: `bq_stage_ta` (Location: `australia-southeast1` or your designated multi-region).
    *   Project: `prj-ausd-core-gcp`, Dataset: `finance_ta` (Location: `australia-southeast1` or your designated multi-region).
2.  Execute the DDL scripts in the target BigQuery environment:
    ```bash
    bq query --use_legacy_sql=false < src/sql/ddl_staging.sql
    bq query --use_legacy_sql=false < src/sql/ddl_core.sql
    ```

### B. GCS Bucket Provisioning
1.  Create the raw landing bucket: `prj-ausd-stage-gcp-raw-data`.
2.  Configure a lifecycle policy on this bucket to archive or delete files older than 90 days to manage storage costs.

### C. IAM & Permissions
Ensure the Cloud Composer / Airflow service account has the following IAM roles:
*   `roles/storage.objectAdmin` on `prj-ausd-stage-gcp-raw-data`.
*   `roles/bigquery.jobUser` on the orchestrating project.
*   `roles/bigquery.dataEditor` on `bq_stage_ta` and `finance_ta` datasets.
*   Network access (via Cloud VPN or Interconnect) to the on-premise Oracle ERP database.

### D. Airflow Connections & Variables
Configure the following connections in the Airflow UI (**Admin -> Connections**):
1.  **`oracle_erp_jdbc_conn`**:
    *   **Conn Type**: `JDBC`
    *   **Connection URL**: `jdbc:oracle:thin:@//<oracle-host>:<port>/<service-name>`
    *   **Login**: Oracle database username.
    *   **Password**: Oracle database password (stored securely in Secret Manager).
    *   **Drivers**: Ensure the Oracle JDBC driver (e.g., `ojdbc8.jar`) is present in the Airflow worker's Java classpath.
2.  **`google_cloud_default`**:
    *   **Conn Type**: `Google Cloud`
    *   *Note*: If running on Cloud Composer, this defaults to the environment's service account.

### E. Scheduling Alignment
*   Coordinate with the ERP team to identify the exact daily ledger close time.
*   Disable the legacy scheduler (e.g., Control-M, UC4, or crontab) for job `AUSD_BP_TA_IBCP_CCID` to prevent concurrent execution.

---

## 5. Known Gaps & Unresolved References

1.  **Oracle Segment Mapping Verification**:
    *   *Gap*: The mapping of `segment1` through `segment5` to specific business dimensions (Entity, Cost Center, Account, Sub-Account, Intercompany Partner) is based on standard Oracle configurations.
    *   *Action*: The local ERP administrator must verify that these segments align with the active Australian Chart of Accounts (COA) before deployment.
2.  **Australian Entity Codes**:
    *   *Gap*: The hardcoded filter `segment1 IN ('AU', '080')` was inferred.
    *   *Action*: Confirm if additional company codes represent Australian operations and update `merge_ccid_ibcp.sql` and the Airflow DAG accordingly.
3.  **JDBC Driver Dependency**:
    *   *Gap*: Airflow workers require a Java Runtime Environment (JRE) and the Oracle JDBC driver to execute `JdbcToGCSOperator`.
    *   *Action*: Ensure the Cloud Composer environment is configured with a custom container image containing the driver, or migrate to an alternative extraction mechanism if Java is restricted.

---

## 6. Validation

To validate the migration and ensure the pipeline is operating correctly, perform the following steps:

### Running the Pipeline Test
1.  In the Airflow UI, locate the DAG `ausd_bp_ta_ibcp_ccid`.
2.  Trigger a manual run of the DAG for a historical date (e.g., yesterday).
3.  Monitor the execution of the tasks:
    `extract_*` $\rightarrow$ `load_*` $\rightarrow$ `run_merge_transformations` $\rightarrow$ `reconcile_and_verify`.

### Definition of "Passing"
The migration is considered successful and validated when:
1.  All Airflow tasks complete with a status of `SUCCESS`.
2.  The `reconcile_and_verify` task logs show no exceptions.
3.  Running the validation script `src/sql/reconcile_ccid_ibcp.sql` in the BigQuery console returns a status of **`PASSED`** for all rows:

| table_name | row_count_delta | distinct_keys_delta | total_amount_delta | reconciliation_status |
| :--- | :--- | :--- | :--- | :--- |
| `dim_ccid_ibcp` | 0 | 0 | 0.00 | **PASSED** |
| `fact_ibcp_ledger` | 0 | 0 | 0.00 | **PASSED** |

---

## 7. Rollback Procedure

In the event of a critical failure or data corruption during go-live, execute the following rollback steps:

1.  **Pause the Airflow DAG**:
    *   Immediately turn off the toggle for `ausd_bp_ta_ibcp_ccid` in the Airflow UI to prevent subsequent scheduled runs.
2.  **Re-enable Legacy Scheduling**:
    *   Re-activate the legacy scheduler job (Control-M/UC4/crontab) to resume processing via the legacy PL-SQL/KSH pipeline.
3.  **Restore BigQuery Tables (If Corrupted)**:
    *   If data corruption occurred in the core tables, restore them to their pre-migration state using BigQuery's Time Travel feature:
    ```sql
    -- Example: Restoring dim_ccid_ibcp to its state 1 hour ago
    CREATE OR REPLACE TABLE `prj-ausd-core-gcp.finance_ta.dim_ccid_ibcp` AS
    SELECT * FROM `prj-ausd-core-gcp.finance_ta.dim_ccid_ibcp`
    FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
    ```
4.  **Clean Up GCS Storage**:
    *   Delete any corrupted Parquet files generated during the failed run from `gs://prj-ausd-stage-gcp-raw-data/`.