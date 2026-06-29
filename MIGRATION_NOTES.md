# Migration Notes

**System/Job:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_basis.ksh`  
**Target Platform:** Google Cloud Platform (GCP)  
**Author:** Technical Writing & Cloud Migration Team  

---

## 1. Summary

This document details the migration of the legacy KornShell (KSH) orchestrator script `k_ausd_bp_ta_bpr_basis.ksh` and its associated Oracle SQL script `d_ausd_bp_ta_bpr_basis.sql` to a modern, serverless architecture on Google Cloud Platform (GCP).

*   **Source Environment:** Legacy Unix/Linux server running KornShell (KSH), sourcing local environment profiles, and executing database operations via Oracle SQL\*Plus.
*   **Target Environment:** Google Cloud Platform (GCP).
    *   **Orchestration:** Cloud Composer (Apache Airflow 2.x) managing workflow execution, parameter validation, and date arithmetic.
    *   **Compute & Storage:** Google BigQuery executing SQL transformations via Stored Procedures and storing target tables.
    *   **Audit Logging:** Centralized BigQuery logging table (`job_control_log`) replacing local temporary files and legacy shell-based logging frameworks.

---

## 2. Generated Artifacts

The migration process has generated the following target artifacts to replace the legacy components:

| Artifact File Name | Target Platform / Path | Role / Purpose |
| :--- | :--- | :--- |
| `k_ausd_bp_ta_bpr_basis_orchestrator.py` | Cloud Composer (`/dags` bucket) | **Airflow DAG**: Replaces the KSH script. Handles parameter validation, date derivation (replacing `gestern.ksh`), and triggers the BigQuery stored procedure. |
| `sp_d_ausd_bp_ta_bpr_basis.sql` | Google BigQuery (Stored Procedure) | **Stored Procedure**: Replaces `d_ausd_bp_ta_bpr_basis.sql`. Contains the core SQL transformation and data loading logic. |
| `job_control_log_ddl.sql` | Google BigQuery (DDL Script) | **Database Schema**: Creates the centralized audit log table `project.dataset.job_control_log` to track job execution metrics. |
| `cibasis_post_processing.sql` | Google BigQuery (SQL Script) | **SQL Query**: Replaces legacy commented-out file processing logic (`sed`, `sort`, `join` on raw `.dat` files) with native BigQuery SQL CTEs. |

---

## 3. Key Design Decisions

### 3.1 Serverless Orchestration via Cloud Composer (Apache Airflow)
*   **Decision:** Replaced the legacy KSH orchestrator with a Python-based Apache Airflow DAG.
*   **Reasoning:** Airflow provides native scheduling, robust error handling, built-in retry mechanisms, and deep integration with Google Cloud services. This eliminates the need to maintain virtual machines or legacy cron schedulers.
*   **Trade-off:** Passing parameters between tasks requires using Airflow XComs, which introduces minor operational overhead compared to local shell variables but guarantees strict task isolation and auditability.

### 3.2 Python-Native Date Arithmetic
*   **Decision:** Replaced the external legacy script `gestern.ksh` and custom validation library `h_alis_date.ksh` with Python's standard `datetime` library inside an Airflow `PythonOperator`.
*   **Reasoning:** Native Python date manipulation is highly reliable, platform-independent, and simplifies debugging by keeping date derivation logic within the orchestration layer.

### 3.3 BigQuery Stored Procedures for SQL Logic
*   **Decision:** Migrated the Oracle SQL logic from `d_ausd_bp_ta_bpr_basis.sql` into a BigQuery Stored Procedure (`sp_d_ausd_bp_ta_bpr_basis`) rather than embedding raw SQL strings inside the Airflow DAG.
*   **Reasoning:** This maintains a clean separation of concerns. Database administrators can optimize and update the SQL logic directly in BigQuery without redeploying Airflow DAG code.

### 3.4 In-Database File Processing
*   **Decision:** Replaced legacy commented-out shell commands (`sed`, `sort`, `join` operations on raw `.dat` files) with BigQuery SQL Common Table Expressions (CTEs).
*   **Reasoning:** Performing text manipulation and joins on local files is highly inefficient and error-prone. Moving this logic to BigQuery leverages its massive parallel processing capabilities and eliminates local disk I/O dependencies.

---

## 4. Manual Steps Before Go-Live

Before deploying and running the migrated job in production, the following manual setup steps must be completed:

### 4.1 Schema & Dataset Creation
1.  Ensure the target BigQuery dataset (e.g., `project.dataset`) exists in your GCP project.
2.  Execute the DDL script `job_control_log_ddl.sql` to create the centralized audit table:
    ```sql
    CREATE TABLE IF NOT EXISTS `project.dataset.job_control_log` (
      job_kennung STRING,
      eintrags_nr STRING,
      tab_name STRING,
      stichtag DATE,
      status STRING,
      record_count INT64,
      message STRING,
      created_at TIMESTAMP
    );
    ```
3.  Deploy the target table `PoolBasisprodukt` and the raw staging tables (`cibasis_data24_raw`, `cibasis_data96_raw`, `cibasis_fax_raw`) in BigQuery.
4.  Compile the BigQuery Stored Procedure `sp_d_ausd_bp_ta_bpr_basis` in the target dataset.

### 4.2 IAM & Permissions
Ensure the Service Account used by Cloud Composer has the following IAM roles:
*   `roles/bigquery.jobUser` (to execute queries and stored procedures)
*   `roles/bigquery.dataEditor` (on the target BigQuery dataset)
*   `roles/storage.objectViewer` (on the GCS bucket containing the DAGs)

### 4.3 Connection Strings & Secrets
*   Verify that the default BigQuery connection (`bigquery_default`) is properly configured in the Airflow Connections console.
*   If project or dataset names are dynamic, configure them as Airflow Variables (e.g., `gcp_project_id`, `bq_dataset_name`).

### 4.4 Scheduling
*   The migrated DAG is initialized with `schedule_interval=None` (on-demand execution). If this job must run on a recurring schedule, update the `schedule_interval` parameter in `k_ausd_bp_ta_bpr_basis_orchestrator.py` to the desired cron expression.

---

## 5. Known Gaps & Unresolved References

The following items have been flagged for follow-up and must be resolved prior to final production sign-off:

1.  **Downstream SQL Syntax Conversion (Redesign B4 Item):**
    *   The internal logic of `d_ausd_bp_ta_bpr_basis.sql` was not fully converted in this phase. Oracle-specific syntax (such as `(+)` outer joins, Oracle-specific date functions, and session-level `ALTER SESSION` statements) must be manually refactored to standard BigQuery SQL inside the stored procedure.
2.  **Enterprise Metadata Integration:**
    *   The legacy script contained commented-out calls to framework operations (`FOSJobDeaktivate` and `FOSJobErzeugeEintrag`). While we have implemented a local `job_control_log` table, integration with any broader GCP-native metadata catalog or enterprise logging framework remains an open action item.
3.  **Wiederanlaufwert (Restart Parameter) Functional Verification:**
    *   The restart parameter (`-l` / `p_wiederanlaufWert`) is successfully passed from Airflow to the stored procedure. However, the functional logic of how this parameter controls recovery behavior inside the SQL script must be thoroughly verified during integration testing.

---

## 6. Validation

To validate the migration, execute the following testing procedure:

### 6.1 How to Run the Tests
1.  Upload `k_ausd_bp_ta_bpr_basis_orchestrator.py` to the `/dags` folder of your Cloud Composer environment.
2.  Navigate to the Airflow UI, locate the DAG `k_ausd_bp_ta_bpr_basis_orchestrator`, and trigger it manually.
3.  Provide the following JSON configuration in the "Trigger DAG w/ config" window:
    ```json
    {
      "p_JobKennung": "TEST_JOB_01",
      "p_EintragsNr": "100249",
      "p_Stichtag": "31122022",
      "p_wiederanlaufWert": "0"
    }
    ```

### 6.2 What "Passing" Means
The migration is considered successful and validated when:
*   The Airflow DAG execution completes with a `SUCCESS` status.
*   The `validate_inputs_and_dates` task successfully parses the date and pushes the correct ISO-formatted dates to XComs.
*   The `execute_d_ausd_bp_ta_bpr_basis_sp` task successfully calls the BigQuery stored procedure without syntax or runtime errors.
*   The target table `PoolBasisprodukt` is populated with the expected records.
*   A new row is written to `project.dataset.job_control_log` containing the correct job metadata, execution timestamp, and processed record count.

---

## 7. Rollback Procedure

In the event of a critical failure during deployment or execution, follow these steps to roll back the migration:

1.  **Pause the DAG:**
    *   Open the Airflow UI and toggle the switch to **Pause** the `k_ausd_bp_ta_bpr_basis_orchestrator` DAG. This prevents any further automated or manual executions.
2.  **Remove Artifacts:**
    *   Delete the DAG file `k_ausd_bp_ta_bpr_basis_orchestrator.py` from the Cloud Composer GCS bucket.
3.  **Clean Up Target Tables:**
    *   If a failed run corrupted target data, restore the target table `PoolBasisprodukt` to its pre-migration state using BigQuery Time Travel:
        ```sql
        CREATE OR REPLACE TABLE `project.dataset.PoolBasisprodukt` AS
        SELECT * FROM `project.dataset.PoolBasisprodukt`
        FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
        ```
4.  **Revert to Legacy Execution:**
    *   If necessary, re-enable the legacy KornShell script execution on the legacy environment. Ensure that database links and environment variables are restored to point to the legacy Oracle database.