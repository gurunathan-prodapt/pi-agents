# MIGRATION NOTES: k_ausd_bp_ta_cntrct_dist.ksh

This document provides the migration notes, architectural decisions, manual setup steps, and validation procedures for migrating the legacy KornShell (KSH) batch job `k_ausd_bp_ta_cntrct_dist.ksh` to Google Cloud Platform (GCP) using Cloud Composer (Apache Airflow) and BigQuery.

---

## 1. Summary

The legacy KornShell script `k_ausd_bp_ta_cntrct_dist.ksh` served as an orchestration and validation wrapper. It validated input parameters, verified business dates, executed an underlying SQL transformation script (`d_ausd_bp_ta_cntrct_dist.sql`), tracked record counts via local temporary files, and logged execution status.

This job has been migrated to a modern, cloud-native architecture on **Google Cloud Platform (GCP)**:
*   **Orchestration & Parameter Validation**: Handled by **Cloud Composer (Apache Airflow)**.
*   **Data Transformation & Logging**: Handled by **BigQuery Stored Procedures** and structured logging tables.

---

## 2. Generated Artifacts

The migration process generated three primary files, each mapping to a specific layer of the target architecture:

| Artifact Path | Language / Format | Role & Description |
| :--- | :--- | :--- |
| `dags/k_ausd_bp_ta_cntrct_dist.py` | Python / Airflow DAG | **Orchestrator**: Replaces the KSH command-line parsing (`getopts`), parameter validation, and date derivation (`gestern.ksh`). It triggers the BigQuery stored procedure. |
| `ddl/sp_k_ausd_bp_ta_cntrct_dist.sql` | SQL (BigQuery Stored Proc) | **Control Procedure**: Replaces the shell wrapper's execution logic. It performs database-level assertions, handles execution flow, and logs job status/errors to BigQuery audit tables. |
| `ddl/d_ausd_bp_ta_cntrct_dist.sql` | SQL (BigQuery Stored Proc) | **Transformation Procedure**: Replaces the legacy `d_ausd_bp_ta_cntrct_dist.sql` script. It executes the core business logic, loads the target table (`PoolBasisprodukt`), and returns the record count. |

---

## 3. Key Design Decisions

### 3.1. Decoupled Validation and Execution
*   **Decision**: Split validation between Airflow (pre-execution checks) and BigQuery (database-level assertions).
*   **Reasoning**: Airflow handles fail-fast validation before spinning up BigQuery resources, saving costs and providing immediate feedback in the orchestration UI. BigQuery stored procedures use `ASSERT` statements to guarantee transactional safety and data integrity at the database layer.

### 3.2. State and Metadata Persistence
*   **Decision**: Replaced legacy local temporary files (`bert_k_ausd_bp_ta_cntrct_dist.tmp`) and legacy FOS job tracking calls with structured BigQuery logging tables (`audit_log.job_run_log` and `audit_log.job_error_log`).
*   **Reasoning**: Local file systems are ephemeral in cloud-native execution environments like GKE (underlying Cloud Composer). Centralizing logs in BigQuery enables persistent audit trails, easier debugging, and native integration with Looker or Cloud Monitoring.

### 3.3. Modular Stored Procedures
*   **Decision**: Maintained a 1:1 mapping of the legacy separation between the wrapper script (`k_ausd_bp_ta_cntrct_dist.ksh`) and the transformation SQL (`d_ausd_bp_ta_cntrct_dist.sql`) by creating two nested stored procedures: `sp_k_ausd_bp_ta_cntrct_dist` and `sp_d_ausd_bp_ta_cntrct_dist`.
*   **Reasoning**: This preserves the original code modularity, simplifies unit testing of the transformation logic independently of the wrapper, and aligns with clean database design principles.

---

## 4. Manual Steps Before Go-Live

Before deploying the DAG and executing the migrated pipeline, the following setup steps must be completed in the target GCP environment:

### 4.1. Schema and Dataset Creation
Ensure the target BigQuery datasets exist in your target region (e.g., `EU` or `US`):
```sql
CREATE SCHEMA IF NOT EXISTS `gcp-project-id.isbert_schema`;
CREATE SCHEMA IF NOT EXISTS `gcp-project-id.audit_log`;
```
*(Note: Replace `gcp-project-id` with your actual GCP Project ID).*

### 4.2. Table and Stored Procedure Deployment
1.  **Deploy Audit Tables**: Run the `CREATE TABLE IF NOT EXISTS` statements found at the top of `ddl/sp_k_ausd_bp_ta_cntrct_dist.sql` to initialize `audit_log.job_error_log` and `audit_log.job_run_log`.
2.  **Deploy Transformation Procedure**: Execute the DDL in `ddl/d_ausd_bp_ta_cntrct_dist.sql` to create `sp_d_ausd_bp_ta_cntrct_dist`.
3.  **Deploy Control Procedure**: Execute the DDL in `ddl/sp_k_ausd_bp_ta_cntrct_dist.sql` to create `sp_k_ausd_bp_ta_cntrct_dist`.

### 4.3. IAM & Permissions
The Cloud Composer Service Account (typically `service-XXX@gcp-sa-webserver.iam.gserviceaccount.com` or a custom user-managed service account) must be granted the following IAM roles:
*   **BigQuery Data Editor** (`roles/bigquery.dataEditor`) on `isbert_schema` and `audit_log` datasets.
*   **BigQuery Job User** (`roles/bigquery.jobUser`) on the project level.

### 4.4. Airflow Connection Configuration
Ensure that the Airflow connection `google_cloud_default` is configured correctly in your Cloud Composer environment and has access to the target GCP project.

### 4.5. DAG Deployment
Copy `dags/k_ausd_bp_ta_cntrct_dist.py` into the Cloud Composer DAGs bucket (`gs://<composer-bucket>/dags/`).

---

## 5. Known Gaps & Unresolved References

### 5.1. Core Business Logic Placeholder (Redesign B4 Item)
*   **Gap**: The generated stored procedure `sp_d_ausd_bp_ta_cntrct_dist` contains a simplified placeholder query that inserts metadata into `PoolBasisprodukt`.
*   **Action Required**: The actual business logic from the legacy `d_ausd_bp_ta_cntrct_dist.sql` must be translated into BigQuery SQL dialect and pasted into the body of `sp_d_ausd_bp_ta_cntrct_dist` (replacing the placeholder `INSERT` statement).

### 5.2. Legacy Commented-Out File Operations
*   **Gap**: The legacy script contained commented-out post-processing commands (`sed`, `sort`, `join`). 
*   **Action Required**: Confirm with business analysts if these file-based operations are obsolete. If they are still required, they must be implemented as SQL transformations (e.g., using `ORDER BY` and `JOIN` clauses) inside BigQuery rather than flat-file manipulations.

### 5.3. Hardcoded Project ID
*   **Gap**: The generated SQL files contain the placeholder project ID `gcp-project-id`.
*   **Action Required**: Replace `gcp-project-id` with the actual GCP project ID (e.g., `prod-data-project`) during the CI/CD deployment pipeline or via manual search-and-replace.

---

## 6. Validation

To validate the migration, execute the pipeline under both successful and failure scenarios.

### 6.1. How to Run the Tests
You can trigger the DAG manually via the Airflow UI or CLI with a custom JSON configuration:

#### Test Case 1: Successful Run
Trigger the DAG with valid parameters:
```json
{
  "job_kennung": "PoolBasisprodukt",
  "eintrags_nr": "100234",
  "stichtag": "31122023",
  "wiederanlauf_wert": 0
}
```

#### Test Case 2: Validation Failure (Missing Parameter)
Trigger the DAG with a missing `eintrags_nr`:
```json
{
  "job_kennung": "PoolBasisprodukt",
  "stichtag": "31122023"
}
```

#### Test Case 3: Validation Failure (Invalid Date Format)
Trigger the DAG with an invalid date format:
```json
{
  "job_kennung": "PoolBasisprodukt",
  "eintrags_nr": "100234",
  "stichtag": "2023-12-31"
}
```

### 6.2. What "Passing" Means
*   **For Test Case 1**:
    *   The Airflow DAG run completes with a `SUCCESS` status.
    *   A new record is appended to `isbert_schema.PoolBasisprodukt` for `stichtag = '2023-12-31'`.
    *   An entry is written to `audit_log.job_run_log` containing the correct record count and execution timestamp.
    *   No new entries are written to `audit_log.job_error_log`.
*   **For Test Cases 2 & 3**:
    *   The Airflow DAG run fails immediately at the `validate_params` task.
    *   An error entry describing the validation failure is written to `audit_log.job_error_log`.
    *   No data is modified in `isbert_schema.PoolBasisprodukt`.

---

## 7. Rollback Procedure

In the event of an unforeseen production issue, follow these steps to roll back to the legacy environment:

1.  **Pause the Airflow DAG**:
    Go to the Airflow UI and toggle the switch for `k_ausd_bp_ta_cntrct_dist` to **Off** (paused) to prevent any further scheduled or manual executions.
2.  **Re-enable Legacy Scheduler**:
    Uncomment or reactivate the legacy job trigger in the on-premises orchestration engine (e.g., Control-M, UC4, or cron).
3.  **Data Cleanup (Optional)**:
    If the migrated job partially ran and wrote corrupted data to BigQuery, clean up the target table for the affected business date:
    ```sql
    DELETE FROM `gcp-project-id.isbert_schema.PoolBasisprodukt` 
    WHERE stichtag = 'YYYY-MM-DD';
    ```