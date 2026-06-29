# Migration Notes: k_ausd_bp_ta_cntrct_dist.ksh

This document details the migration of the legacy ISBERT data preparation script `k_ausd_bp_ta_cntrct_dist.ksh` and its associated SQL logic to Google Cloud Platform (GCP).

---

## 1. Summary

The legacy KornShell (Ksh) script `k_ausd_bp_ta_cntrct_dist.ksh` and its associated Oracle SQL*Plus execution logic (`d_ausd_bp_ta_cntrct_dist.sql`) have been migrated to a modern, cloud-native architecture on **Google Cloud Platform (GCP)**.

*   **Source Platform**: On-premises Unix/Linux running KornShell (Ksh), Oracle SQL*Plus, and custom shell-based utility frameworks (`gestern.ksh`, `FOSJobErzeugeEintrag`).
*   **Target Platform**: **Google Cloud Composer (Apache Airflow 2.x)** for orchestration and **Google BigQuery** for serverless data warehousing, transformation, and storage.
*   **Business Domain**: ISBERT (Information System Business Reporting) — specifically, data preparation and contract distribution processing for basic product transaction accounts (`PoolBasisprodukt`).

---

## 2. Generated Artifacts

The migration process generated the following target artifacts:

| Artifact Path | Target Technology | Role / Description |
| :--- | :--- | :--- |
| `src/sql/ddl/job_audit_log.sql` | BigQuery DDL | Creates the centralized, partitioned, and clustered audit log table (`job_audit_log`) to replace legacy shell-based bookkeeping. |
| `src/sql/ddl/pool_basisprodukt.sql` | BigQuery DDL | Creates the target table (`PoolBasisprodukt`) and the staging table (`PoolBasisprodukt_Staging`) with optimized partitioning and clustering. |
| `src/sql/procedures/sp_d_ausd_bp_ta_cntrct_dist.sql` | BigQuery SQL (Stored Procedure) | Encapsulates parameter validation, date calculations (replacing `gestern.ksh`), core transformation logic, and transactional audit logging. |
| `src/dags/dag_k_ausd_bp_ta_cntrct_dist.py` | Apache Airflow (Python) | Orchestrates the workflow. Validates input parameters, handles task dependencies, and executes the BigQuery stored procedure. |

---

## 3. Key Design Decisions

### 3.1. Orchestration Shift (Airflow DAG)
*   **Decision**: Replace the KornShell wrapper with an Apache Airflow DAG.
*   **Reasoning**: Airflow provides native scheduling, centralized monitoring, automated retries, parameter validation, and clear visibility into execution states, replacing fragile cron- or shell-based scheduling.

### 3.2. Push-Down Computation (BigQuery Stored Procedure)
*   **Decision**: Encapsulate the core transformation and date validation logic inside a BigQuery Stored Procedure (`sp_d_ausd_bp_ta_cntrct_dist`) rather than performing calculations in Python or shell.
*   **Reasoning**: Minimizes data movement, leverages BigQuery's serverless execution engine, and keeps the data transformation logic close to the data.

### 3.3. Idempotency and "Delete-Insert" Pattern
*   **Decision**: The stored procedure automatically deletes existing records in the target table for the given `stichtag` before inserting new ones.
*   **Reasoning**: This ensures that if a job fails mid-run or needs to be re-run for a specific reporting date, it can be safely retried without duplicating data or leaving partial loads.

### 3.4. Native Date Calculations
*   **Decision**: Replace the legacy `gestern.ksh` utility with BigQuery's native `DATE_SUB` and `PARSE_DATE` functions.
*   **Reasoning**: Eliminates external file-system and shell dependencies, making the SQL code self-contained and highly portable.

### 3.5. Omission of Commented Legacy Code
*   **Decision**: Commented-out shell operations (`sed`, `sort`, `join` on CSV files) in the legacy script were excluded from the active migration.
*   **Reasoning**: These operations were inactive in the source production environment. If this logic must be reinstated, it should be implemented as native BigQuery SQL joins over staging tables rather than file-level manipulations.

---

## 4. Manual Steps Before Go-Live

Before deploying and running the migrated pipeline in production, complete the following setup steps:

### 4.1. Schema and Dataset Creation
1.  Ensure the target BigQuery dataset (e.g., `project.dataset`) exists in your GCP project.
2.  Execute the DDL scripts in the following order to create the tables:
    *   `src/sql/ddl/job_audit_log.sql`
    *   `src/sql/ddl/pool_basisprodukt.sql`

### 4.2. Stored Procedure Deployment
1.  Deploy the stored procedure by executing the SQL script `src/sql/procedures/sp_d_ausd_bp_ta_cntrct_dist.sql` in the target BigQuery dataset.

### 4.3. IAM & Permissions
Ensure that the service account used by Cloud Composer / Airflow has the following IAM roles:
*   `roles/bigquery.dataEditor` on the target dataset (to write to target and audit tables).
*   `roles/bigquery.jobUser` on the GCP project (to run BigQuery query jobs).

### 4.4. Airflow DAG Deployment
1.  Upload `src/dags/dag_k_ausd_bp_ta_cntrct_dist.py` to the `dags/` folder of your Cloud Composer environment's GCS bucket.
2.  Verify that the DAG parses successfully in the Airflow UI without import errors.

---

## 5. Known Gaps & Unresolved References

1.  **Upstream Staging Dependency**:
    *   The stored procedure reads from `project.dataset.PoolBasisprodukt_Staging`. This staging table must be populated by an upstream ingestion process (e.g., a file-to-BigQuery load job) before this DAG is triggered.
2.  **Inner SQL Logic Verification (B4 Redesign Item)**:
    *   *Gap*: The original Oracle SQL file `d_ausd_bp_ta_cntrct_dist.sql` was not fully provided in the source package.
    *   *Action*: The stored procedure currently implements a standard template load from staging to target. If the original Oracle SQL contained complex business transformations, custom joins, or Oracle-specific functions (e.g., `DECODE`, `NVL`, `(+)` outer joins), they must be manually ported into the `INSERT INTO` block of `sp_d_ausd_bp_ta_cntrct_dist.sql`.

---

## 6. Validation

To validate the migration, perform both manual and automated test executions.

### 6.1. Unit Testing the Stored Procedure
Run the following SQL command directly in the BigQuery console to test parameter validation and execution:

```sql
-- Test successful run
CALL `project.dataset.sp_d_ausd_bp_ta_cntrct_dist`('JOB_TEST_01', 'ENTRY_01', '31122023', '');

-- Test invalid date format validation (should fail)
CALL `project.dataset.sp_d_ausd_bp_ta_cntrct_dist`('JOB_TEST_01', 'ENTRY_01', '2023-12-31', '');
```

### 6.2. End-to-End DAG Testing
1.  Open the Airflow UI.
2.  Locate `dag_k_ausd_bp_ta_cntrct_dist` and click **Trigger DAG w/ config**.
3.  Provide the following JSON configuration:
    ```json
    {
      "p_JobKennung": "MIG_TEST_JOB",
      "p_EintragsNr": "99999",
      "p_Stichtag": "31122023",
      "p_wiederanlaufWert": ""
    }
    ```
4.  Trigger the execution.

### 6.3. Definition of "Passing"
The migration is considered successful and validated if:
*   The Airflow DAG runs and completes with a `SUCCESS` status.
*   The target table `project.dataset.PoolBasisprodukt` contains the expected rows for `stichtag = '2023-12-31'`.
*   The `project.dataset.job_audit_log` table contains a corresponding entry with `status = 'S'`, `record_count` matching the inserted rows, and the correct metadata.
*   Passing an invalid date (e.g., `20231231`) or omitting a mandatory parameter causes the DAG to fail cleanly during the validation task or stored procedure execution, logging a `status = 'F'` entry in the audit log.

---

## 7. Rollback Procedure

In the event of a critical failure or data corruption during deployment or post-go-live execution, follow these rollback steps:

### 7.1. Data Rollback
To remove data written by a faulty run for a specific reporting date (`Stichtag`), execute the following query in BigQuery:

```sql
DELETE FROM `project.dataset.PoolBasisprodukt`
WHERE stichtag = PARSE_DATE('%d%m%Y', 'DDMMYYYY'); -- Replace DDMMYYYY with the target date
```

### 7.2. Code Rollback
1.  **Pause the Airflow DAG**: In the Airflow UI, toggle the DAG switch to **Off** to prevent further scheduled or manual executions.
2.  **Remove DAG File**: Delete `dag_k_ausd_bp_ta_cntrct_dist.py` from the Composer GCS `dags/` bucket.
3.  **Revert to Legacy**: If necessary, re-enable the legacy KornShell script execution in the legacy scheduler (e.g., UC4/Automic or cron).