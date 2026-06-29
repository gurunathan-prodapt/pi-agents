# Migration Notes: `ausd_bp_ta_bpr_bcp`

This document provides the comprehensive migration notes for transitioning the legacy UC4 and KornShell-based job **`ausd_bp_ta_bpr_bcp`** to Google Cloud Platform (GCP) using **BigQuery** and **Cloud Composer (Apache Airflow)**.

---

## 1. Summary

The legacy job `ausd_bp_ta_bpr_bcp` has been migrated from an on-premises Oracle and UC4/Automic scheduler environment to a modern, cloud-native architecture on GCP.

*   **Source Platform**: Oracle Database, UC4/Automic Scheduler, KornShell (ksh) scripts.
*   **Target Platform**: Google Cloud Platform (GCP)
    *   **Orchestration**: Cloud Composer (Apache Airflow)
    *   **Data Warehouse**: Google BigQuery
*   **Business Purpose**: This job provisions the "BusinessCardPackage" (BCP) instantiated base products for BERT (Forderungsscoring/Collection Management). It filters base product instances to extract, deduplicate, and load contracts associated with the specific base product ID `3142` ("Zusatzkarte im BusinessCardPackage") into the target table `sof_ta_bpr_bcp`. This ensures downstream scoring processes have access to clean, deduplicated contract structures.

---

## 2. Generated Artifacts

The migration process has consolidated the legacy multi-layered shell scripts and UC4 XML definitions into two primary, maintainable artifacts:

### 1. BigQuery SQL Script
*   **File Path**: `src/sql/d_ausd_bp_ta_bpr_bcp.sql`
*   **Role**: Contains the complete data transformation and loading logic written in Google Standard SQL. It replaces the legacy Oracle SQL script (`d_ausd_bp_ta_bpr_bcp.sql`) and the control script logic (`k_ausd_bp_ta_bpr_bcp.ksh`).
*   **Key Operations**:
    *   Declares and resolves the legacy metadata variable `v_datum` from `dwtk_meldungen` for audit trail preservation.
    *   Truncates the target table `sof_ta_bpr_bcp`.
    *   Performs a bulk `INSERT` of deduplicated (`DISTINCT`) records from `sof_ta_bpr_instance` where `bpr_id = '3142'`.

### 2. Airflow DAG Python Script
*   **File Path**: `src/dags/dw_bert_ausd_bp_ta_bpr_bcp.py`
*   **Role**: Orchestrates the execution of the BigQuery SQL script. It replaces the legacy UC4 job definition (`DW.BERT_AUSD_BP_TA_BPR_BCP.xml`) and the KornShell wrapper scripts (`r_ausd_bp_ta_bpr_bcp.ksh`, `k_ausd_bp_ta_bpr_bcp.ksh`).
*   **Key Operations**:
    *   Defines the execution schedule, retries, and alerting configurations.
    *   Uses the `BigQueryExecuteQueryOperator` to run the SQL logic as a single, atomic transaction block in BigQuery.

---

## 3. Key Design Decisions

### Consolidation of Orchestration and Scripting Layers
*   **Decision**: Replaced the legacy three-tier execution model (UC4 Job -> Wrapper Shell Script -> Control Shell Script -> Oracle SQL) with a single Airflow DAG executing a native BigQuery SQL statement.
*   **Reasoning**: This drastically reduces operational complexity, eliminates the maintenance overhead of shell scripts, and leverages Airflow's native logging, retry mechanisms, and alerting capabilities.

### Schema Sanitization and Naming Conventions
*   **Decision**: Converted the legacy Oracle table name `sof$ta_bpr_bcp` to `sof_ta_bpr_bcp` (replacing the special character `$` with `_`).
*   **Reasoning**: BigQuery does not support special characters like `$` in table identifiers. Standardizing on underscores ensures compatibility with Google Standard SQL and improves readability.

### Removal of Database Optimizer Hints
*   **Decision**: Stripped Oracle-specific optimizer hints (e.g., `/*+ full(bp) parallel(bp,4) */`) from the SQL statement.
*   **Reasoning**: BigQuery is a serverless, highly parallelized data warehouse that automatically optimizes query execution plans. Manual execution hints are obsolete and unsupported in BigQuery.

### Preservation of Legacy Metadata Lookup
*   **Decision**: Retained the query to retrieve `v_datum` from `dwtk_meldungen` using a local SQL variable, even though the variable is not actively used in the subsequent `INSERT` statement.
*   **Reasoning**: This preserves the exact lineage and schema read dependencies of the legacy job, ensuring that any automated audit checks or dependency tracking tools continue to function without breaking during the initial migration phase.

---

## 4. Manual Steps Before Go-Live

Before deploying and enabling the Airflow DAG, the following manual setup steps must be completed in the target GCP environment:

### 1. Schema and Table Creation
Ensure that the target dataset and tables exist in BigQuery. If they do not exist, execute the following DDL statements in your BigQuery console:

```sql
-- Create the dataset (if not already present)
-- CREATE SCHEMA IF NOT EXISTS `your_project_id.isbert_schema` OPTIONS(location="EU");

-- Create Target Table
CREATE TABLE IF NOT EXISTS `isbert_schema.sof_ta_bpr_bcp`
(
  cntrct_id STRING OPTIONS(description="Contract Identifier"),
  bpr_id STRING OPTIONS(description="Base Product Identifier"),
  cntrct_id_ref STRING OPTIONS(description="Reference Contract Identifier")
)
OPTIONS(
  description="Deduplicated BusinessCardPackage (BCP) contracts table"
);
```

### 2. IAM & Permissions
The service account running the Cloud Composer workers must have the following IAM roles assigned:
*   **`roles/bigquery.jobUser`** on the project level (to run BigQuery jobs).
*   **`roles/bigquery.dataEditor`** on the `isbert_schema` dataset (to truncate and insert data into `sof_ta_bpr_bcp`).
*   **`roles/bigquery.dataViewer`** on the source dataset containing `sof_ta_bpr_instance` and `dwtk_meldungen` (if they reside in a different dataset).

### 3. Airflow Connections
*   Ensure the default Google Cloud connection (`google_cloud_default`) is properly configured in Airflow with the correct Project ID and Service Account key (if running outside of GKE/Composer-managed environments).

### 4. Scheduling and Upstream Dependencies
*   The DAG is scheduled to run daily at **04:00 AM UTC** (`0 4 * * *`).
*   Verify that the upstream processes populating `isbert_schema.sof_ta_bpr_instance` and `isbert_schema.dwtk_meldungen` are scheduled to complete well before 04:00 AM UTC to prevent processing stale data.

---

## 5. Known Gaps & Unresolved References

### Unused Variable `v_datum` (Redesign B4 Item)
*   **Description**: The SQL script queries `dwtk_meldungen` to set `v_datum` but never utilizes this variable in the filtering logic of the `INSERT` statement. This is a legacy carryover from older versions of the job.
*   **Impact**: Low. It introduces a minor, unnecessary read operation on `dwtk_meldungen`.
*   **Follow-up / Redesign**: In a future optimization phase, if `dwtk_meldungen` is deprecated or no longer populated in GCP, this variable declaration and its associated query should be completely removed from the SQL script.

### Downstream References to `sof$ta_bpr_bcp`
*   **Description**: Any legacy downstream jobs, scripts, or BI tools that still reference the Oracle table name `sof$ta_bpr_bcp` must be updated.
*   **Impact**: High if not addressed; downstream queries will fail.
*   **Follow-up**: Ensure all downstream consumers are refactored to query the new BigQuery table `isbert_schema.sof_ta_bpr_bcp`.

---

## 6. Validation

To validate the migration and ensure the job is functioning correctly, perform the following steps:

### 1. Dry-Run Validation
Run a dry-run of the SQL script in the BigQuery Console to verify syntax and access permissions:
*   Copy the contents of `src/sql/d_ausd_bp_ta_bpr_bcp.sql` into the BigQuery SQL workspace.
*   Click **More** -> **Query settings** -> Check **Dry run**.
*   Verify that the query validator returns a green checkmark indicating the query is valid.

### 2. DAG Execution Test
*   Upload the DAG file `dw_bert_ausd_bp_ta_bpr_bcp.py` to the Cloud Composer DAGs folder.
*   Access the Airflow UI, locate the DAG `dw_bert_ausd_bp_ta_bpr_bcp`, and trigger it manually.
*   Verify that the task `execute_bpr_bcp_processing` completes with a `success` status.

### 3. Data Reconciliation (Passing Criteria)
Run the following validation queries to verify data integrity:

```sql
-- Query 1: Count of expected source records
SELECT COUNT(DISTINCT cntrct_id) AS expected_count
FROM `isbert_schema.sof_ta_bpr_instance`
WHERE bpr_id = '3142';

-- Query 2: Count of migrated target records
SELECT COUNT(*) AS actual_count
FROM `isbert_schema.sof_ta_bpr_bcp`;
```

*   **Passing Criteria**: The `actual_count` from Query 2 must **exactly match** the `expected_count` from Query 1.
*   Verify that no records with `bpr_id` other than `'3142'` exist in the target table:
    ```sql
    SELECT COUNT(*) FROM `isbert_schema.sof_ta_bpr_bcp` WHERE bpr_id != '3142'; -- Must return 0
    ```

---

## 7. Rollback Procedure

In the event of a critical failure or data corruption during the go-live window, execute the following rollback steps:

1.  **Pause the Airflow DAG**:
    *   Open the Airflow UI.
    *   Locate `dw_bert_ausd_bp_ta_bpr_bcp` and toggle the switch to **Off** (Paused) to prevent further scheduled executions.
2.  **Revert to Legacy Scheduler**:
    *   Re-enable the UC4 job `DW.BERT_AUSD_BP_TA_BPR_BCP` in the legacy environment.
3.  **Clean Up Target BigQuery Table**:
    *   If the BigQuery table was partially populated or contains corrupted data, truncate it to avoid downstream confusion:
        ```sql
        TRUNCATE TABLE `isbert_schema.sof_ta_bpr_bcp`;
        ```
    *   Alternatively, if you need to restore the table to its state prior to the DAG run, use BigQuery's Time Travel feature:
        ```sql
        -- Restore table to its state 1 hour ago
        CREATE OR REPLACE TABLE `isbert_schema.sof_ta_bpr_bcp`
        AS SELECT * FROM `isbert_schema.sof_ta_bpr_bcp`
        FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
        ```