# Migration Notes: `ausd_bp_ta_bpr_instance`

This document provides the technical details, design decisions, manual setup steps, validation procedures, and rollback strategies for the migrated `ausd_bp_ta_bpr_instance` pipeline.

---

## 1. Summary

The legacy batch job `ausd_bp_ta_bpr_instance` has been migrated from an on-premises environment to the **Google Cloud Platform (GCP)**. 

*   **Legacy Platform**: UC4/Automic Scheduler, KornShell (KSH) wrapper scripts (`r_ausd_bp_ta_bpr_instance.ksh`, `k_ausd_bp_ta_bpr_instance.ksh`), and Oracle SQL\*Plus executing dynamic DML against an Oracle database via DB Link (`@pcrs1`).
*   **Target Platform**: **Google Cloud Composer (Apache Airflow)** for orchestration and **BigQuery** for serverless data warehousing and processing.
*   **Business Objective**: Prepares and filters instantiated basis products (`ta_bpr_instance`) based on active contract states and dynamic watermarks. This data is consumed downstream by BERT scoring models and reporting engines.

---

## 2. Generated Artifacts

The migration process consolidated multiple legacy shell scripts and SQL files into two primary, maintainable artifacts:

| Target Relative Path | Target Language | Role / Description |
| :--- | :--- | :--- |
| `dags/dag_ausd_bp_ta_bpr_instance.py` | Python (Airflow DAG) | Orchestrates the pipeline. It defines the DAG, sets the execution schedule, loads environment variables, and triggers the BigQuery execution task. |
| `sql/d_ausd_bp_ta_bpr_instance.sql` | BigQuery Standard SQL | Contains the core business logic. It dynamically resolves the watermark date (`v_datum`), truncates the target table, and performs an optimized insert-select operation using Common Table Expressions (CTEs) and Temporary User Defined Functions (UDFs). |

---

## 3. Key Design Decisions

### 3.1. Elimination of Shell Scripting (KSH)
*   **Decision**: Completely remove the KornShell wrapper scripts (`r_...ksh` and `k_...ksh`).
*   **Reasoning**: Legacy shell scripts were primarily used for environment setup, parameter validation, and calling SQL\*Plus. In GCP, Apache Airflow natively handles orchestration, parameter passing, and task execution. Removing the shell layer reduces infrastructure complexity, eliminates the need for containerized shell execution environments, and simplifies logging.

### 3.2. Dynamic Watermarking via BigQuery Scripting
*   **Decision**: Implement dynamic SQL scripting (`DECLARE`, `SET`) directly inside the BigQuery SQL file to resolve `v_datum`.
*   **Reasoning**: The legacy script dynamically queried `dwtk_meldungen` to find the watermark timestamp of the `BERT_DROP_TEMP_TABLE` job. By using BigQuery scripting, we keep this logic atomic and self-contained within the SQL layer. Airflow does not need to run a pre-query to pass the date as a parameter, minimizing round-trips and keeping the DAG clean.

### 3.3. Safe String Concatenation (UDF)
*   **Decision**: Created a temporary SQL function `build_iccid` to handle the concatenation of the five ICCID fields.
*   **Reasoning**: In Oracle, concatenating a null value with a string ignores the null (e.g., `'A' || NULL || 'B'` results in `'AB'`). In BigQuery Standard SQL, `CONCAT` returns `NULL` if any argument is `NULL`. The temporary function wraps each component in a `COALESCE(..., '')` to guarantee functional parity and prevent entire ICCID strings from evaluating to `NULL`.

### 3.4. Environment Parameterization via Jinja
*   **Decision**: Use Airflow variables and Jinja templating (`{{ var.value.gcp_project_id }}`) for all dataset and project references.
*   **Reasoning**: This ensures that the exact same SQL and DAG code can be promoted through DEV, QA, and PROD environments without manual modifications.

---

## 4. Manual Steps Before Go-Live

Before enabling the Airflow DAG in production, the following setup steps must be completed:

### 4.1. Schema and Dataset Verification
Ensure that the following BigQuery datasets exist in your target GCP project:
*   `isbert_schema` (or the configured name for tracking tables)
*   `sof` (target dataset)
*   `cds` (source contract dataset)
*   `pds` (source product instance dataset)

Verify that the target table `sof.ta_bpr_instance` exists with the correct schema:
```sql
CREATE TABLE IF NOT EXISTS `your_project.sof.ta_bpr_instance` (
  CNTRCT_ID INT64,
  BPR_ID INT64,
  BPR_INSTANCE_ID INT64,
  ICCID STRING,
  IMSI_MCC STRING,
  IMSI_MNC STRING,
  IMSI_HLR STRING,
  IMSI_SI STRING,
  CNTRCT_ID_REF INT64
);
```

### 4.2. IAM & Permissions
The service account running the Cloud Composer workers must have the following IAM roles:
*   **BigQuery Data Editor** (`roles/bigquery.dataEditor`) on the target dataset (`sof`).
*   **BigQuery Data Viewer** (`roles/bigquery.dataViewer`) on the source datasets (`cds`, `pds`, `isbert_schema`).
*   **BigQuery Job User** (`roles/bigquery.jobUser`) at the project level to execute queries.

### 4.3. Airflow Variables Configuration
Configure the following Airflow Variables in the Composer environment (Admin -> Variables):

| Variable Key | Example Value | Description |
| :--- | :--- | :--- |
| `gcp_project_id` | `gcp-dwh-prod` | The target GCP Project ID. |
| `isbert_dataset` | `isbert_schema` | Dataset containing `dwtk_meldungen`. |
| `sof_dataset` | `sof` | Dataset containing `ta_bpr_instance`. |
| `cds_dataset` | `cds` | Dataset containing `ta_cntrct`. |
| `pds_dataset` | `pds` | Dataset containing `ta_bpri_com`. |

### 4.4. Connection Strings
Ensure the Airflow connection `google_cloud_default` is properly configured and points to the correct GCP environment.

### 4.5. Upstream Replication Verification
The legacy DB Link (`@pcrs1`) has been replaced by pre-replicated tables in BigQuery (`cds.ta_cntrct` and `pds.ta_bpri_com`). **You must verify that the replication pipelines for these tables are running successfully and up-to-date before enabling this DAG.**

### 4.6. Scheduling Coordination
*   Deactivate the legacy UC4 job `AUSD_BP_TA_BPR_INSTANCE` to prevent concurrent writes.
*   Unpause the migrated Airflow DAG `dw_bert_ausd_bp_ta_bpr_instance`.

---

## 5. Known Gaps & Unresolved References

1.  **Upstream Replication Dependency**: This pipeline assumes that `cds.ta_cntrct` and `pds.ta_bpri_com` are already replicated from the source database. If the replication pipeline lags, this job will process stale data.
2.  **Timezone Alignment**: The legacy Oracle database and UC4 scheduler operated on Central European Time (CET/CEST). BigQuery natively stores and processes timestamps in UTC. While `DATE()` conversions have been applied, minor discrepancies could occur around timezone boundaries if the upstream replication does not normalize timestamps to UTC.
3.  **B4 Redesign Candidate**: The tracking table `dwtk_meldungen` is a legacy pattern. In a future phase, this pull-based watermark check should be replaced by native Airflow execution date partitioning (`{{ ds }}`) once the upstream systems are fully modernized.

---

## 6. Validation

To validate the migration in the QA/UAT environment, perform the following steps:

### 6.1. Execution Test
1.  Go to the Airflow UI.
2.  Locate the DAG `dw_bert_ausd_bp_ta_bpr_instance`.
3.  Trigger the DAG manually.
4.  Verify that the task `run_d_ausd_bp_ta_bpr_instance` completes with a `success` status.

### 6.2. Data Parity Validation
Run the following validation query in BigQuery and compare the results against the legacy Oracle database for the same watermark date:

```sql
-- 1. Row Count Validation
SELECT COUNT(1) FROM `your_project.sof.ta_bpr_instance`;

-- 2. Sample Check for Null Concatenation Prevention
SELECT ICCID 
FROM `your_project.sof.ta_bpr_instance` 
WHERE ICCID LIKE '%-%-%-%-%' 
LIMIT 10;

-- 3. Check for unexpected NULLs in business keys
SELECT COUNT(1) 
FROM `your_project.sof.ta_bpr_instance` 
WHERE CNTRCT_ID IS NULL OR BPR_ID IS NULL;
```

**Definition of "Passing"**:
*   The DAG runs to completion without errors.
*   The row count in BigQuery matches the legacy Oracle table row count within a 0% tolerance (assuming identical source data states).
*   The `ICCID` format matches the legacy pattern exactly, with no unexpected `NULL` values resulting from concatenation.

---

## 7. Rollback Procedure

If critical issues or data discrepancies are discovered post-go-live, execute the following rollback steps:

1.  **Pause the Airflow DAG**:
    Go to the Airflow UI and toggle the switch for `dw_bert_ausd_bp_ta_bpr_instance` to **Off**.
2.  **Re-enable Legacy Scheduler**:
    Re-activate the legacy UC4 job `AUSD_BP_TA_BPR_INSTANCE`.
3.  **Data Restoration (Optional)**:
    If downstream systems consumed corrupted data from BigQuery, you can restore the target table to its state prior to the DAG run using BigQuery's time-travel feature:
    ```sql
    -- Restore table to its state 1 hour ago
    CREATE OR REPLACE TABLE `your_project.sof.ta_bpr_instance`
    AS SELECT * FROM `your_project.sof.ta_bpr_instance`
    FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
    ```