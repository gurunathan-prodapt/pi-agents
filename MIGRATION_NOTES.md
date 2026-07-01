# Migration Notes: `ausd_bp_ta_bpr_opt_text`

This document details the migration of the batch job `ausd_bp_ta_bpr_opt_text` from the legacy on-premises environment to Google Cloud Platform (GCP).

---

## 1. Summary

The `ausd_bp_ta_bpr_opt_text` batch job has been migrated from a legacy architecture managed by the UC4/Automic scheduler, KornShell (Ksh) orchestration, and Oracle PL/SQL to a modern, cloud-native architecture on **Google Cloud Platform (GCP)**.

*   **Source Platform**: UC4/Automic Scheduler, KornShell (`.ksh`), Oracle 11g/12c Database
*   **Target Platform**: Google Cloud Composer (Apache Airflow) & Google BigQuery
*   **Core Business Logic**: This job performs an ETL process that truncates the target table `sof_ta_bpr_opt_text` and repopulates it by joining contract-level options (`sof_ta_bpr_optionen`) with their descriptive lookup texts (`sof_ta_bpr_beschr`).

### Target Architecture

```
   [ Cloud Composer / Apache Airflow ]
                    │
                    ▼
     [ BigQuery Execute Operator ]
                    │
         ┌──────────┴──────────┐
         ▼                     ▼
[ Truncate Target ]   [ JOIN & Insert ]
 sof_ta_bpr_opt_text   - sof_ta_bpr_optionen
                       - sof_ta_bpr_beschr
```

---

## 2. Generated Artifacts

The migration process consolidated four legacy files into two clean, maintainable target artifacts:

| Artifact Path | Type | Role / Description | Replaces |
| :--- | :--- | :--- | :--- |
| `dags/dag_ausd_bp_ta_bpr_opt_text.py` | Python (Airflow DAG) | Orchestrates the workflow, resolves environment-specific variables, reads the SQL script, and triggers execution in BigQuery. | `DW.BERT_AUSD_BP_TA_BPR_OPT_TEXT.xml`<br>`r_ausd_bp_ta_bpr_opt_text.ksh`<br>`k_ausd_bp_ta_bpr_opt_text.ksh` |
| `gcp/bigquery/sql/d_ausd_bp_ta_bpr_opt_text.sql` | BigQuery SQL | Contains the native BigQuery DDL/DML statements to truncate the target table and perform the high-performance join. | `d_ausd_bp_ta_bpr_opt_text.sql` (Oracle PL/SQL) |

---

## 3. Key Design Decisions

### 3.1. Consolidation of Orchestration Layers
The legacy architecture utilized a multi-layered execution chain (UC4 XML -> Wrapper Shell Script -> Controller Shell Script -> Oracle SQL). This has been consolidated into a single **Apache Airflow DAG** that directly executes the SQL logic in BigQuery. This reduces operational complexity, eliminates shell-script maintenance overhead, and provides native logging and alerting.

### 3.2. Table Naming and Schema Compliance
BigQuery does not support or recommend special characters such as `$` in table names. 
*   **Decision**: All legacy tables containing `$` have been renamed to use underscores (`_`).
    *   `sof$ta_bpr_opt_text` $\rightarrow$ `sof_ta_bpr_opt_text`
    *   `sof$ta_bpr_optionen` $\rightarrow$ `sof_ta_bpr_optionen`
    *   `sof$ta_bpr_beschr` $\rightarrow$ `sof_ta_bpr_beschr`

### 3.3. Dynamic Environment Resolution
To prevent hardcoding project IDs and dataset names across Dev, Test, and Prod environments, the Airflow DAG uses a `PythonOperator` to dynamically read the SQL file and replace placeholders (`${gcp_project_id}` and `${target_dataset}`) with environment variables or runtime configurations. An inline SQL fallback is provided to ensure local development and testing can proceed without GCS file dependencies.

### 3.4. Elimination of Redundant and Dead Code
*   **Oracle Hints**: Legacy optimizer hints (e.g., `/*+ full(...) parallel(...) */`) have been removed as BigQuery's execution engine automatically optimizes query plans.
*   **Inactive Code**: Commented-out legacy logic in `k_ausd_bp_ta_bpr_opt_text.ksh` that performed local file merges using `sed`, `sort`, and `join` (manipulating `cibasis_data24.dat`) was verified as obsolete and excluded from the migration.

---

## 4. Manual Steps Before Go-Live

The following setup steps must be completed in the target GCP environment before triggering the pipeline:

### 4.1. BigQuery Dataset & Table Creation
Ensure the target dataset exists in BigQuery. If the tables do not exist, they must be initialized with schemas matching the legacy structures (with `$` replaced by `_`):

```sql
CREATE TABLE IF NOT EXISTS `your_project.your_dataset.sof_ta_bpr_opt_text` (
  CNTRCT_ID STRING,
  BPR_ID STRING,
  PDS_DESCRIPTION STRING
);
```

### 4.2. IAM & Permissions
The Cloud Composer Service Account (e.g., `service-account@your-project.iam.gserviceaccount.com`) must be granted the following IAM roles:
*   **BigQuery Data Editor** (`roles/bigquery.dataEditor`) on the target dataset.
*   **BigQuery Job User** (`roles/bigquery.jobUser`) on the GCP project.
*   **Storage Object Viewer** (`roles/storage.objectViewer`) on the Composer GCS bucket (to read the SQL file).

### 4.3. Airflow Connections & Variables
1.  **GCP Connection**: Ensure the Airflow connection `google_cloud_default` is configured and points to the correct GCP project.
2.  **Environment Variables**: Define the following environment variables in Cloud Composer:
    *   `GCP_PROJECT_ID`: The target GCP project ID.
    *   `BQ_DATASET`: The target BigQuery dataset name.
    *   `BQ_LOCATION`: The geographic location of the dataset (e.g., `EU` or `US`).

### 4.4. Scheduling & Upstream Dependencies
The DAG is currently configured with `schedule_interval=None`. 
*   **Action**: Integrate this DAG into the master orchestration schedule. It must be configured to run only after the upstream tables `sof_ta_bpr_optionen` and `sof_ta_bpr_beschr` have completed their daily loads.

---

## 5. Known Gaps & Unresolved References

### 5.1. Upstream Audit Table Dependency (`dwtk_meldungen`)
The migrated SQL script queries `${gcp_project_id}.${target_dataset}.dwtk_meldungen` to determine the latest execution date of the legacy job `BERT_DROP_TEMP_TABLE`.
*   **Gap**: This assumes that the process populating `dwtk_meldungen` has already been migrated to GCP.
*   **Mitigation**: If the upstream process is not yet migrated, the query will default to `'19000101'`. Ensure the migration sequence prioritizes the job populating `dwtk_meldungen` or mock the required metadata row in BigQuery.

### 5.2. Legacy CSV File Generation (B4 Redesign Item)
The legacy shell script contained commented-out logic for generating a local CSV file (`cibasisprodukt.csv`). 
*   **Gap**: If any downstream legacy system still expects this physical file on an on-premises mount, it will fail.
*   **Mitigation**: If downstream systems require this file, a GCS-to-On-Premises transfer or a BigQuery-to-GCS export task (`BigQueryToGCSOperator`) must be added to the DAG.

---

## 6. Validation

To validate the migration, execute the following steps in the test environment:

### 6.1. Dry-Run Validation
Run a dry-run of the SQL script in the BigQuery console to verify syntax and schema compatibility:
```sql
-- Replace placeholders with actual values before running
DECLARE v_datum STRING;
SET v_datum = (
  SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(DATE(timecreated))), '19000101')
  FROM `your_project.your_dataset.dwtk_meldungen`
  WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
);
```

### 6.2. Execution Testing
1.  Trigger the DAG `dag_ausd_bp_ta_bpr_opt_text` manually from the Airflow UI.
2.  Verify that both tasks (`build_sql_query` and `process_basisprodukte_bq`) complete with a `success` status.

### 6.3. Data Reconciliation (Passing Criteria)
The migration is considered successful if:
*   The target table `sof_ta_bpr_opt_text` is successfully truncated and reloaded.
*   The row count in `sof_ta_bpr_opt_text` matches the expected join results:
    ```sql
    SELECT COUNT(*) FROM `your_project.your_dataset.sof_ta_bpr_optionen` bp
    JOIN `your_project.your_dataset.sof_ta_bpr_beschr` bs ON bp.bpr_id = bs.bpr_id;
    ```
*   No null values exist in key columns (`CNTRCT_ID`, `BPR_ID`).

---

## 7. Rollback Procedure

In the event of a critical failure during deployment or execution:

### 7.1. GCP Rollback
1.  **Pause the DAG**: Immediately pause the `dag_ausd_bp_ta_bpr_opt_text` DAG in the Airflow UI to prevent further scheduled runs.
2.  **Table Restore**: If the target table was corrupted or truncated incorrectly, restore the table to its pre-execution state using BigQuery's time travel feature:
    ```sql
    CREATE OR REPLACE TABLE `your_project.your_dataset.sof_ta_bpr_opt_text`
    AS SELECT * FROM `your_project.your_dataset.sof_ta_bpr_opt_text`
    FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
    ```

### 7.2. Legacy Fallback
If a complete fallback to the legacy environment is required:
1.  Re-enable the UC4 job `DW.BERT_AUSD_BP_TA_BPR_OPT_TEXT.xml`.
2.  Verify that the legacy Oracle database schemas are online and synchronized.
3.  Trigger the UC4 job to resume on-premises processing.