# Migration Notes: ausd_bp_ta_rn_einzeln

This document provides comprehensive migration notes for the job `ausd_bp_ta_rn_einzeln`, detailing its transition from a legacy Oracle and UC4 environment to Google Cloud Platform (BigQuery and Cloud Composer / Apache Airflow).

---

## 1. Summary

The `ausd_bp_ta_rn_einzeln` workload has been migrated from an on-premises Oracle DWH environment orchestrated by UC4 to **Google Cloud Platform (GCP)**. 

*   **Source Platform:** Oracle Database, UC4 (Automic) Scheduler, KornShell (KSH) scripts.
*   **Target Platform:** Google BigQuery (Serverless Data Warehouse), Google Cloud Composer (Apache Airflow).
*   **Workload Function:** Resolves contract call number roles for multiple product groups (including voice, data, fax, multiSIM, and others) by joining base product instances (`sof_ta_bpr_basis`) with standard MSISDN assignments (`sof_ta_msisdn`). The final output is written to the target table `sof_ta_rn_einzeln`.

---

## 2. Generated Artifacts

The migration process consolidated multiple legacy components (one UC4 XML definition, two KornShell wrappers, and one Oracle SQL*Plus script) into two clean, maintainable target files:

| Target File Path | Target Language | Role / Description |
| :--- | :--- | :--- |
| `dags/dag_ausd_bp_ta_rn_einzeln.py` | Python (Airflow DAG) | Orchestrates the pipeline. It defines the DAG structure, execution parameters, and triggers the BigQuery SQL execution. |
| `gcp_sql/d_ausd_bp_ta_rn_einzeln.sql` | BigQuery Standard SQL | Contains the core data transformation logic. It dynamically extracts the execution date variable (`v_datum`), truncates the target table, and performs the complex conditional join to populate `sof_ta_rn_einzeln`. |

---

## 3. Key Design Decisions

### 3.1. Consolidation of Orchestration
*   **Decision:** The legacy UC4 XML job definition and the two KornShell wrapper scripts (`r_ausd_bp_ta_rn_einzeln.ksh` and `k_ausd_bp_ta_rn_einzeln.ksh`) were unified into a single Airflow DAG.
*   **Reasoning:** Legacy shell scripts primarily handled environment setup, parameter parsing, and SQL*Plus invocation. Airflow's native `BigQueryExecuteQueryOperator` eliminates the need for shell-level wrappers, reducing execution overhead and simplifying error handling.

### 3.2. Dynamic Variable Extraction (`v_datum`)
*   **Decision:** Replaced Oracle SQL*Plus command-line substitution (`&v_datum`) with a native BigQuery SQL `DECLARE` statement.
*   **Reasoning:** In the legacy system, the date parameter was queried from `dwtk_meldungen` and passed into the SQL script via shell variables. In BigQuery, we declare `v_datum` directly at the top of the SQL script:
    ```sql
    DECLARE v_datum STRING DEFAULT (
      SELECT COALESCE(MAX(FORMAT_DATE('%Y%m%d', DATE(timecreated))), '19000101')
      FROM `{{ var.value.gcp_project_id }}.{{ var.value.bq_dataset }}.dwtk_meldungen`
      WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
    );
    ```
    This keeps the SQL script self-contained, highly testable, and independent of external shell-level parameter injection.

### 3.3. Atomic Truncate-and-Insert
*   **Decision:** Replaced the legacy Oracle PL/SQL custom utility call (`DWPA_UTIL_SKRIPT.runstatement` executing `TRUNCATE TABLE ... REUSE STORAGE`) with an explicit BigQuery `TRUNCATE TABLE` statement followed by an `INSERT INTO` block.
*   **Reasoning:** BigQuery handles transactions and table replacements natively. Performing an explicit `TRUNCATE` followed by an `INSERT` ensures that the schema remains intact and the table is cleanly repopulated on every run.

### 3.4. Removal of Optimizer Hints
*   **Decision:** All Oracle-specific parallel and full-table scan hints (e.g., `/*+ parallel(bp,4) */`) were stripped from the SQL.
*   **Reasoning:** BigQuery is a serverless columnar database that automatically manages query parallelization and execution plans. Database hints are non-functional and syntactically invalid in BigQuery Standard SQL.

---

## 4. Manual Steps Before Go-Live

Before deploying and running the migrated pipeline in a production environment, the following manual setup steps must be completed:

### 4.1. Schema & Dataset Creation
Ensure that the target BigQuery dataset exists in your project.
1.  Create the dataset (e.g., `isbert_schema`) in the desired region (e.g., `EU`).
2.  Pre-create the target table `sof_ta_rn_einzeln` with the correct schema, or verify that the upstream tables (`sof_ta_bpr_basis`, `sof_ta_msisdn`, and `dwtk_meldungen`) are fully migrated and populated.

### 4.2. Airflow Variables & Connections
Configure the following Airflow variables in the Cloud Composer environment (via Airflow UI -> Admin -> Variables):

*   `gcp_project_id`: The ID of the GCP project hosting the BigQuery datasets (e.g., `prod-dwh-bert-1`).
*   `bq_dataset`: The name of the target BigQuery dataset (e.g., `isbert_schema`).
*   `bq_location`: The geographic location of the dataset (e.g., `EU` or `US`).

Ensure that the Airflow connection `google_cloud_default` is configured with a Service Account possessing the following IAM roles:
*   **BigQuery Data Editor** (on the target dataset)
*   **BigQuery Job User** (on the project level)

### 4.3. Code Deployment
1.  Upload `d_ausd_bp_ta_rn_einzeln.sql` to the Cloud Composer environment's GCS bucket under the `dags/gcp_sql/` directory.
2.  Upload `dag_ausd_bp_ta_rn_einzeln.py` to the `dags/` directory of the same bucket.

### 4.4. Scheduling
The DAG is currently configured with `schedule_interval=None` (manual/triggered execution) to match the legacy ad-hoc/UC4-driven orchestration. If this job needs to run on a time-based schedule, update the `schedule_interval` parameter in `dag_ausd_bp_ta_rn_einzeln.py` (e.g., `"0 2 * * *"` for daily execution at 2:00 AM).

---

## 5. Known Gaps & Unresolved References

*   **Date Type Compatibility:** The legacy Oracle script compared dates using `ms.valid_to <= TO_DATE('19000101', 'YYYYMMDD')`. In the migrated BigQuery SQL, this is converted to `ms.valid_to <= PARSE_DATE('%Y%m%d', v_datum)`. This assumes `ms.valid_to` is stored as a `DATE` type in BigQuery. If `ms.valid_to` was migrated as a `TIMESTAMP` or `STRING`, explicit casting may be required.
*   **Upstream Dependency Alignment:** This job relies on `dwtk_meldungen` containing a record where `job_kennung = 'BERT_DROP_TEMP_TABLE'` to resolve `v_datum`. Ensure that the upstream process populating this table is fully operational in GCP before scheduling this job.
*   **Template Search Path:** The DAG uses `/home/airflow/gcs/dags/gcp_sql` and `/opt/airflow/dags/gcp_sql` as template search paths. If your Cloud Composer environment uses a non-standard directory structure, adjust the `template_searchpath` array in the DAG definition.

---

## 6. Validation

To validate the migration, execute the following testing steps:

### 6.1. Dry-Run Validation
Run a dry-run of the SQL script in the BigQuery Console to verify syntax and schema references:
1.  Open the BigQuery Console.
2.  Replace the Airflow template placeholders (e.g., `{{ var.value.gcp_project_id }}`) with actual environment values.
3.  Click **Analyze** (or run a dry-run) to ensure there are no syntax errors or missing table references.

### 6.2. DAG Execution Test
1.  Navigate to the Airflow UI.
2.  Locate `dag_ausd_bp_ta_rn_einzeln` and unpause it.
3.  Trigger the DAG manually by clicking the **Play** button.
4.  Monitor the task `process_rn_einzeln` to ensure it completes successfully (turns green).

### 6.3. Data Reconciliation (Passing Criteria)
A successful migration test must meet the following criteria:
*   **Row Count Match:** The row count of `sof_ta_rn_einzeln` in BigQuery should match the row count of `SOF$TA_RN_EINZELN` in the legacy Oracle database (assuming identical source data states).
*   **Null Value Check:** Verify that critical columns such as `CNTRCT_ID` do not contain unexpected `NULL` values.
*   **Logic Verification:** Query a sample of contracts with known multiSIM or fax configurations to verify that the conditional `CASE` statements mapped the MSISDNs and statuses (`A` for Active, `L` for Legacy/Inactive) correctly based on the calculated `v_datum`.

---

## 7. Rollback Procedure

In the event of a critical failure during deployment or execution, follow these steps to roll back to the legacy system:

1.  **Pause the Airflow DAG:**
    Go to the Airflow UI and toggle the switch next to `dag_ausd_bp_ta_rn_einzeln` to **Off** (paused).
2.  **Re-enable Legacy Orchestration:**
    Re-activate the UC4 job `DW.BERT_AUSD_BP_TA_RN_EINZELN` in the legacy scheduler.
3.  **Verify Legacy Database State:**
    If the target table in Oracle was modified or truncated during the cutover attempt, restore the Oracle table `SOF$TA_RN_EINZELN` from the last database backup, or re-run the legacy Oracle SQL script `d_ausd_bp_ta_rn_einzeln.sql` to rebuild the data.
4.  **Investigate Logs:**
    Check the Airflow task logs and BigQuery job history to diagnose the root cause of the failure before attempting redeployment.