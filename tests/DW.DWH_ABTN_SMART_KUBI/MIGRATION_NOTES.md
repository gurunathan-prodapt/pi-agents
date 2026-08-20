# Migration Notes: DW.DWH_ABTN_SMART_KUBI

This document provides comprehensive technical notes for the migration of the legacy UC4 UNIX job `DW.DWH_ABTN_SMART_KUBI` and its associated shell and SQL utilities to Google Cloud Platform (GCP).

---

## 1. Summary

The standalone UC4 UNIX job `DW.DWH_ABTN_SMART_KUBI` has been migrated from its legacy on-premises environment to **Google Cloud Platform (GCP)**. 

* **Legacy Platform**: UC4/Automic Scheduler executing KornShell (KSH) scripts and Oracle PL/SQL anonymous blocks on a dedicated UNIX host (`dwhdwh1p`).
* **Target Platform**: **Google Cloud Composer (Apache Airflow)** for orchestration and **Google BigQuery** for data warehousing, scripting, and execution.
* **Core Function**: The job calculates a dynamic reporting month parameter (`MONATSID`) based on the execution date, truncates the target table `dwh$ta_t_smart_kubi`, and executes an aggregation query to populate it with metrics from `dwh$ta_f_d1_twvv_tn` and related dimensions.

---

## 2. Generated Artifacts

The migration process generated the following files, which replace the legacy shell scripts, SQL files, and UC4 configurations:

| Legacy File / Object | Migrated Artifact | Role / Description |
| :--- | :--- | :--- |
| `DW.DWH_ABTN_SMART_KUBI` (UC4 Job) | `dags/DW_DWH_ABTN_SMART_KUBI.py` | Airflow DAG that orchestrates parameter calculation and triggers the downstream execution tasks. |
| `.dw_init` (KSH) | `local/home/gurunathan_t/kubi/.dw_init.py` | Python script that initializes environment variables and directory paths, mapping them to GCP equivalents. |
| `d_abtn_x_smart_kubi.sql` (PL/SQL) | `local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql` | BigQuery SQL scripting block containing the converted table truncation and ANSI-compliant aggregation query. |
| `f_alis_msgerr.ksh` (KSH Library) | `local/home/gurunathan_t/kubi/f_alis_msgerr.py` | Python utility module replacing the legacy error-handling, status-logging, and telemetry framework (`BERT_MELDUNG`). |
| `h_alis_sqlplus.ksh` (KSH Library) | `local/home/gurunathan_t/kubi/h_alis_sqlplus.py` | Python utility module replacing the SQL*Plus wrapper to validate and execute SQL scripts against BigQuery. |
| `r_sqlscript` (KSH Wrapper) | `local/home/gurunathan_t/kubi/r_sqlscript.py` | Python runner script that parses command-line arguments, resolves relative paths, and executes SQL scripts on BigQuery. |

---

## 3. Key Design Decisions

### 3.1 Transition to BigQuery Scripting
The legacy Oracle PL/SQL anonymous block was converted into a native **BigQuery Scripting Block** (`DECLARE`, `BEGIN...EXCEPTION...END`). This approach preserves the procedural flow (variable declaration, static truncation, and conditional inserts) within a single SQL asset, minimizing the need for complex Python-based query orchestration.

### 3.2 ANSI SQL Join Conversion
Oracle-proprietary outer join syntax `(+)` was refactored into standard ANSI-compliant `LEFT OUTER JOIN` statements. Join filters involving date ranges (e.g., `l_monats_date > d.gueltig_von(+)`) were integrated directly into the `ON` clauses of the respective `LEFT JOIN` blocks to guarantee identical logical evaluation.

### 3.3 Partition Pruning over Partition-Extended Syntax
The legacy Oracle query targeted a specific partition explicitly using `partition(dwh$ta_f_d1_twvv_tn_&1)`. In BigQuery, explicit partition-extended table names are unsupported. The migration relies on BigQuery's native partition pruning by applying a standard `WHERE` filter on the partitioned column `gueltigkeitszeitpunkt` using the calculated `l_monats_id`.

### 3.4 Python-Based Date Logic in Airflow
The legacy UC4 date arithmetic (subtracting days to find the previous month if the execution day is before the 15th) was migrated to a Python helper function (`calculate_monatsid`) inside the Airflow DAG. This leverages Airflow's execution context (`logical_date`) to ensure deterministic, reproducible parameter generation.

### 3.5 Telemetry and Logging Realignment
The legacy `BERT_MELDUNG` Oracle package calls were mapped to BigQuery stored procedures (e.g., `CALL logging_dataset.SetzeStatusOk(...)`) or direct table updates. This preserves operational telemetry (job status, execution IDs, and timing metrics) in the target BigQuery environment.

---

## 4. Manual Steps Before Go-Live

To ensure a successful deployment, the following manual setup steps must be completed in the target environment:

### 4.1 BigQuery Schema and Dataset Creation
1. Create the target dataset (e.g., `dwh_dataset`) and the logging dataset (e.g., `logging_dataset`) in BigQuery.
2. Deploy the target table `dwh$ta_t_smart_kubi` with the correct schema.
3. Ensure the source table `dwh$ta_f_d1_twvv_tn` is partitioned on `gueltigkeitszeitpunkt` to optimize query performance and cost.
4. Deploy the logging tables (`bert_meldung`, `bert_fehler`) and their associated stored procedures (`SetzeStatusOk`, `SetzeStatusAbbruch`, `Erzeuge_Eintrag`, `Fehler`, `SetzeZusatzInfos`) in the logging dataset.

### 4.2 IAM and Permissions
1. Grant the Cloud Composer / Airflow worker service account the following IAM roles:
   * `roles/bigquery.jobUser` (to run queries and scripts)
   * `roles/bigquery.dataEditor` on the target and logging datasets
   * `roles/storage.objectViewer` on the GCS bucket housing the SQL scripts

### 4.3 Airflow Variables and Connections
Configure the following Airflow Variables in the Cloud Composer environment:
* `GCP_PROJECT`: The target Google Cloud Project ID.
* `GCP_REGION`: The target GCP region (e.g., `europe-west3`).
* `GCS_BUCKET`: The GCS bucket path (e.g., `gs://my-dwh-bucket`) where scripts and logs are stored.
* `BQ_DATASET`: The BigQuery dataset name for logging (maps to the legacy `DW_ORAUSER` or schema namespace).

### 4.4 Scheduling and Triggering
The migrated DAG is configured with `schedule=None` to match its legacy standalone, externally triggered behavior. If this job needs to be scheduled on a time or event basis, update the `schedule` parameter in `dags/DW_DWH_ABTN_SMART_KUBI.py` (e.g., `schedule="0 2 15 * *"` to run on the 15th of every month).

---

## 5. Known Gaps & Unresolved References

### 5.1 Unresolved Environment Files
The legacy `.dw_init` script sourced `.dw_global` and `.dw_lokal` files, which were not supplied in the migration bundle. 
* **Resolution**: Any environment-specific variables historically set by these files must be configured as Airflow Variables or Cloud Composer environment variables.

### 5.2 Airflow DAG Operator Stub
The `populate_temp_table` task in `DW_DWH_ABTN_SMART_KUBI.py` is currently defined as an `EmptyOperator` acting as a structural placeholder.
* **Resolution**: Before go-live, replace this stub with a `BigQueryInsertJobOperator` pointing to the migrated `d_abtn_x_smart_kubi.sql` script, or a `BashOperator`/`PythonOperator` executing `r_sqlscript.py`.

### 5.3 Oracle-Specific Functions and Hints
* All Oracle optimizer hints (e.g., `/*+ parallel(t,4) full(t) */`) have been stripped as they are ignored or unsupported by BigQuery.
* Ensure that any custom User-Defined Functions (UDFs) or legacy packages not covered in this migration are deployed or refactored.

---

## 6. Validation

To validate the migrated workflow, execute the following test cases in the staging environment:

### 6.1 Parameter Calculation Test
1. Trigger the Airflow DAG manually with a logical date set to **before the 15th of the month** (e.g., `2023-10-10`).
   * **Expected Output**: The task log must output the exact German text: `Berichtsmonat:  202309` (previous month).
2. Trigger the Airflow DAG manually with a logical date set to **on or after the 15th of the month** (e.g., `2023-10-20`).
   * **Expected Output**: The task log must output the exact German text: `Berichtsmonat:  202310` (current month).

### 6.2 End-to-End Execution Test
1. Ensure test data exists in the source tables (`dwh$vi_l_map_fa_tarif`, `bl_d_tarif`, `dwh$ta_f_d1_twvv_tn`, `dwh$ta_c_vertrag`) for the target reporting month.
2. Trigger the DAG.
3. **Verification Steps**:
   * Verify that the DAG completes with a `SUCCESS` status.
   * Verify that the target table `dwh$ta_t_smart_kubi` was successfully truncated and populated.
   * Run a row-count comparison between the legacy Oracle table and the new BigQuery table for the same `MONATSID` to ensure data parity.
   * Check the `bert_meldung` logging table in BigQuery to confirm that a record was created with status `OK` and the correct execution metadata.

---

## 7. Rollback Procedure

If critical issues are encountered post-go-live, perform the following steps to roll back to the legacy system:

1. **Pause the Airflow DAG**:
   Disable the migrated DAG in the Airflow UI or via the CLI:
   ```bash
   gcloud composer environments run <env-name> \
       --location <region> dags pause -- dw_dwh_abtn_smart_kubi
   ```
2. **Clean Up Target Tables**:
   If the migrated job partially executed and corrupted the target table, truncate the BigQuery table to prevent downstream consumption of invalid data:
   ```sql
   TRUNCATE TABLE `your_project.dwh_dataset.dwh$ta_t_smart_kubi`;
   ```
3. **Re-enable Legacy UC4 Job**:
   In the UC4/Automic UI, locate the job `DW.DWH_ABTN_SMART_KUBI` and set its active flag back to `1` (Active).
4. **Verify Legacy Execution**:
   Trigger the legacy job manually and verify that the Oracle database table `DWH$TA_T_SMART_KUBI` is populated correctly and that operational logs are generated without errors.