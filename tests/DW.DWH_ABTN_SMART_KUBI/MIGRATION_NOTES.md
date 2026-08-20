# Migration Notes: DW.DWH_ABTN_SMART_KUBI

This document provides comprehensive migration notes for transitioning the legacy UC4 UNIX job `DW.DWH_ABTN_SMART_KUBI` and its associated shell utilities and SQL scripts to Google Cloud Platform (GCP), utilizing **Cloud Composer (Apache Airflow)** for orchestration and **Google BigQuery** as the target data warehouse.

---

## 1. Summary

The legacy UC4 job `DW.DWH_ABTN_SMART_KUBI` has been migrated from an on-premise UNIX/Oracle environment to a modern cloud-native architecture on GCP. 

### Scope of Migration
* **Orchestration:** Migrated from UC4 (`JOBS_UNIX`) to an **Apache Airflow DAG** running in Cloud Composer.
* **Database & Execution Layer:** Migrated from Oracle (PL/SQL, SQL*Plus) to **Google BigQuery** (Standard SQL Procedural Scripting).
* **Shell Wrapper Utilities:** Legacy KornShell (KSH) scripts (`r_sqlscript`, `f_alis_msgerr.ksh`, `h_alis_sqlplus.ksh`, `.dw_init`) have been refactored into modular **Python 3** scripts and JSON configurations.

### Core Business Function
The primary purpose of this pipeline is to aggregate contract, tariff, and transaction metrics for a dynamically calculated reporting month (`MONATSID`) and load the results into the target table `dwh_ta_t_smart_kubi` (formerly `DWH$TA_T_SMART_KUBI`). 

The reporting month calculation logic is as follows:
* If the execution day of the month is **before the 15th**, the reporting month context is shifted back to the **prior month** (format: `YYYYMM`).
* If the execution day is **on or after the 15th**, the reporting month context remains the **current month** (format: `YYYYMM`).

---

## 2. Generated Artifacts

The migration process generated the following target artifacts:

| Target File Path | Language | Role / Description |
| :--- | :--- | :--- |
| `dags/kubi/dw_dwh_abtn_smart_kubi.py` | Python (Airflow) | The main Airflow DAG orchestrating the pipeline. It dynamically calculates `MONATSID` and sequences the execution. |
| `.dw_init.json` | JSON | Configuration file mapping legacy environment directory pointers (e.g., `DW_DIR_ROOT`, `DW_DIR_PROT`) to Google Cloud Storage (GCS) bucket paths. Used to initialize Airflow Variables. |
| `kubi/d_abtn_x_smart_kubi.sql` | SQL (BigQuery) | Migrated BigQuery Standard SQL Procedural Script. Replaces the Oracle PL/SQL anonymous block, converting Oracle-specific functions to BigQuery equivalents and wrapping execution in explicit transactions. |
| `f_alis_msgerr.py` | Python 3 | Python port of the legacy `f_alis_msgerr.ksh` logging library. Provides functions to log job status (OK, Abbruch), register errors, and generate log filenames, calling BigQuery stored procedures (`BERT_MELDUNG__*`) to maintain legacy metadata tracking. |
| `h_alis_sqlplus.py` | Python 3 | Python port of `h_alis_sqlplus.ksh`. Validates SQL script readability and executes SQL files against BigQuery using the `google.cloud.bigquery` client. |
| `r_sqlscript.py` | Python 3 | Python port of the `r_sqlscript` execution wrapper. Handles command-line arguments, resolves relative SQL file paths, sets up exception handling, and executes the target SQL script on BigQuery. |

---

## 3. Key Design Decisions

### 3.1. Procedural BigQuery SQL over Python Rewrite
The core ETL logic in `d_abtn_x_smart_kubi.sql` was migrated to a BigQuery Standard SQL Procedural Script (`DECLARE`, `BEGIN TRANSACTION`, `EXCEPTION WHEN ERROR`). This keeps the data processing entirely within BigQuery, maximizing performance and minimizing data egress costs, while preserving the transactional safety of the original PL/SQL block.

### 3.2. Python Port of Shell Utilities
Sourced shell scripts (`r_sqlscript`, `f_alis_msgerr.ksh`, `h_alis_sqlplus.ksh`) were converted to Python modules rather than being retired completely or left as Bash scripts. This ensures compatibility with Cloud Composer (which runs in a serverless Kubernetes environment where local shell environments are ephemeral and lack Oracle/SQL*Plus clients) and allows seamless integration with the Google Cloud SDK.

### 3.3. Dynamic Date Calculation in Airflow
The legacy logic for calculating `MONATSID` (shifting back one month if the execution day is before the 15th) is implemented as a Python helper function within the Airflow DAG. This ensures idempotency and allows the calculated value to be passed as a query parameter (`@p_monats_id`) to BigQuery.

### 3.4. Table Name Normalization
Replaced the invalid special character `$` in legacy table names (e.g., `DWH$TA_T_SMART_KUBI`) with underscores (e.g., `dwh_ta_t_smart_kubi`) to comply with BigQuery naming standards.

---

## 4. Manual Steps Before Go-Live

Before deploying and running the migrated pipeline, the following manual setup steps must be completed:

### 4.1. Schema and Dataset Creation
1. Create the target BigQuery dataset (e.g., `dwh_dataset`) in your designated GCP region.
2. Deploy the target table `dwh_ta_t_smart_kubi` and source tables/views (`dwh_vi_l_map_fa_tarif`, `bl_d_tarif`, `dwh_ta_f_d1_twvv_tn`, `dwh_ta_c_vertrag`).
3. Deploy the migrated `BERT_MELDUNG` stored procedures (`BERT_MELDUNG__SetzeStatusOk`, `BERT_MELDUNG__SetzeStatusAbbruch`, `BERT_MELDUNG__Erzeuge_Eintrag`, `BERT_MELDUNG__Fehler`, `BERT_MELDUNG__SetzeZusatzInfos`) and the sequence generator function `generate_next_eintrags_nr` in BigQuery.

### 4.2. IAM and Permissions
1. Ensure the Cloud Composer environment's service account has the `BigQuery Job User` and `BigQuery Data Editor` roles on the target dataset.
2. Ensure the service account has read/write permissions on the GCS bucket used for logging (`DW_DIR_PROT`).

### 4.3. Airflow Variables and Connections
1. Import the `.dw_init.json` file into Airflow Variables to establish GCS path mappings.
2. Set the following Airflow Variables in the Airflow UI:
   * `GCP_PROJECT`: Your GCP Project ID.
   * `GCP_REGION`: Your GCP Region (e.g., `europe-west3`).
   * `BQ_DATASET`: The target BigQuery dataset name.

### 4.4. Scheduling
The DAG is currently configured with `schedule=None` (on-demand/manual). If this job needs to be triggered on a schedule, update the `schedule` parameter in `dw_dwh_abtn_smart_kubi.py` to match the business requirements (e.g., a cron expression).

---

## 5. Known Gaps & Unresolved References

### 5.1. Unmigrated Upstream SQL Dependency
The Airflow DAG currently uses an `EmptyOperator` placeholder (`dwh_abtn_smart_kubi`) for the actual SQL execution. This must be manually replaced with a `BigQueryInsertJobOperator` or a call to `r_sqlscript.py` once the SQL migration pipeline is fully validated.

### 5.2. Legacy Logging Package Integration
The custom Oracle logging package calls (`dwpa_meldung.fehler` and `dwpa_util_skript`) inside `d_abtn_x_smart_kubi.sql` have been commented out or replaced with standard SQL `ERROR()` raises. A developer must manually implement a BigQuery-compatible logging table write if these application-level logs must be preserved.

### 5.3. Sequence Generation
BigQuery does not natively support sequences. The function `generate_next_eintrags_nr` used in `f_alis_msgerr.py` must be backed by a custom implementation (e.g., a single-row tracking table or UUID-to-integer mapping).

---

## 6. Validation

To validate the migration, perform the following tests:

### 6.1. How to Run the Tests
1. **Airflow DAG Test:** Trigger the DAG manually in the Airflow UI with a specific `logical_date` (e.g., `2023-10-10` and `2023-10-16` to test both sides of the 15th-day boundary).
2. **SQL Script Test:** Execute the BigQuery SQL script `d_abtn_x_smart_kubi.sql` directly in the BigQuery console using mock parameters:
   ```sql
   DECLARE p_monats_id INT64 DEFAULT 202309;
   DECLARE p_eintrags_nr INT64 DEFAULT 999999;
   -- Run script...
   ```

### 6.2. What "Passing" Means
* The Airflow DAG completes successfully without errors.
* The task `calculate_monatsid` prints the correct `Berichtsmonat` to the task logs (e.g., `Berichtsmonat:  202309` for execution date `2023-10-10`).
* The target table `dwh_ta_t_smart_kubi` is truncated and successfully populated with aggregated rows matching the source partition.
* The execution metadata is correctly registered in the `BERT_MELDUNG` tracking tables via the stored procedures.

---

## 7. Rollback Procedure

In the event of a critical failure during deployment or execution, follow these steps to roll back to the legacy system:

1. **Pause the Airflow DAG:** Turn off the toggle for `dw_dwh_abtn_smart_kubi` in the Airflow UI to prevent further executions.
2. **Clean Up Target Data:** If a failed run partially loaded data, run a manual truncate on the BigQuery target table:
   ```sql
   TRUNCATE TABLE `your_project.your_dataset.dwh_ta_t_smart_kubi`;
   ```
3. **Re-enable Legacy Job:** Reactivate the legacy UC4 job `DW.DWH_ABTN_SMART_KUBI` on the legacy UNIX host.
4. **Verify Legacy Execution:** Monitor the legacy logs to ensure the job runs successfully against the Oracle database and that downstream consumers are unaffected.