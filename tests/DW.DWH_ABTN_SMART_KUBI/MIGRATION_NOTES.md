# Migration Notes: DW.DWH_ABTN_SMART_KUBI

This document details the migration of the UC4/Automic UNIX job `DW.DWH_ABTN_SMART_KUBI` and its associated shell wrappers and SQL scripts to Google Cloud Platform (GCP) using **BigQuery** and **Cloud Composer (Apache Airflow)**.

---

## 1. Summary

The legacy workflow consists of a single UC4 UNIX job (`DW.DWH_ABTN_SMART_KUBI`) that executes a PL/SQL script to aggregate subscriber and contract data for a dynamically calculated reporting month (`MONATSID`). 

### Migration Scope
* **Source Platform**: UC4/Automic Scheduler, KornShell (KSH) wrappers, Oracle Database (PL/SQL).
* **Target Platform**: Google Cloud Platform (GCP).
  * **Orchestration**: Cloud Composer (Apache Airflow 2.x).
  * **Data Warehouse / Compute**: BigQuery (SQL Scripting).
* **Core Business Logic**: Truncates the temporary table `dwh_ta_t_smart_kubi` and repopulates it with aggregated contract and tariff data from active tariffs and contract master records matching specific KPI codes (`VVLREIN`, `VVLTWC2C`, `MIGP2CBF`).

---

## 2. Generated Artifacts

The following files have been generated to replace the legacy UC4 job, shell wrappers, and PL/SQL scripts:

| File Path | Language | Role / Description |
| :--- | :--- | :--- |
| `DW.DWH_ABTN_SMART_KUBI.py` | Python (Airflow) | The parent Airflow DAG. It handles the dynamic calculation of the reporting month (`MONATSID`) and orchestrates the execution. |
| `d_abtn_x_smart_kubi.sql` | BigQuery SQL | The core business logic. Migrated from Oracle PL/SQL to a native BigQuery Scripting block (`DECLARE...BEGIN...EXCEPTION...END`). |
| `dw_init.py` | Python | Migrated from `.dw_init`. Initializes environment variables and directory paths (retained primarily for legacy compatibility). |
| `f_alis_msgerr.py` | Python | Migrated from `f_alis_msgerr.ksh`. Provides status tracking, auditing, and error handling by calling BigQuery stored procedures. |
| `h_alis_sqlplus.py` | Python | Migrated from `h_alis_sqlplus.ksh`. Validates SQL file readability and executes queries using the BigQuery Client API. |
| `r_sqlscript.py` | Python | Migrated from `r_sqlscript`. A CLI-compatible runner that parses arguments, resolves script paths, and manages execution logging. |

---

## 3. Key Design Decisions

### BigQuery Scripting over Python-based ETL
* **Decision**: The core PL/SQL block was migrated to a native BigQuery Scripting block (`DECLARE...BEGIN...EXCEPTION...END`) rather than a Python/Pandas transformation.
* **Reasoning**: Keeping the heavy aggregation logic inside BigQuery leverages its native distributed compute engine, automatic partition pruning, and transactional safety (`BEGIN TRANSACTION...COMMIT/ROLLBACK TRANSACTION`). It avoids unnecessary data egress/ingress overhead.

### Airflow-Level Parameter Generation
* **Decision**: The legacy date logic (calculating `MONATSID` based on whether the execution day is before the 15th) was moved to the Airflow DAG level using Python.
* **Reasoning**: This allows the reporting month to be calculated dynamically based on the DAG's logical execution date and passed cleanly as a query parameter (`@p_monats_id`) to BigQuery, ensuring reproducibility and compatibility with Airflow backfills.

### Modernization of Joins and Functions
* **Decision**: Oracle-proprietary outer joins (`(+)`) were modernized to standard ANSI `LEFT JOIN` syntax. Oracle-specific functions were mapped to BigQuery equivalents (e.g., `NVL` $\rightarrow$ `COALESCE`, `DECODE` $\rightarrow$ `CASE WHEN`, `ADD_MONTHS` $\rightarrow$ `DATE_ADD`).
* **Reasoning**: Ensures compatibility with BigQuery Standard SQL while improving query readability and maintainability.

---

## 4. Manual Steps Before Go-Live

Before deploying and running the migrated workflow, the following setup steps must be performed in the target GCP environment:

### 4.1 Schema and Dataset Creation
1. **Target Dataset**: Ensure the BigQuery dataset containing the DWH tables exists (e.g., `dwh_dataset`).
2. **Tables**: Verify that the following tables are created and populated:
   * `dwh_ta_t_smart_kubi` (Target table)
   * `dwh_vi_l_map_fa_tarif` (Source view/table)
   * `bl_d_tarif` (Source table)
   * `dwh_ta_f_d1_twvv_tn` (Source partition table - ensure it is partitioned by `gueltigkeitszeitpunkt`)
   * `dwh_ta_c_vertrag` (Source table)
3. **Metadata Logging Procedures**: Deploy the stored procedures representing the legacy `BERT_MELDUNG` package into your metadata dataset (e.g., `metadata_dataset`):
   * `BERT_MELDUNG_SetzeStatusOk`
   * `BERT_MELDUNG_SetzeStatusAbbruch`
   * `BERT_MELDUNG_Erzeuge_Eintrag`
   * `BERT_MELDUNG_Fehler`
   * `BERT_MELDUNG_SetzeZusatzInfos_Stichtag`
   * `BERT_MELDUNG_SetzeZusatzInfos_Timing`
   * `GetUniqueEintragsNr` (Stored Function)

### 4.2 IAM and Permissions
Ensure the Service Account used by Cloud Composer/Airflow has the following IAM roles:
* `roles/bigquery.jobUser` (To run BigQuery jobs)
* `roles/bigquery.dataEditor` on the target and metadata datasets (To read/write tables and execute stored procedures)

### 4.3 Airflow Variables
Configure the following Airflow Variables in the Composer environment:
* `GCP_PROJECT`: The target GCP Project ID.
* `BQ_METADATA_DATASET`: The dataset where the `BERT_MELDUNG` stored procedures reside (e.g., `metadata_dataset`).
* `GCS_BUCKET`: The Cloud Storage bucket used for staging or logging.

### 4.4 Scheduling
The DAG is currently configured with `schedule=None` (externally triggered) to match the legacy UC4 configuration. If this job needs to run on a temporal schedule, update the `schedule` parameter in `DW.DWH_ABTN_SMART_KUBI.py` (e.g., `schedule="0 2 15 * *"` to run on the 15th of every month).

---

## 5. Known Gaps & Unresolved References

### 5.1 Unresolved References
* **UC4 Includes**: `DW.HOLE_PFAD` and `DW.LESE_LOG` were referenced in the legacy UC4 job but not supplied. Human review has confirmed these are legacy environment/logging utilities that are **retired** in GCP, as Airflow handles logging natively.
* **Environment Files**: `.dw_global` and `.dw_lokal` were not supplied. Any environment-specific variables they set must be configured as Airflow Variables or Cloud Composer Environment Variables.

### 5.2 Redesign (B4) / Modernization Opportunities
* **Direct Airflow-to-BigQuery Execution**: The current DAG uses a placeholder `PythonOperator` for `dw_dwh_abtn_smart_kubi_task`. To align with GCP best practices, it is highly recommended to bypass the Python wrapper scripts (`r_sqlscript.py`, `h_alis_sqlplus.py`) and execute the SQL script directly using the **`BigQueryInsertJobOperator`**:

```python
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

execute_sql = BigQueryInsertJobOperator(
    task_id="execute_sql",
    configuration={
        "query": {
            "query": "{% include 'd_abtn_x_smart_kubi.sql' %}",
            "useLegacySql": False,
            "parameterMode": "NAMED",
            "queryParameters": [
                {
                    "name": "p_monats_id",
                    "parameterType": {"type": "INT64"},
                    "parameterValue": {"value": "{{ task_instance.xcom_pull(task_ids='dw_dwh_abtn_smart_kubi_task') }}"}
                },
                {
                    "name": "p_eintrags_nr",
                    "parameterType": {"type": "INT64"},
                    "parameterValue": {"value": "{{ run_id }}"}
                }
            ]
        }
    }
)
```

---

## 6. Validation

To validate the migration, perform the following tests:

### 6.1 Dry Run (BigQuery Console)
1. Open the BigQuery Console.
2. Copy the contents of `d_abtn_x_smart_kubi.sql`.
3. Declare and set the parameters manually at the top of the editor for testing:
   ```sql
   DECLARE p_monats_id INT64 DEFAULT 201509;
   DECLARE p_eintrags_nr INT64 DEFAULT 12345;
   ```
4. Run the query and verify that it compiles and executes without errors.

### 6.2 DAG Execution Test
1. Upload `DW.DWH_ABTN_SMART_KUBI.py` to your Airflow DAGs folder.
2. Trigger the DAG manually via the Airflow UI or CLI:
   ```bash
   airflow dags trigger dw_dwh_abtn_smart_kubi
   ```
3. Verify that the task `dw_dwh_abtn_smart_kubi_task` completes successfully.

### 6.3 Success Criteria
* The DAG run status is `SUCCESS`.
* The target table `dwh_ta_t_smart_kubi` is truncated and populated with aggregated records.
* The number of rows inserted matches the expected count for the calculated `MONATSID`.
* The execution metadata is correctly logged to the tracking tables via the `BERT_MELDUNG` stored procedures.

---

## 7. Rollback Procedure

In the event of a critical failure or data corruption during deployment:

1. **Pause the DAG**: Immediately pause the Airflow DAG `dw_dwh_abtn_smart_kubi` in the Airflow UI to prevent further executions.
2. **Restore Target Table**: If the target table `dwh_ta_t_smart_kubi` was corrupted, restore it to its pre-execution state using BigQuery Time Travel:
   ```sql
   CREATE OR REPLACE TABLE `your_project.your_dataset.dwh_ta_t_smart_kubi`
   AS SELECT * FROM `your_project.your_dataset.dwh_ta_t_smart_kubi`
   FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
   ```
3. **Revert Metadata**: If necessary, mark the corresponding entry in the metadata tracking table as aborted or delete the failed run entry.
4. **Re-enable Legacy Job**: If a complete fallback is required, re-enable the legacy UC4 job `DW.DWH_ABTN_SMART_KUBI` on the on-premises system.