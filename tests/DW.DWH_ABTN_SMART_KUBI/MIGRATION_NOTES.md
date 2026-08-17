# Migration Notes: DW.DWH_ABTN_SMART_KUBI

This document details the migration of the legacy UC4 job `DW.DWH_ABTN_SMART_KUBI` and its associated PL/SQL script to Google Cloud Platform (GCP) using Apache Airflow (Cloud Composer) and Google BigQuery.

---

## 1. Summary
The `DW.DWH_ABTN_SMART_KUBI` workflow is a data preparation job designed to aggregate subscriber transaction data (specifically contract renewals and migrations) and populate the temporary target table `dwh_ta_t_smart_kubi`. 

* **Source Platform**: UC4 (Automic) & Oracle Database (Host: `DWHDWH1P`)
* **Target Platform**: Google Cloud Platform (GCP)
  * **Orchestration**: Apache Airflow (Cloud Composer)
  * **Database**: Google BigQuery
* **Triggering**: The legacy job was externally triggered with no fixed calendar schedule. The migrated Airflow DAG maintains this behavior (`schedule=None`).

---

## 2. Generated Artifacts
The migration process generated two primary artifacts:

1. **Airflow DAG File** (`dw_dwh_abtn_smart_kubi_dag.py`)
   * **Role**: Orchestrates the workflow. It dynamically calculates the reporting month (`MONATSID`) based on the execution's logical date, logs the value in the legacy format, and provides the execution framework.
2. **BigQuery SQL Script** (`d_abtn_x_smart_kubi.sql`)
   * **Role**: Contains the converted procedural database logic. It truncates the target table, executes a complex transactional `INSERT INTO ... SELECT` query with ANSI-compliant joins, and handles exceptions.

---

## 3. Key Design Decisions

### BigQuery Scripting Block (`DECLARE ... BEGIN ... EXCEPTION`)
To preserve the procedural structure of the original Oracle PL/SQL block, the SQL script was migrated as a BigQuery Scripting Block. This allows local variables (such as `l_monats_id`, `l_monats_date`, and `v_anzahl_ds`) to remain scoped within the execution block, maintaining parity with the legacy code.

### ANSI Join Rewrite
The legacy Oracle script utilized proprietary outer-join syntax (`(+)`) combined with range checks in the `WHERE` clause:
```sql
WHERE fact.dwh_vertrag_id = d.dwh_vertrag_id(+)
  AND l_monats_date > d.gueltig_von(+)
  AND l_monats_date <= d.gueltig_bis(+)
```
This was rewritten into standard ANSI `LEFT JOIN` syntax. The range checks were moved directly into the `LEFT JOIN ... ON` clause to prevent incorrect row elimination:
```sql
LEFT JOIN dwh_ta_c_vertrag d 
  ON fact.dwh_vertrag_id = d.dwh_vertrag_id
 AND l_monats_date > CAST(d.gueltig_von AS DATE)
 AND l_monats_date <= CAST(d.gueltig_bis AS DATE)
```

### Static Truncate
The legacy script executed a truncate statement dynamically via a utility package (`dwpa_util_skript.runstatement`). Since BigQuery natively supports the `TRUNCATE TABLE` statement, this was simplified to a static, high-performance `TRUNCATE TABLE dwh_ta_t_smart_kubi;` command.

### Idempotent Date Logic
The legacy UC4 script calculated the reporting month (`MONATSID`) using the system date at runtime. To ensure idempotency (the ability to safely backfill historical runs), the Airflow DAG calculates `MONATSID` using the DAG run's `logical_date` (execution date) instead of timezone-unaware system time.

---

## 4. Manual Steps Before Go-Live

### 1. Schema and Dataset Creation
Ensure that the target table and all source tables/views exist in BigQuery under the dataset specified by the `BQ_DATASET` variable:
* **Target Table**: `dwh_ta_t_smart_kubi`
* **Source Tables/Views**:
  * `dwh_vi_l_map_fa_tarif`
  * `bl_d_tarif`
  * `dwh_ta_f_d1_twvv_tn`
  * `dwh_ta_c_vertrag`

### 2. Deploy Mock Logging Stored Procedure
The exception block in the SQL script calls a custom logging procedure:
```sql
CALL dwpa_meldung_fehler('F', EintragsNr, FehlerNr, ErrText, CAST(ErrC AS STRING));
```
If your organization has a centralized logging framework in BigQuery, ensure this stored procedure is deployed. If no such framework exists, this call must be mocked or commented out.

### 3. Airflow Variables Configuration
Configure the following Airflow Variables in the Airflow UI (**Admin -> Variables**):
* `GCP_PROJECT`: The target GCP Project ID.
* `GCP_REGION`: The target GCP Region (e.g., `europe-west3`).
* `BQ_DATASET`: The target BigQuery dataset name.

### 4. IAM Permissions
Ensure the service account running the Cloud Composer workers has the following IAM roles:
* `roles/bigquery.dataEditor` on the target dataset.
* `roles/bigquery.jobUser` on the GCP project.

---

## 5. Known Gaps & Unresolved References

### Redesign (B4) Item: SQL Execution Operator
The generated Airflow DAG currently contains an `EmptyOperator` stub (`stub_dwh_abtn_smart_kubi`) representing the execution of the SQL script:
```python
# REVIEW-STRUCT: launcher wraps SQL script [d_abtn_x_smart_kubi.sql] converted by a separate pipeline
stub_dwh_abtn_smart_kubi = EmptyOperator(
    task_id="dwh_abtn_smart_kubi",
    ...
)
```
**Required Action**: Before go-live, replace this stub with a `BigQueryInsertJobOperator`. The operator must:
1. Read the SQL from `d_abtn_x_smart_kubi.sql`.
2. Inject the calculated `MONATSID` from XCom as the query parameter `@param_monats_id`.
3. Inject the Airflow task instance try number as `@param_eintragsnr`.

Example implementation:
```python
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

execute_sql = BigQueryInsertJobOperator(
    task_id="dwh_abtn_smart_kubi",
    configuration={
        "query": {
            "query": "{% include 'd_abtn_x_smart_kubi.sql' %}",
            "useLegacySql": False,
            "parameterMode": "NAMED",
            "queryParameters": [
                {
                    "name": "param_monats_id",
                    "parameterType": {"type": "INT64"},
                    "parameterValue": {"value": "{{ ti.xcom_pull(task_ids='log_monatsid', key='MONATSID') }}"}
                },
                {
                    "name": "param_eintragsnr",
                    "parameterType": {"type": "INT64"},
                    "parameterValue": {"value": "{{ ti.try_number }}"}
                }
            ]
        }
    }
)
```

### Partition Pruning
The source table `dwh_ta_f_d1_twvv_tn` was partitioned in Oracle. To maintain query performance and control costs in BigQuery, ensure that `dwh_ta_f_d1_twvv_tn` is partitioned on the `gueltigkeitszeitpunkt` column.

---

## 6. Validation

### How to Run Tests
1. **Dry Run**: Validate the SQL syntax in the BigQuery Console.
2. **DAG Execution**: Trigger the Airflow DAG manually via the Airflow UI using the "Trigger DAG w/ config" option. Test with two distinct logical dates:
   * **Scenario A (Day < 15)**: Set the execution date to `2023-10-10`.
   * **Scenario B (Day >= 15)**: Set the execution date to `2023-10-20`.

### What "Passing" Means
* **Date Logic Validation**:
  * For Scenario A (`2023-10-10`), the task log for `log_monatsid` must output:
    ```text
    Berichtsmonat:  202309
    ```
  * For Scenario B (`2023-10-20`), the task log for `log_monatsid` must output:
    ```text
    Berichtsmonat:  202310
    ```
* **Database Validation**:
  * The target table `dwh_ta_t_smart_kubi` is successfully truncated.
  * The query executes without errors, and the transaction commits.
  * The final log output of the SQL script displays the row count:
    ```text
    [X] rows inserted in DWH$TA_T_SMART_KUBI
    ```

---

## 7. Rollback Procedure

In the event of a critical failure post-deployment:

1. **Pause the Airflow DAG**: Immediately pause the `dw_dwh_abtn_smart_kubi` DAG in the Airflow UI to prevent further executions.
2. **Revert Code Deployment**: Remove the DAG file `dw_dwh_abtn_smart_kubi_dag.py` from the Cloud Composer DAGs bucket.
3. **Database Cleanup**: If a partial or corrupted load occurred, run a manual truncate on the BigQuery target table:
   ```sql
   TRUNCATE TABLE `your_project.your_dataset.dwh_ta_t_smart_kubi`;
   ```
4. **Re-enable Legacy Job**: If a fallback to the legacy environment is required, re-enable the `DW.DWH_ABTN_SMART_KUBI` job in UC4.